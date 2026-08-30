#!/usr/bin/env python3
"""Extract FIA regulation PDFs to markdown + JSON sidecars (PLAN.md §6).

Per-PDF flow (cached by ``source_hash``, PLAN.md §2 decision 10):

1. Identify the document (year / section / regulation_type / issue / variant)
   from the cover page + filename; the *actual document year* (parsed from the
   title) drives the output folder, not the source folder (decision 11).
2. Strip page furniture (running headers/footers, blue banner spans, white
   text); capture the printed page number per page first (§6.1 step 4-5).
3. Classify span colours (exact ints, then nearest hue — §6.3) and map them
   to semantic tags via the document's own cover-page convention legend.
4. Detect FIA-drawn strike rectangles geometrically (§3.2) → ``[REMOVED]``;
   detect tall highlight rects (2023-era "highlighted text") → ``[CHANGED]``.
5. Split the body into level-2 article sections (§3.3/§6.4), record clause
   offsets, render pipe tables (§2 decision 9), emit ``[PAGE n]`` markers.
6. Normalize ligature bugs (§6.6, counted), write ``.md`` + ``.json`` sidecar.

Failures are logged per-PDF and never block the batch (§6.1 step 8).

Usage:
  uv run regs_pipeline/extract_markdown.py            # everything, cached
  uv run regs_pipeline/extract_markdown.py --force    # re-extract all
  uv run regs_pipeline/extract_markdown.py --pdf <path> [--force]
"""

from __future__ import annotations

import argparse
import os
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pymupdf

import config as cfg

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Span:
    text: str
    bbox: tuple[float, float, float, float]
    color_int: int
    color_cls: str  # black/pink/red/orange/green/navy/white/unknown
    color_exact: bool
    size: float
    bold: bool
    struck: bool = False
    highlighted: bool = False
    strike_frac: float = 0.0   # fraction of span width covered by strike rects
    strike_left: bool = True   # whether the struck coverage is on the left half
    tag: str | None = None      # resolved later (convention-dependent)
    inner: str | None = None    # inner tag for struck non-pink spans (§6.2)


@dataclass
class Line:
    spans: list[Span]
    bbox: tuple[float, float, float, float]

    @property
    def text(self) -> str:
        out = []
        prev: Span | None = None
        for s in self.spans:
            if prev is not None and _needs_space(prev, s):
                out.append(" ")
            out.append(s.text)
            prev = s
        return "".join(out)

    @property
    def y0(self) -> float:
        return self.bbox[1]

    @property
    def fully_bold(self) -> bool:
        return all(s.bold for s in self.spans if s.text.strip()) and any(s.text.strip() for s in self.spans)

    @property
    def max_size(self) -> float:
        return max((s.size for s in self.spans if s.text.strip()), default=0.0)


@dataclass
class PageData:
    pdf_page: int                      # 1-based
    printed_page: int | None = None
    lines: list[Line] = field(default_factory=list)          # body-zone, furniture stripped
    words: list[tuple] = field(default_factory=list)          # body-zone words (x0,y0,x1,y1,text,...)
    furniture_texts: list[str] = field(default_factory=list)  # stripped (cover fields/title)
    raw_tables: list[tuple[tuple, list[list[Any]]]] = field(default_factory=list)  # (bbox, rows of cell bboxes)
    tables: list[dict[str, Any]] = field(default_factory=list)  # sidecar entries
    n_strike_rects: int = 0

    @property
    def page_label(self) -> int:
        return self.printed_page if self.printed_page is not None else self.pdf_page


@dataclass
class Block:
    """A rendered markdown block assigned to an article (or front matter)."""

    kind: str  # heading1 | heading2 | para | table | pagemarker
    text: str
    page: int  # pdf_page
    printed: int  # page label used in [PAGE n]
    y0: float = 0.0
    tags: set[str] = field(default_factory=set)
    n_changed: int = 0
    n_removed: int = 0
    clause: str | None = None  # clause number if the para starts with one


@dataclass
class ArticleSeg:
    number: str
    title: str
    parent: str | None
    level: int  # 1 = appendix/preamble-style, 2 = normal level-2
    blocks: list[Block] = field(default_factory=list)
    occurrence: int = 0


@dataclass
class Identity:
    year: int
    section: str | None
    regulation_type: str
    section_name: str
    slug: str
    issue: int
    variant: int | None
    doc_id: str
    title: str
    status: str | None
    issue_date: str | None
    wmsc_approval_date: str | None
    convention: dict[str, str]
    color_tags: dict[str, str | None]
    pages: int


# ---------------------------------------------------------------------------
# Identity (cover parse + filename hints)
# ---------------------------------------------------------------------------


def _title_case(text: str) -> str:
    """Normalize an ALL-CAPS cover title; keep known acronyms."""
    if text != text.upper():
        return text
    out = []
    for word in text.split():
        stripped = word.strip("():–-")
        if stripped in ("FIA", "F1", "PU", "WMSC"):
            out.append(word)
        else:
            out.append(word.capitalize())
    return " ".join(out)


def _section_name(title: str) -> str:
    name = re.sub(r"\b20\d{2}\b", "", title)
    name = re.sub(r"\b(FIA|Formula One|Formula 1|F1)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*[:–-]\s*", " ", name)
    name = re.sub(r"\s{2,}", " ", name).strip(" :–-")
    name = re.sub(r"\(\s+", "(", name)
    name = re.sub(r"\s+\)", ")", name)
    return name or title


def _regulation_type(title: str, section: str | None, fname: str) -> str:
    t = title.upper()
    pu = "POWER UNIT" in t or re.search(r"\bPU\b", t) or "-pu-" in fname
    if "SPORTING" in t:
        base = "sporting"
    elif "TECHNICAL" in t:
        base = "technical"
    elif "FINANCIAL" in t:
        return "financial-pu" if pu else "financial-teams"
    elif "OPERATIONAL" in t:
        base = "operational"
    elif "GENERAL" in t:
        base = "general"
    elif section and section in cfg.SECTION_TYPE:
        return cfg.SECTION_TYPE[section]
    else:
        return "unknown"
    return f"{base}-pu" if pu else base


def _parse_convention(cover_lines: list[Line]) -> tuple[dict[str, str], dict[str, str | None]]:
    """Parse the cover 'Convention:'/'CONVENTION:' legend (PLAN.md §3.1/§6.3).

    Returns ``(convention, color_tags)``: ``convention`` maps color class →
    legend description (stored in the sidecar); ``color_tags`` is the
    DEFAULT_COLOR_TAGS map with legend-driven overrides (e.g. 2024 dark red
    means "changes previously approved" → CHANGED).
    """
    convention: dict[str, str] = {}
    color_tags = dict(cfg.DEFAULT_COLOR_TAGS)
    in_legend = False
    last_cls: str | None = None
    for line in cover_lines:
        text = line.text.strip()
        if not text:
            continue
        if cfg.RE_CONVENTION.match(text):
            in_legend = True
            continue
        if cfg.RE_CONTENTS_MARKER.match(text) and in_legend:
            break  # legend ends where the TOC starts (some docs put TOC first)
        if not in_legend:
            continue
        if re.match(r"^(ARTICLE|APPENDIX)\s+\d", text):
            break  # walked past the legend into the body
        colored = [s for s in line.spans if s.text.strip() and s.color_cls not in ("black", "navy", "white", "unknown")]
        if colored:
            cls = colored[0].color_cls
            meaning = " ".join(s.text for s in line.spans if s not in colored).strip()
            meaning = re.sub(r"^[\s:–-]+", "", meaning).strip()
            convention[cls] = meaning
            last_cls = cls
            low = meaning.lower()
            if "unchanged" in low or "as approved" in low:
                color_tags[cls] = None
            elif "governance" in low or "advisory" in low:
                color_tags[cls] = "GOVERNANCE"
            elif "reference" in low:
                color_tags[cls] = "REFERENCE"
            elif ("comment" in low or "explanation" in low or "non-binding" in low
                  or "attention" in low):
                color_tags[cls] = "COMMENT"
            elif "change" in low:
                color_tags[cls] = "CHANGED"
        elif last_cls and line.spans and all(s.color_cls == "black" for s in line.spans if s.text.strip()):
            convention[last_cls] = (convention[last_cls] + " " + text).strip()
    return convention, color_tags


def identify(doc: pymupdf.Document, path: Path) -> Identity:
    """Cheap first pass over page 0 (+ filename) to name the output files."""
    fname = path.stem.lower()
    # legend can trail a multi-page TOC (e.g. 2021 sporting: page 3) — read 6
    front = _read_pages(doc, only_first=6)
    cover = front[0]
    all_lines = cover.lines
    furniture = cover.furniture_texts
    legend_lines = [line for pd in front for line in pd.lines]

    # --- title: the big bold cover line is authoritative; footer furniture
    # --- can carry a STALE template year (the 2023 sporting regs have a
    # --- '2022 Formula 1 Sporting Regulations' footer). Furniture title is
    # --- the fallback for 2026-family docs whose cover has no year. ---
    cover_title, best_size = "", 0.0
    for line in all_lines:
        stripped = line.text.strip()
        if (cfg.RE_CONVENTION.match(stripped) or cfg.RE_CONTENTS_MARKER.match(stripped)
                or cfg.RE_PUBLISHED_ON.search(stripped) or re.match(r"^ISSUE\b", stripped)):
            continue
        if line.fully_bold and line.max_size > best_size and len(stripped) > 6:
            cover_title, best_size = stripped, line.max_size
    furn_title = ""
    for txt in furniture:
        if cfg.RE_TITLE_YEAR.search(txt) and "formula" in txt.lower():
            furn_title = txt.strip()
            break
    if cover_title and cfg.RE_TITLE_YEAR.search(cover_title):
        title = cover_title
    elif furn_title:
        title = furn_title
    else:
        title = cover_title
    title = _title_case(re.sub(r"\s{2,}", " ", title).strip())

    # --- cover fields (Version/Status/Date/WMSC pairs; PUBLISHED ON; ISSUE n) ---
    issue: int | None = None
    status: str | None = None
    issue_date: str | None = None
    wmsc_date: str | None = None
    for line in all_lines:
        texts = [s.text.strip() for s in line.spans if s.text.strip()]
        for i, t in enumerate(texts):
            if i + 1 >= len(texts):
                break
            if cfg.RE_VERSION_ISSUE.match(t):
                m = cfg.RE_ISSUE.search(texts[i + 1])
                if m:
                    issue = int(m.group(1))
            elif cfg.RE_STATUS.match(t):
                status = texts[i + 1].upper()
            elif cfg.RE_DATE_FIELD.match(t):
                issue_date = cfg.parse_date(texts[i + 1])
            elif cfg.RE_WMSC_FIELD.match(t):
                wmsc_date = cfg.parse_date(texts[i + 1])
        joined = line.text.strip()
        if issue is None and len(joined) < 20:
            m = cfg.RE_ISSUE.search(joined)
            if m:
                issue = int(m.group(1))
        m = cfg.RE_PUBLISHED_ON.search(joined)
        if m:
            issue_date = issue_date or cfg.parse_date(m.group(1).strip())
            status = status or "PUBLISHED"

    convention, color_tags = _parse_convention(legend_lines)

    # --- section letter ---
    section: str | None = None
    for txt in furniture + [title]:
        m = cfg.RE_SECTION_BANNER.match(txt.strip())
        if m:
            section = txt.strip()[8].upper()
            break
    if section is None:
        m = cfg.RE_FN_SECTION.search(fname)
        if m:
            section = m.group(1).upper()

    # --- year: title > filename > issue_date (documented fallback) ---
    year: int | None = None
    m = cfg.RE_TITLE_YEAR.search(title)
    if m:
        year = int(m.group(1))
    if year is None:
        m = cfg.RE_FN_YEAR.search(fname)
        if m:
            year = int(m.group(1))
    if year is None and issue_date:
        year = int(issue_date[:4])
    if year is None:
        raise ValueError("cannot determine regulation year")

    # --- issue / variant fallbacks from the filename ---
    if issue is None:
        m = cfg.RE_FN_ISSUE.search(fname)
        if m:
            issue = int(m.group(1))
    variant: int | None = None
    m = cfg.RE_FN_VARIANT.search(fname)
    if m:
        variant = int(m.group(1))
    else:
        m = cfg.RE_FN_LEADING_SEQ.match(fname)
        if m:
            variant = int(m.group(1))
            if variant >= 2000:
                # leading YEAR, not a sequence number: disambiguate same-issue
                # republications by the filename's trailing full date
                m2 = re.search(r"-(20\d{2})-(\d{1,2})-(\d{1,2})$", fname)
                if m2:
                    variant = int(f"{m2.group(1)}{int(m2.group(2)):02d}{int(m2.group(3)):02d}")
        else:
            m = re.search(r"-(20\d{2})-(\d{1,2})$", fname)
            if m:
                variant = int(m.group(2))
    if issue is None:
        issue = 1

    regulation_type = _regulation_type(title, section, fname)
    section_name = _section_name(title)
    slug = cfg.SECTION_SLUG[section] if section else f"{regulation_type}-{year}"
    doc_id = f"{year}/{slug}/iss-{issue:02d}"
    if variant is not None:
        doc_id += f"-v{variant}"

    return Identity(
        year=year, section=section, regulation_type=regulation_type,
        section_name=section_name, slug=slug, issue=issue, variant=variant,
        doc_id=doc_id, title=title, status=status, issue_date=issue_date,
        wmsc_approval_date=wmsc_date, convention=convention,
        color_tags=color_tags, pages=doc.page_count,
    )


# ---------------------------------------------------------------------------
# Page reading: spans, furniture, page numbers, strike/highlight rects
# ---------------------------------------------------------------------------


def _thin_strike(rect: pymupdf.Rect, fill: tuple[float, ...] | None) -> bool:
    """FIA strikes are thin filled rects, magenta or dark red (§3.2)."""
    if fill is None:
        return False
    h, w = rect.y1 - rect.y0, rect.x1 - rect.x0
    if h > cfg.STRIKE_MAX_H or w < cfg.STRIKE_MIN_W:
        return False
    r, g = fill[0], fill[1]
    return r >= 0.6 and g <= 0.35  # magenta (1,0,1) and dark red (0.75,0,0)


def _tall_highlight(rect: pymupdf.Rect, fill: tuple[float, ...] | None) -> bool:
    """Highlight rects are line-height and chromatic (yellow/magenta/cyan);
    gray/white/black fills are table/form shading and excluded."""
    if fill is None:
        return False
    h, w = rect.y1 - rect.y0, rect.x1 - rect.x0
    if h < cfg.HIGHLIGHT_MIN_H or w < cfg.HIGHLIGHT_MIN_W:
        return False
    mx, mn = max(fill), min(fill)
    return mx >= 0.7 and (mx - mn) >= 0.4


def _read_pages(doc: pymupdf.Document, only_first: int | None = None) -> list[PageData]:
    out: list[PageData] = []
    n = doc.page_count if only_first is None else min(only_first, doc.page_count)
    for pno in range(n):
        page = doc[pno]
        height = page.rect.height
        pd = PageData(pdf_page=pno + 1)

        strike_rects: list[pymupdf.Rect] = []
        highlight_rects: list[pymupdf.Rect] = []
        for dr in page.get_drawings():
            r = dr["rect"]
            fill = dr.get("fill")
            if _thin_strike(r, fill):
                strike_rects.append(r)
            elif _tall_highlight(r, fill):
                highlight_rects.append(r)
        pd.n_strike_rects = len(strike_rects)

        raw = page.get_text("dict", sort=True)
        for block in raw["blocks"]:
            if block.get("type") != 0:
                continue
            for rline in block["lines"]:
                spans: list[Span] = []
                for rs in rline["spans"]:
                    if not rs["text"]:
                        continue
                    cls, exact = cfg.classify_color(rs["color"])
                    spans.append(Span(
                        text=rs["text"], bbox=tuple(rs["bbox"]), color_int=rs["color"],
                        color_cls=cls, color_exact=exact, size=rs["size"],
                        bold=bool(rs["flags"] & 16),
                    ))
                if not spans:
                    continue
                line = Line(spans=spans, bbox=tuple(rline["bbox"]))
                if all(s.color_cls == "white" for s in spans if s.text.strip()):
                    pd.furniture_texts.append(line.text.strip())
                    if line.bbox[1] > height * cfg.FOOTER_ZONE:
                        _capture_page_number(pd, line)
                    continue
                if line.bbox[3] < height * cfg.HEADER_ZONE or line.bbox[1] > height * cfg.FOOTER_ZONE:
                    pd.furniture_texts.append(line.text.strip())
                    if line.bbox[1] > height * cfg.FOOTER_ZONE:
                        _capture_page_number(pd, line)
                    continue
                pd.lines.append(line)

        pd.lines = _merge_visual_rows(pd.lines)
        intervals = [(l.bbox[1], l.bbox[3]) for l in pd.lines]
        pd.words = [w for w in page.get_text("words", sort=True)
                    if any(w[1] < iv[1] and w[3] > iv[0] for iv in intervals)]

        for line in pd.lines:
            for span in line.spans:
                if not span.text.strip():
                    continue
                sb = pymupdf.Rect(span.bbox)
                width = max(sb.x1 - sb.x0, 1e-6)
                mid = (sb.y0 + sb.y1) / 2
                intervals: list[tuple[float, float]] = []
                for r in strike_rects:
                    if r.y0 <= mid <= r.y1 or (sb.y0 < r.y1 and r.y0 < sb.y1):
                        ov_l, ov_r = max(sb.x0, r.x0), min(sb.x1, r.x1)
                        if ov_r > ov_l:
                            intervals.append((ov_l, ov_r))
                if intervals:
                    # union the x-intervals (strikes are drawn as paired
                    # parallel rects covering the same range — §3.2)
                    intervals.sort()
                    merged: list[list[float]] = [list(intervals[0])]
                    for lo, hi in intervals[1:]:
                        if lo <= merged[-1][1]:
                            merged[-1][1] = max(merged[-1][1], hi)
                        else:
                            merged.append([lo, hi])
                    struck_w = sum(hi - lo for lo, hi in merged)
                    span.strike_frac = struck_w / width
                    span.strike_left = (merged[0][0] + merged[-1][1]) / 2 <= (sb.x0 + sb.x1) / 2
                    if struck_w >= cfg.STRIKE_COVER * width:
                        span.struck = True
                        continue
                area = sb.get_area()
                if area > 0:
                    for r in highlight_rects:
                        inter = sb & r
                        if not inter.is_empty and inter.get_area() >= cfg.HIGHLIGHT_COVER * area:
                            span.highlighted = True
                            break
        out.append(pd)
    return out


def _merge_visual_rows(lines: list[Line]) -> list[Line]:
    """Merge line objects sharing a visual row (PyMuPDF splits TOC columns,
    headings and table cells into separate lines at the same y). Spans of the
    merged row are sorted by x so leading-number detection keeps working."""
    if not lines:
        return lines
    ordered = sorted(lines, key=lambda l: (l.bbox[1], l.bbox[0]))
    rows: list[Line] = []
    for line in ordered:
        if rows:
            prev = rows[-1]
            h = min(line.bbox[3] - line.bbox[1], prev.bbox[3] - prev.bbox[1])
            overlap = min(line.bbox[3], prev.bbox[3]) - max(line.bbox[1], prev.bbox[1])
            if h > 0 and overlap >= 0.5 * h and line.bbox[1] - prev.bbox[1] < 0.6 * h:
                prev.spans.extend(line.spans)
                prev.spans.sort(key=lambda s: s.bbox[0])
                prev.bbox = (min(prev.bbox[0], line.bbox[0]), min(prev.bbox[1], line.bbox[1]),
                             max(prev.bbox[2], line.bbox[2]), max(prev.bbox[3], line.bbox[3]))
                continue
        rows.append(Line(spans=list(line.spans), bbox=line.bbox))
    return rows


def _capture_page_number(pd: PageData, line: Line) -> None:
    if pd.printed_page is not None:
        return
    for s in line.spans:
        m = cfg.RE_FOOTER_PAGE_NUM.match(s.text.strip())
        if m and m.group(2):
            pd.printed_page = int(m.group(2))
            return


# ---------------------------------------------------------------------------
# TOC parsing + body start (PLAN.md §6.1 step 3)
# ---------------------------------------------------------------------------

RE_TOC_L1 = re.compile(r"^(ARTICLE|APPENDIX)\s+([A-Z]?\d+)\s*:?\s*(.*?)\s*(\d{1,3}(?:\s*[-–]\s*\d{1,3})?)$")
RE_TOC_MODERN = re.compile(r"^(\d{1,2})\s+([A-Z].*?)\s+(\d{1,3}(?:\s*[-–]\s*\d{1,3})?)$")
RE_TOC_FIN = re.compile(r"^(\d{1,2})\.\s+([A-Z].*?)\s+(\d{1,3}(?:\s*[-–]\s*\d{1,3})?)$")  # financial '1. TITLE … p'
RE_TOC_L2 = re.compile(r"^([A-Z]?\d{1,2}\.\d{1,2})\s+(.*?)(?:\s+(\d{1,3}))?$")
RE_TOC_PREAMBLE = re.compile(r"^PREAMBLE\s+(\d{1,3})$", re.IGNORECASE)


def _toc_entry(line: Line) -> dict[str, Any] | None:
    return _toc_entry_text(_struck_adjusted_text(line))


def _toc_entry_text(raw_text: str) -> dict[str, Any] | None:
    text = re.sub(cfg.RE_LEADERS, " ", raw_text.replace("\xa0", " ").replace("\u2010", "-")).strip()
    text = re.sub(r"\s{2,}", " ", text)
    if not text or cfg.RE_CONTENTS_MARKER.match(text):
        return None
    if text.upper() in ("ART CONTENTS PAGE", "PAGE", "PAGES", "PAGE(S)", "ART CONTENTS PAGE(S)"):
        return None
    m = RE_TOC_L1.match(text)
    if m and m.group(4):
        kind, num, title, page = m.groups()
        return {"article": f"{kind.upper()} {num}", "level": 1, "title": title.strip(" :–-"),
                "printed_page": int(re.match(r"\d+", page).group(0))}
    m = RE_TOC_PREAMBLE.match(text)
    if m:
        return {"article": "PREAMBLE", "level": 1, "title": "Preamble", "printed_page": int(m.group(1))}
    m = RE_TOC_MODERN.match(text)
    if m:
        num, title, page = m.groups()
        return {"article": num, "level": 1, "title": title.strip(),
                "printed_page": int(re.match(r"\d+", page).group(0))}
    m = RE_TOC_FIN.match(text)
    if m:
        num, title, page = m.groups()
        return {"article": num, "level": 1, "title": title.strip(),
                "printed_page": int(re.match(r"\d+", page).group(0))}
    m = RE_TOC_L2.match(text)
    if m:
        num, title, page = m.groups()
        if title and not title[0].isdigit():
            return {"article": num, "level": 2, "title": title.strip(),
                    "printed_page": int(page) if page else None}
    return None


def _toc_line_fragments(line: Line) -> list[Line]:
    """Split a visually-merged line at 2-column TOC breaks (legacy regs):
    a large x-gap where the left part already ends in a page number and the
    right part starts with a fresh entry number, e.g. the merged row
    '3 GENERAL CONDITIONS 2   30 REFUELLING 24' is two entries."""
    if not line.spans:
        return [line]
    frags: list[list[Span]] = [[line.spans[0]]]
    for prev, s in zip(line.spans, line.spans[1:]):
        gap = s.bbox[0] - prev.bbox[2]
        if gap > 40:
            left = "".join(x.text for x in frags[-1]).strip()
            right = s.text.strip()
            if (re.search(r"\d$", left)
                    and re.match(r"(?:ARTICLE|APPENDIX)\b|[A-Z]?\d{1,2}[).\s]", right, re.IGNORECASE)):
                frags.append([])
        frags[-1].append(s)
    out = []
    for f in frags:
        if any(x.text.strip() for x in f):
            out.append(Line(spans=f, bbox=(
                min(x.bbox[0] for x in f), min(x.bbox[1] for x in f),
                max(x.bbox[2] for x in f), max(x.bbox[3] for x in f))))
    return out


def _toc_columns(pd: PageData) -> list[float]:
    """x-starts of TOC columns, detected from repeated 'ART CONTENTS PAGE'
    header cells (legacy regs print 2-column TOCs whose column gap can be
    smaller than the intra-entry title→page gap, so gap-based splitting
    fails; span x0 bucketing by column range is robust)."""
    starts: list[float] = []
    for line in pd.lines:
        if "CONTENTS" not in line.text.upper():
            continue
        arts = [s for s in line.spans if s.text.strip().upper().startswith("ART")]
        starts.extend(s.bbox[0] for s in arts)
    starts.sort()
    cols: list[float] = []
    for x in starts:
        if not cols or x - cols[-1] > 30:
            cols.append(x)
    return cols if len(cols) >= 2 else []


def _column_fragments(line: Line, cols: list[float]) -> list[Line]:
    """Split a merged line's spans into one fragment per TOC column."""
    bounds = cols[1:]  # span belongs to column k if x0 < bounds[k]
    buckets: list[list[Span]] = [[] for _ in cols]
    for s in line.spans:
        k = 0
        while k < len(bounds) and s.bbox[0] >= bounds[k]:
            k += 1
        buckets[k].append(s)
    out = []
    for f in buckets:
        if any(x.text.strip() for x in f):
            out.append(Line(spans=f, bbox=(
                min(x.bbox[0] for x in f), min(x.bbox[1] for x in f),
                max(x.bbox[2] for x in f), max(x.bbox[3] for x in f))))
    return out


RE_TOC_ENTRY_START = re.compile(r"^(?:\d{1,2}(?=\s)|APPENDIX\b|ARTICLE\b|PREAMBLE\b)")


def _toc_column_entries(pd: PageData, cols: list[float]) -> list[str]:
    """Assemble TOC entries per column from word bboxes (robust where spans
    straddle both columns). Wrapped titles and right-aligned page numbers
    on their own line are folded into the preceding entry."""
    def word_col(x0: float) -> int:
        ci = 0
        for k, cs in enumerate(cols):
            if x0 + 2 >= cs:  # tolerance downward only: no double-assignment
                ci = k
        return ci

    entries: list[str] = []
    for ci, col_x0 in enumerate(cols):
        words = [w for w in pd.words if word_col(w[0]) == ci]
        words.sort(key=lambda w: (round(w[1] / 3.0), w[0]))
        rows: list[list[tuple]] = []
        for w in words:
            if rows and abs(w[1] - rows[-1][0][1]) < 3.5:
                rows[-1].append(w)
            else:
                rows.append([w])
        # TOC entries only exist below the 'Art CONTENTS Page(s)' header
        for idx, row in enumerate(rows):
            if "CONTENTS" in " ".join(w[4] for w in row).upper():
                rows = rows[idx + 1:]
                break
        cur: str | None = None
        for row in rows:
            row.sort(key=lambda w: w[0])
            text = " ".join(w[4] for w in row).strip()
            if not text:
                continue
            m = RE_TOC_ENTRY_START.match(text)
            starts_entry = bool(m) and row[0][0] < col_x0 + 40
            if starts_entry and m.group(0)[0].isdigit() and int(m.group(0)) > 60:
                starts_entry = False  # year-like token inside a wrapped title
            if starts_entry or cur is None:
                if cur:
                    entries.append(cur)
                cur = text
            elif re.search(r"\d{1,3}(?:\s*[-–]\s*\d{1,3})?$", cur):
                break  # entry complete; non-entry line -> end of this column's TOC
            else:
                cur += " " + text
        if cur:
            entries.append(cur)
    return entries


def parse_toc(front_pages: list[PageData]) -> list[dict[str, Any]]:
    toc: list[dict[str, Any]] = []
    for pd in front_pages:
        cols = _toc_columns(pd)
        if cols and pd.words:
            for text in _toc_column_entries(pd, cols):
                entry = _toc_entry_text(text)
                if entry:
                    toc.append(entry)
            continue
        for line in pd.lines:
            for frag in _toc_line_fragments(line):
                entry = _toc_entry(frag)
                if entry:
                    toc.append(entry)
    # 2-column TOCs interleave columns row-wise; order by printed page
    toc.sort(key=lambda e: (e["printed_page"] is None, e["printed_page"] or 0))
    # L2 titles can end in digits that are not page numbers ('16.2 Frontal
    # test 1', '19.1 Purpose of Article 19'): fold implausible pages back
    l1_pages = [e["printed_page"] for e in toc if e.get("level") == 1 and e.get("printed_page")]
    min_l1 = min(l1_pages) if l1_pages else None
    for e in toc:
        if e.get("level") == 2 and e.get("printed_page") is not None:
            major = e["article"].split(".")[0]
            if (min_l1 and e["printed_page"] < min_l1) \
                    or (major.isdigit() and e["printed_page"] == int(major)):
                e["title"] = f"{e['title']} {e['printed_page']}".strip()
                e["printed_page"] = None
    return toc


def _toc_like(pd: PageData) -> bool:
    """A page that (mostly) lists TOC entries: ≥5 lines starting with an
    article/number token and ending in a page number (or dotted leaders)."""
    n = 0
    for line in pd.lines:
        text = re.sub(cfg.RE_LEADERS, " ", line.text).strip()
        if _toc_entry(line) and (cfg.RE_LEADERS.search(line.text) or re.search(r"\d{1,3}$", text)):
            n += 1
    return n >= 5


def find_body_start(pages: list[PageData], toc: list[dict[str, Any]]) -> int:
    """Index (into ``pages``) of the first body page (PLAN.md §6.1).

    Primary: the page whose printed number matches the first level-1 TOC
    entry. Fallback: the first non-TOC-like page after the last page
    containing a CONTENTS marker. Last resort: first page with a bold
    level-1-looking heading at index > 0.
    """
    targets = [e["printed_page"] for e in toc if e.get("level") == 1 and e.get("printed_page")]
    if targets:
        first = min(targets)
        want = next((e for e in toc if e.get("level") == 1 and e.get("printed_page") == first), None)
        want_key = _toc_l1_key(want["article"]) if want else None
        for i, pd in enumerate(pages):
            if i > 0 and pd.printed_page == first:
                # verify: a mis-parsed TOC (2-column glue) must not be trusted
                keys = _l1_keys_on_page(pd)
                if want_key is None or want_key in keys:
                    return i
                break
    last_contents: int | None = None
    for i, pd in enumerate(pages):
        if any(cfg.RE_CONTENTS_MARKER.match(l.text.strip()) for l in pd.lines):
            last_contents = i
    if last_contents is not None:
        i = last_contents + 1
        # skip front-matter continuation pages: the first body page always
        # carries a level-1 heading, TOC continuation pages carry no
        # (undecorated) ones
        while i < len(pages) and not _l1_keys_on_page(pages[i]):
            i += 1
        if i < len(pages):
            return i
    for i, pd in enumerate(pages):
        if i == 0:
            continue
        if _l1_keys_on_page(pd):
            return i
    return min((last_contents or 0) + 1, len(pages) - 1)


def _toc_l1_key(article: str) -> str:
    """Kind-qualified key for a TOC level-1 entry ('APPENDIX 1' -> 'APPENDIX-1')."""
    if article == "PREAMBLE":
        return "PREAMBLE-PREAMBLE"
    kind = "APPENDIX" if article.upper().startswith("APPENDIX") else "ARTICLE"
    return f"{kind}-{article.split()[-1]}"


#: TOC heading lines are decorated with a trailing page number / page range
#: ('ARTICLE 3 : BODYWORK AND DIMENSIONS 10-36'); body headings are not.
RE_TRAILING_PAGEREF = re.compile(r"\s+\d{1,3}(?:\s*[-–]\s*\d{1,3})?$")


def _l1_keys_on_page(pd: PageData) -> set[str]:
    keys: set[str] = set()
    for line in pd.lines:
        if not _is_l1(line):
            continue
        text = _struck_adjusted_text(line).strip().replace("\u2010", "-")
        if RE_TRAILING_PAGEREF.search(text):
            continue  # TOC-style decorated line, not a body heading
        try:
            kind, num, _ = _l1_parts(line)
            keys.add(f"{kind}-{num}")
        except Exception:
            pass
    return keys


# ---------------------------------------------------------------------------
# Body structure: line classification and article segmentation (§3.3/§6.4)
# ---------------------------------------------------------------------------

RE_NUM_PREFIX = re.compile(r"^[A-Z]?\d[\d.]*$")
RE_NUM_CONT = re.compile(r"^\.\s?\d[\d.]*$|^[\d.]+$")


def _leading_number_token(line: Line) -> tuple[str | None, int]:
    """Join x-adjacent number-ish spans at line start (handles numbers split
    by recoloring: 'B1.1.'+'4', 'B1.5'+'.1', 'B1.9.'+'. 5').

    Returns ``(token, n_spans_consumed)`` or ``(None, 0)``.

    If the number begins a whole-sentence span ('10.2 Suspension geometry : '),
    the span is split in place so the token stands alone (idempotent).
    """
    for i, s in enumerate(line.spans):
        if not s.text.strip():
            continue
        if not RE_NUM_PREFIX.match(s.text.strip()):
            m = re.match(r"\s*([A-Z]?\d[\d.]*)\s", s.text)
            if not m:
                break
            end = m.end(1)
            frac = end / len(s.text)
            x1 = s.bbox[0] + frac * (s.bbox[2] - s.bbox[0])
            rest = Span(text=s.text[end:], bbox=(x1, s.bbox[1], s.bbox[2], s.bbox[3]),
                        color_int=s.color_int, color_cls=s.color_cls,
                        color_exact=s.color_exact, size=s.size, bold=s.bold,
                        struck=s.struck, highlighted=s.highlighted,
                        strike_frac=s.strike_frac, strike_left=s.strike_left,
                        tag=s.tag, inner=s.inner)
            s.text = s.text[:end]
            s.bbox = (s.bbox[0], s.bbox[1], x1, s.bbox[3])
            line.spans.insert(i + 1, rest)
        break
    token = ""
    n = 0
    for s in line.spans:
        t = s.text.strip()
        if not t:
            if token:
                n += 1  # whitespace inside/below the token run; keep going
            continue
        ok = bool(RE_NUM_PREFIX.match(t)) if not token else bool(RE_NUM_PREFIX.match(t) or RE_NUM_CONT.match(t))
        if ok and len(token) + len(t) <= 14:
            token += t
            n += 1
        else:
            break
    token = re.sub(r"\.\s*", ".", token).rstrip(".")
    if token and cfg.RE_NUM_TOKEN.match(token):
        return token, n
    return None, 0


def _choose_l2_style(pages: list[PageData], body_start: int) -> bool:
    """True if this doc uses bold titled L2 headings ('1.26 Motor Generator
    Unit :'); False for the inline-clause family where an L2 number simply
    starts a body clause ('35.2 After the sprint session ...').

    Classifies by the remainder of each L2-token line: heading family = short
    all-bold remainder (the title); inline family = long / regular-weight
    remainder (body text). Number boldness alone is useless — 2018–2025
    sporting regs bold the clause number AND print body text after it.
    """
    heading_like = inline_like = 0
    for pd in pages[body_start:]:
        for line in pd.lines:
            token, nsp = _leading_number_token(line)
            if not token or not cfg.RE_L2_TOKEN.match(token):
                continue
            rest = [s for s in line.spans[nsp:] if s.text.strip()]
            if not rest:
                continue
            rest_text = "".join(s.text for s in rest).strip()
            bold_chars = sum(len(s.text.strip()) for s in rest if s.bold)
            total_chars = sum(len(s.text.strip()) for s in rest) or 1
            if len(rest_text) <= 90 and bold_chars >= 0.6 * total_chars:
                heading_like += 1
            else:
                inline_like += 1
    if heading_like == 0 and inline_like == 0:
        return True
    return heading_like > inline_like


def _is_l1(line: Line) -> bool:
    if not line.fully_bold or line.max_size < 10.5:
        return False
    text = line.text.strip()
    return bool(
        cfg.RE_L1_ARTICLE.match(text)
        or cfg.RE_L1_PREAMBLE.match(text)
        or (cfg.RE_L1_MODERN.match(text) and len(text) > 3)
        or cfg.RE_L1_FIN.match(text)
    )


def _struck_adjusted_text(line: Line) -> str:
    """Line text with partially-struck digit runs repaired (FIA renumbering
    draws the old digit struck next to the new one in a single span, e.g.
    'APPENDIX B' + '34' where '3' is struck → 'B4')."""
    out = []
    prev: Span | None = None
    for s in line.spans:
        t = s.text
        if t.strip().isdigit() and 0.15 < s.strike_frac < cfg.STRIKE_COVER:
            digits = t.strip()
            k = max(1, round(s.strike_frac * len(digits)))
            kept = digits[k:] if s.strike_left else digits[: len(digits) - k]
            t = t.replace(digits, kept)
        if prev is not None and _needs_space(prev, s):
            out.append(" ")
        out.append(t)
        prev = s
    return "".join(out)


def _l1_parts(line: Line) -> tuple[str, str, str]:
    """-> (kind, number, title); kind in ARTICLE/APPENDIX/PREAMBLE."""
    text = _struck_adjusted_text(line).strip()
    if cfg.RE_L1_PREAMBLE.match(text):
        return "PREAMBLE", "PREAMBLE", "Preamble"
    m = cfg.RE_L1_ARTICLE.match(text)
    if m:
        kind, num, title = m.groups()
        return kind.upper(), num, title.strip(" :–-")
    m = cfg.RE_L1_MODERN.match(text)
    if m:
        return "ARTICLE", m.group(1), m.group(2).strip()
    m = cfg.RE_L1_FIN.match(text)
    if m:
        return "ARTICLE", m.group(1), m.group(2).strip()
    return "ARTICLE", text, ""


def _is_l2(line: Line, bold_style: bool) -> tuple[str, int] | None:
    token, nsp = _leading_number_token(line)
    if not token or not cfg.RE_L2_TOKEN.match(token):
        return None
    if bold_style and not any(s.bold for s in line.spans[:nsp]):
        return None
    return token, nsp


def _is_l3(line: Line, bold_style: bool) -> tuple[str, int] | None:
    token, nsp = _leading_number_token(line)
    if not token or not cfg.RE_L3_TOKEN.match(token):
        return None
    if bold_style and not any(s.bold for s in line.spans[:nsp]):
        return None
    return token, nsp


def _merge_heading_continuations(pages: list[PageData], body_start: int, bold_l2: bool) -> None:
    """Merge wrapped multi-line headings into single lines (e.g. 'APPENDIX 9'
    + 'APPROVED CHANGES FOR SUBSEQUENT YEARS')."""
    for pd in pages[body_start:]:
        merged: list[Line] = []
        for line in pd.lines:
            if (
                merged
                and line.fully_bold
                and line.max_size >= 10.5
                and merged[-1].fully_bold
                and _is_l1(merged[-1])
                and not _leading_number_token(line)[0]
                and not _is_l1(line)
                and abs(line.max_size - merged[-1].max_size) < 2.0
            ):
                prev = merged[-1]
                prev.spans.append(Span(" ", line.bbox, 0, "black", True, line.max_size, True))
                prev.spans.extend(line.spans)
                prev.bbox = (prev.bbox[0], prev.bbox[1], max(prev.bbox[2], line.bbox[2]), line.bbox[3])
                continue
            merged.append(line)
        pd.lines = merged


# ---------------------------------------------------------------------------
# Tag resolution and markdown rendering (§6.2)
# ---------------------------------------------------------------------------


def _resolve_tags(pages: list[PageData], ident: Identity, stats: dict[str, int]) -> None:
    for pd in pages:
        for line in pd.lines:
            for span in line.spans:
                if not span.text.strip():
                    continue
                if span.color_cls == "unknown":
                    stats["unknown_color_spans"] = stats.get("unknown_color_spans", 0) + 1
                elif not span.color_exact:
                    stats["near_color_spans"] = stats.get("near_color_spans", 0) + 1
                if span.struck:
                    span.tag = "REMOVED"
                    inner = ident.color_tags.get(span.color_cls)
                    span.inner = inner if inner and inner != "CHANGED" else None
                    stats["removed_spans"] = stats.get("removed_spans", 0) + 1
                elif span.highlighted:
                    span.tag = "CHANGED"
                    stats["highlighted_spans"] = stats.get("highlighted_spans", 0) + 1
                    stats["changed_spans"] = stats.get("changed_spans", 0) + 1
                else:
                    span.tag = ident.color_tags.get(span.color_cls)
                    if span.tag:
                        stats[f"{span.tag.lower()}_spans"] = stats.get(f"{span.tag.lower()}_spans", 0) + 1


def _needs_space(prev: Span, span: Span) -> bool:
    """PyMuPDF span texts lack inter-span whitespace when spans are
    x-separated (TOC columns, tables). Insert a synthetic space when the
    horizontal gap is wide enough; keep word-internal splits (colour changes
    mid-word) glued."""
    if prev.text.endswith((" ", "\u00a0")) or span.text.startswith((" ", "\u00a0")):
        return False
    gap = span.bbox[0] - prev.bbox[2]
    return gap > max(1.0, 0.22 * min(prev.size, span.size))


def _runs_of(line: Line, skip_spans: int = 0) -> list[tuple[tuple[str | None, str | None], str]]:
    runs: list[tuple[tuple[str | None, str | None], str]] = []
    prev_span: Span | None = None
    for span in line.spans[skip_spans:]:
        text = span.text
        if prev_span is not None and _needs_space(prev_span, span):
            text = " " + text
        if span.text.strip():
            key = (span.tag, span.inner)
        else:  # whitespace inherits the surrounding run so it never splits a tag
            key = runs[-1][0] if runs else (None, None)
        if runs and runs[-1][0] == key:
            runs[-1] = (key, runs[-1][1] + text)
        else:
            runs.append((key, text))
        prev_span = span
    return runs


RE_TAG_TOKEN = re.compile(r"(?<!\\)\[/?[A-Z]+\]")


def _escape_brackets(text: str) -> str:
    """Source text can contain literal square brackets (legend samples like
    '[Red Text]', '[TO BE ADDED]') — escape them so they never collide with
    the [CHANGED]/[REMOVED] tag syntax."""
    return text.replace("[", "\\[").replace("]", "\\]")


def _render_runs(runs: list[tuple[tuple[str | None, str | None], str]]) -> str:
    out = []
    for (tag, inner), text in runs:
        if tag is None:
            out.append(_escape_brackets(text))
        elif inner:
            out.append(f"[{tag}][{inner}]{_escape_brackets(text.strip())}[/{inner}][/{tag}]")
        else:
            out.append(f"[{tag}]{_escape_brackets(text.strip())}[/{tag}]")
    text = "".join(out)
    text = re.sub(r" \[/?", lambda m: m.group(0)[1:] if m.group(0).startswith(" [/") else m.group(0), text)
    text = re.sub(r"\[([A-Z]+)\] +", r"[\1]", text)
    text = re.sub(r" +(\[/[A-Z]+\])", r"\1", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def _merge_para_lines(lines: list[Line], skip_first: int = 0) -> tuple[str, set[str], int, int]:
    """Join paragraph lines into tagged markdown, merging tags across line
    breaks so output reads ``[CHANGED]…multi-line…[/CHANGED]`` (§6.2)."""
    merged: list[tuple[tuple[str | None, str | None], str]] = []
    tags: set[str] = set()
    n_changed = n_removed = 0
    for i, line in enumerate(lines):
        for span in line.spans:
            if span.tag == "CHANGED":
                n_changed += 1
            if span.tag == "REMOVED":
                n_removed += 1
            if span.tag:
                tags.add(span.tag)
            if span.inner:
                tags.add(span.inner)
        runs = _runs_of(line, skip_spans=skip_first if i == 0 else 0)
        if not runs:
            continue
        if i > 0 and merged:
            key, t = merged[-1]
            merged[-1] = (key, t.rstrip() + " ")
        for key, text in runs:
            if merged and merged[-1][0] == key:
                merged[-1] = (key, merged[-1][1] + text)
            else:
                merged.append((key, text))
    return _render_runs(merged), tags, n_changed, n_removed


# ---------------------------------------------------------------------------
# Tables (PLAN.md §2 decision 9)
# ---------------------------------------------------------------------------


def _detect_tables(page: pymupdf.Page) -> list[tuple[tuple, list[list[Any]]]]:
    height = page.rect.height
    out = []
    try:
        found = page.find_tables()
    except Exception:
        return out
    for tab in found.tables:
        bbox = tuple(tab.bbox)
        if bbox[1] < height * cfg.HEADER_ZONE or bbox[3] > height * cfg.FOOTER_ZONE:
            continue  # furniture region
        rows = [list(row.cells) for row in tab.rows]
        out.append((bbox, rows))
    return out


def _cell_text(cell_bbox: tuple, lines_in_table: list[Line]) -> tuple[str, set[str], int, int]:
    cell = pymupdf.Rect(cell_bbox)
    runs: list[tuple[tuple[str | None, str | None], str]] = []
    tags: set[str] = set()
    n_changed = n_removed = 0
    for line in lines_in_table:
        for span in line.spans:
            sb = pymupdf.Rect(span.bbox)
            cx, cy = (sb.x0 + sb.x1) / 2, (sb.y0 + sb.y1) / 2
            if cell.contains(pymupdf.Point(cx, cy)):
                if span.tag == "CHANGED":
                    n_changed += 1
                if span.tag == "REMOVED":
                    n_removed += 1
                if span.tag:
                    tags.add(span.tag)
                if span.text.strip():
                    key = (span.tag, span.inner)
                else:
                    key = runs[-1][0] if runs else (None, None)
                if runs and runs[-1][0] == key:
                    runs[-1] = (key, runs[-1][1] + span.text)
                else:
                    runs.append((key, span.text))
    return _render_runs(runs).replace("|", "\\|"), tags, n_changed, n_removed


def _extract_tables(pd: PageData, stats: dict[str, int]) -> list[Block]:
    """Render detected tables as pipe tables; consume covered body lines."""
    blocks: list[Block] = []
    for bbox, rows in pd.raw_tables:
        if len(rows) < 2 or sum(1 for c in rows[0] if c) < 2:
            continue
        trect = pymupdf.Rect(bbox)
        covered = [
            line for line in pd.lines
            if trect.contains(pymupdf.Point((line.bbox[0] + line.bbox[2]) / 2, (line.bbox[1] + line.bbox[3]) / 2))
        ]
        if not covered:
            continue
        grid: list[list[str]] = []
        tags: set[str] = set()
        n_changed = n_removed = 0
        for row in rows:
            out_row = []
            for cell in row:
                if cell is None:
                    out_row.append("")
                    continue
                text, ctags, cch, crem = _cell_text(cell, covered)
                tags |= ctags
                n_changed += cch
                n_removed += crem
                out_row.append(text)
            grid.append(out_row)
        # drop fully-empty filler rows; synthesise a header if none was detected
        grid = [row for row in grid if any(c.strip() for c in row)]
        if not grid:
            continue
        if not any(c.strip() for c in grid[0]):
            grid[0] = [f"C{i + 1}" for i in range(len(grid[0]))]
        pd.tables.append({
            "pdf_page": pd.pdf_page,
            "printed_page": pd.page_label,
            "bbox": [round(v, 1) for v in bbox],
            "cells": [[RE_TAG_TOKEN.sub("", c).replace("\\[", "[").replace("\\]", "]") for c in row] for row in grid],
        })
        header, *body_rows = grid
        pipe = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        pipe += ["| " + " | ".join(row) + " |" for row in body_rows]
        for line in covered:
            line.spans = []
        pd.lines = [l for l in pd.lines if l.spans]
        stats["tables"] = stats.get("tables", 0) + 1
        blocks.append(Block(kind="table", text="\n".join(pipe), page=pd.pdf_page,
                            printed=pd.page_label, y0=bbox[1], tags=tags,
                            n_changed=n_changed, n_removed=n_removed))
    return blocks


# ---------------------------------------------------------------------------
# Segmentation: lines → article segments
# ---------------------------------------------------------------------------


def segment_body(doc: pymupdf.Document, pages: list[PageData], body_start: int, bold_l2: bool, stats: dict[str, int]) -> list[ArticleSeg]:
    segments: list[ArticleSeg] = []
    parent: str | None = None
    parent_pending: list[Block] = []
    current: ArticleSeg | None = None
    parent_has_l2 = False
    pending_l1: tuple[str, str, str] | None = None  # (kind, num, title)

    para_lines: list[Line] = []
    para_clause: str | None = None
    last_y: float | None = None
    last_page = -1
    pending_side: Line | None = None  # standalone bold line, maybe a clause side-title

    def flush_side(page: int, printed: int) -> None:
        """Emit a pending standalone bold line as its own bold paragraph."""
        nonlocal pending_side
        if pending_side is None:
            return
        text, tags, nch, nrem = _merge_para_lines([pending_side])
        if text.strip():
            block = Block(kind="para", text=f"**{text.strip()}**", page=page,
                          printed=printed, tags=tags, n_changed=nch, n_removed=nrem)
            (current.blocks if current is not None else parent_pending).append(block)
        pending_side = None

    def flush_para(page: int, printed: int) -> None:
        nonlocal para_lines, para_clause, last_y
        if not para_lines:
            return
        skip = 0
        if para_clause:
            _, skip = _leading_number_token(para_lines[0]) or (None, 0)
        text, tags, nch, nrem = _merge_para_lines(para_lines, skip_first=skip)
        if para_clause:
            text = f"**{para_clause}** " + text
        if text.strip():
            block = Block(kind="para", text=text, page=page, printed=printed,
                          tags=tags, n_changed=nch, n_removed=nrem, clause=para_clause)
            (current.blocks if current is not None else parent_pending).append(block)
        para_lines = []
        para_clause = None
        last_y = None

    def flush_orphan_l1() -> None:
        """An L1 group with no L2 children becomes its own row (appendices)."""
        nonlocal pending_l1, parent_pending, parent_has_l2
        if pending_l1 is None and parent_pending:
            # body content before the first L1 heading — keep it as its own
            # row instead of dropping it
            seg = ArticleSeg(number="BODY_INTRO", title="Body introduction",
                             parent=None, level=1)
            seg.blocks = parent_pending
            segments.append(seg)
            parent_pending = []
        elif pending_l1 is not None and not parent_has_l2:
            kind, num, title = pending_l1
            number = f"{kind}-{num}" if kind == "APPENDIX" else num
            seg = ArticleSeg(number=number, title=title, parent=None, level=1)
            seg.blocks = parent_pending
            segments.append(seg)
            parent_pending = []
        parent_has_l2 = False

    for pd in pages[body_start:]:
        pd.raw_tables = _detect_tables(doc[pd.pdf_page - 1])
        table_blocks = _extract_tables(pd, stats)
        emitted_marker = False

        def ensure_marker() -> None:
            nonlocal emitted_marker
            if not emitted_marker:
                flush_para(pd.pdf_page, pd.page_label)
                marker = Block(kind="pagemarker", text=f"[PAGE {pd.page_label}]",
                               page=pd.pdf_page, printed=pd.page_label)
                (current.blocks if current is not None else parent_pending).append(marker)
                emitted_marker = True

        items: list[tuple[float, int, Any]] = [(tb.y0, 0, tb) for tb in table_blocks]
        items += [(line.y0, 1, line) for line in pd.lines]
        items.sort(key=lambda it: (it[0], it[1]))

        for _, kind, obj in items:
            if kind == 0:
                ensure_marker()
                flush_para(pd.pdf_page, pd.page_label)
                flush_side(pd.pdf_page, pd.page_label)
                (current.blocks if current is not None else parent_pending).append(obj)
                continue
            line: Line = obj
            text = line.text.strip()
            if not text:
                continue
            if _is_l1(line):
                ensure_marker()
                flush_para(pd.pdf_page, pd.page_label)
                flush_side(pd.pdf_page, pd.page_label)
                flush_orphan_l1()
                kind1, num, title = _l1_parts(line)
                pending_l1 = (kind1, num, title)
                parent = f"{num} — {title}" if title else num
                current = None
                if kind1 == "PREAMBLE":
                    heading_text = "# PREAMBLE"
                else:
                    heading_text = f"# {kind1} {num}: {title}" if title else f"# {kind1} {num}"
                heading = Block(kind="heading1", text=heading_text.rstrip(" :"),
                                page=pd.pdf_page, printed=pd.page_label, y0=line.y0)
                heading.tags = {s.tag for s in line.spans if s.tag}
                heading.n_changed = sum(1 for s in line.spans if s.tag == "CHANGED")
                heading.n_removed = sum(1 for s in line.spans if s.tag == "REMOVED")
                parent_pending.append(heading)
                if kind1 == "ARTICLE":
                    # visual-row merging can glue the first L2 heading onto the
                    # L1 line: 'ARTICLE 10 : SUSPENSION …  10.1 Sprung suspension'
                    m2 = re.search(rf"\s+({re.escape(num)}\.\d{{1,2}})\s+", title)
                    if m2:
                        l2tok, l2title = m2.group(1), title[m2.end():].strip()
                        title = title[:m2.start()].strip()
                        heading.text = f"# {kind1} {num}: {title}".rstrip(" :")
                        pending_l1 = (kind1, num, title)
                        parent = f"{num} — {title}" if title else num
                        seg = ArticleSeg(number=l2tok, title=l2title, parent=parent, level=2)
                        h2 = Block(kind="heading2", text=f"## {l2tok} {l2title}".rstrip(),
                                   page=pd.pdf_page, printed=pd.page_label, y0=line.y0)
                        h2.tags = {s.tag for s in line.spans if s.tag}
                        h2.n_changed = sum(1 for s in line.spans if s.tag == "CHANGED")
                        h2.n_removed = sum(1 for s in line.spans if s.tag == "REMOVED")
                        seg.blocks = parent_pending + [h2]
                        parent_pending = []
                        parent_has_l2 = True
                        segments.append(seg)
                        current = seg
                continue
            l2 = _is_l2(line, bold_l2)
            if l2:
                ensure_marker()
                flush_para(pd.pdf_page, pd.page_label)
                token, nsp = l2
                rest = line.spans[nsp:]
                side_title: str | None = None
                if pending_side is not None:
                    side_title = pending_side.text.strip()
                    pending_side = None
                if bold_l2:
                    # heading family: remainder of the line is the article title
                    title = " ".join(s.text for s in rest if s.text.strip()).strip()
                    if side_title:
                        title = f"{side_title} {title}".strip()
                    rest = []
                else:
                    # inline-number family (financial regs): the number is a
                    # clause marker — remainder is body text, not a title.
                    # Exception: a short all-bold remainder is a side-heading.
                    cand = " ".join(s.text for s in rest if s.text.strip()).strip()
                    if cand and len(cand) <= 80 and all(s.bold for s in rest if s.text.strip()):
                        title = cand
                        rest = []
                    else:
                        title = ""
                    if side_title:
                        title = f"{side_title} {title}".strip()
                seg = ArticleSeg(number=token, title=title, parent=parent, level=2)
                heading = Block(kind="heading2", text=f"## {token} {title}".rstrip(),
                                page=pd.pdf_page, printed=pd.page_label, y0=line.y0)
                heading.tags = {s.tag for s in line.spans if s.tag}
                heading.n_changed = sum(1 for s in line.spans if s.tag == "CHANGED")
                heading.n_removed = sum(1 for s in line.spans if s.tag == "REMOVED")
                seg.blocks = parent_pending + [heading]
                parent_pending = []
                parent_has_l2 = True
                segments.append(seg)
                current = seg
                if rest and any(s.text.strip() for s in rest):
                    para_lines.append(Line(spans=rest, bbox=line.bbox))
                    last_y = line.bbox[3]
                    last_page = pd.pdf_page
                continue
            l3 = _is_l3(line, bold_l2)
            if l3 is None and not cfg.RE_ITEM.match(text) and len(text) <= 80 \
                    and all(s.bold for s in line.spans if s.text.strip()):
                # standalone bold line: hold — may be the side-title of the
                # next clause (financial regs) or a run-in sub-heading
                flush_para(pd.pdf_page, pd.page_label)
                if pending_side is not None:  # wrapped side-title: merge lines
                    pending_side.spans = pending_side.spans + [Span(text=" ", bbox=(0, 0, 0, 0),
                                        color_int=0, color_cls="black", color_exact=True,
                                        size=0.0, bold=True)] + line.spans
                else:
                    pending_side = line
                continue
            flush_side(pd.pdf_page, pd.page_label)
            new_para = False
            if l3:
                new_para = True
            elif cfg.RE_ITEM.match(text) and len(text) < 200:
                new_para = True
            elif last_y is not None and line.y0 - last_y > 1.8 * max(line.max_size, 9):
                new_para = True
            elif last_page != pd.pdf_page:
                new_para = True
            if not para_lines:
                ensure_marker()
                new_para = True
            if new_para:
                flush_para(pd.pdf_page, pd.page_label)
            if l3:
                para_clause = l3[0]
            para_lines.append(line)
            last_y = line.bbox[3]
            last_page = pd.pdf_page
        if not emitted_marker:
            ensure_marker()  # content-free page (blank/image-only): still mark it
        flush_para(pd.pdf_page, pd.page_label)
        flush_side(pd.pdf_page, pd.page_label)
    flush_orphan_l1()

    # occurrence indices for duplicate numbers (PLAN.md §6.5)
    seen: dict[str, int] = {}
    dup = 0
    for seg in segments:
        seg.occurrence = seen.get(seg.number, 0)
        seen[seg.number] = seg.occurrence + 1
        if seg.occurrence:
            dup += 1
    if dup:
        stats["duplicate_articles"] = dup
    return segments


# ---------------------------------------------------------------------------
# Front matter (PLAN.md §2 decision 8: kept as a FRONT_MATTER article row)
# ---------------------------------------------------------------------------


def front_matter_blocks(pages: list[PageData], body_start: int) -> list[Block]:
    blocks: list[Block] = []
    for pd in pages[:body_start]:
        blocks.append(Block(kind="pagemarker", text=f"[PAGE {pd.page_label}]",
                            page=pd.pdf_page, printed=pd.page_label))
        page_lines = [l for l in pd.lines if l.text.strip()]
        if not page_lines:
            continue
        for line in page_lines:
            text, tags, nch, nrem = _merge_para_lines([line])
            if text.strip():
                blocks.append(Block(kind="para", text=text, page=pd.pdf_page,
                                    printed=pd.page_label, tags=tags,
                                    n_changed=nch, n_removed=nrem))
    return blocks


# ---------------------------------------------------------------------------
# Assembly (PLAN.md §5.4 sidecar shape; §7 offsets for slicing)
# ---------------------------------------------------------------------------


def render_document(
    ident: Identity,
    front: list[Block],
    segments: list[ArticleSeg],
    stats: dict[str, int],
) -> tuple[str, list[dict[str, Any]]]:
    pieces: list[str] = []
    cursor = 0

    def emit(text: str) -> int:
        nonlocal cursor
        start = cursor
        pieces.append(text)
        cursor += len(text)
        return start

    emit(cfg.normalize_text(f"# {ident.title} — Issue {ident.issue:02d}\n\n", stats))

    articles_meta: list[dict[str, Any]] = []

    def emit_seg(number: str, title: str, parent: str | None, occurrence: int, blocks: list[Block]) -> None:
        start = cursor
        pages = [b.page for b in blocks]
        printeds = [b.printed for b in blocks]
        tags: set[str] = set()
        nch = nrem = 0
        clauses: list[dict[str, Any]] = []
        body_parts: list[str] = []
        rel = 0
        for b in blocks:
            tags |= b.tags
            nch += b.n_changed
            nrem += b.n_removed
            # normalize per block so recorded offsets match the final md
            body_parts.append(cfg.normalize_text(b.text, stats) + "\n\n")
            if b.clause:
                clauses.append({"number": b.clause, "offset": rel, "length": 0})
            rel += len(b.text) + 2
        for i in range(len(clauses)):
            end = clauses[i + 1]["offset"] if i + 1 < len(clauses) else rel
            clauses[i]["length"] = end - clauses[i]["offset"]
        emit("".join(body_parts))
        articles_meta.append({
            "article_number": number,
            "article_title": title,
            "parent_article": parent,
            "occurrence": occurrence,
            "pdf_page_start": min(pages) if pages else None,
            "pdf_page_end": max(pages) if pages else None,
            "printed_page_start": min(printeds) if printeds else None,
            "printed_page_end": max(printeds) if printeds else None,
            "offset": start,
            "length": cursor - start,
            "has_changes": "CHANGED" in tags,
            "has_removals": "REMOVED" in tags,
            "has_governance": "GOVERNANCE" in tags,
            "has_reference": "REFERENCE" in tags,
            "has_comment": "COMMENT" in tags,
            "n_changed": nch,
            "n_removed": nrem,
            "clauses": clauses,
        })

    emit_seg("FRONT_MATTER", "Front matter (cover, convention, contents)", None, 0, front)
    for seg in segments:
        emit_seg(seg.number, seg.title, seg.parent, seg.occurrence, seg.blocks)

    md = "".join(pieces).rstrip() + "\n"
    stats["articles"] = len(articles_meta)
    return md, articles_meta


# ---------------------------------------------------------------------------
# Main per-PDF extraction
# ---------------------------------------------------------------------------


def output_paths(ident: Identity, pdf_path: Path) -> tuple[Path, Path]:
    iss_dir = f"iss-{ident.issue:02d}" + (f"-v{ident.variant}" if ident.variant is not None else "")
    out_dir = cfg.EXTRACTED_ROOT / str(ident.year) / ident.slug / iss_dir
    return out_dir / f"{pdf_path.stem}.md", out_dir / f"{pdf_path.stem}.json"


def extract_pdf(pdf_path: Path, force: bool = False) -> tuple[str, str]:
    """Extract one PDF. Returns ``(status, doc_id)``; status=ok/cached."""
    source_hash = cfg.sha256_file(pdf_path)
    doc = pymupdf.open(pdf_path)
    try:
        ident = identify(doc, pdf_path)
        md_path, json_path = output_paths(ident, pdf_path)
        if not force and json_path.exists():
            try:
                existing = json.loads(json_path.read_text(encoding="utf-8"))
                if (existing.get("source_hash") == source_hash
                        and existing.get("extractor_version") == cfg.EXTRACTOR_VERSION):
                    return "cached", ident.doc_id
            except (OSError, json.JSONDecodeError):
                pass

        stats: dict[str, int] = {}
        pages = _read_pages(doc)
        # Footer page numbers are unreliable in places: source misprints
        # (2021 sporting iss-11 pdf p3 reads "8") and junk numbers on front
        # matter (2022 sporting iss-07 cover reads "42"). The true sequence
        # increments by 1 per page at a constant pdf−printed offset; keep
        # only pages matching the dominant offset, drop the rest.
        offsets = Counter(pd.pdf_page - pd.printed_page
                          for pd in pages if pd.printed_page is not None)
        if offsets:
            dominant = offsets.most_common(1)[0][0]
            for pd in pages:
                if pd.printed_page is not None and pd.pdf_page - pd.printed_page != dominant:
                    pd.printed_page = None
        # Trailing table sections can restart printed numbering (e.g. 2019
        # technical incidence tables) — keep the first occurrence, else the
        # page label falls back to the absolute pdf page.
        seen_printed: set[int] = set()
        for pd in pages:
            if pd.printed_page is not None:
                if pd.printed_page in seen_printed:
                    pd.printed_page = None
                else:
                    seen_printed.add(pd.printed_page)
        _resolve_tags(pages, ident, stats)
        body_start = find_body_start(pages, [])
        toc = parse_toc(pages[:body_start])
        body_start2 = find_body_start(pages, toc)  # refine with toc knowledge
        if body_start2 != body_start:
            body_start = body_start2
            toc = parse_toc(pages[:body_start])
        bold_l2 = _choose_l2_style(pages, body_start)
        _merge_heading_continuations(pages, body_start, bold_l2)
        front = front_matter_blocks(pages, body_start)
        segments = segment_body(doc, pages, body_start, bold_l2, stats)
        stats["pages"] = doc.page_count
        stats["strike_rects"] = sum(pd.n_strike_rects for pd in pages)
        md, articles_meta = render_document(ident, front, segments, stats)
        for meta in articles_meta:  # keep titles consistent with normalized md
            if meta["article_title"]:
                meta["article_title"] = cfg.normalize_text(meta["article_title"])

        sidecar: dict[str, Any] = {
            "doc_id": ident.doc_id,
            "title": ident.title,
            "year": ident.year,
            "section": ident.section,
            "regulation_type": ident.regulation_type,
            "section_name": ident.section_name,
            "issue": ident.issue,
            "variant": ident.variant,
            "status": ident.status,
            "issue_date": ident.issue_date,
            "wmsc_approval_date": ident.wmsc_approval_date,
            "pages": doc.page_count,
            "source_pdf": cfg.rel_source(pdf_path),
            "source_hash": source_hash,
            "convention": ident.convention,
            "toc": toc,
            "tables": [t for pd in pages for t in pd.tables],
            "articles": articles_meta,
            "stats": stats,
            "extractor_version": cfg.EXTRACTOR_VERSION,
            "extracted_at": cfg.now_utc(),
        }

        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(md, encoding="utf-8")
        json_path.write_text(json.dumps(sidecar, indent=1, ensure_ascii=False), encoding="utf-8")
        return "ok", ident.doc_id
    finally:
        doc.close()


def discover_pdfs() -> list[Path]:
    return sorted(cfg.REGS_DIR.rglob("*.pdf"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract FIA regulation PDFs to markdown + JSON sidecars")
    parser.add_argument("--pdf", type=Path, action="append", help="extract specific PDF(s); repeatable")
    parser.add_argument("--force", action="store_true", help="ignore the source_hash cache")
    args = parser.parse_args()

    pdfs = args.pdf if args.pdf else discover_pdfs()
    missing = [p for p in pdfs if not p.exists()]
    if not pdfs or missing:
        print(f"no such pdf: {missing[0] if missing else '(none)'}", file=sys.stderr)
        return 1

    run: dict[str, Any] = {"ran_at": cfg.now_utc(), "ok": 0, "cached": 0, "extraction_errors": []}
    # shard workers (GNU parallel) write per-shard manifests to avoid clobbering
    shard = os.environ.get("REGS_SHARD")
    manifest_path = (cfg.RUN_MANIFEST_PATH.with_name(
        cfg.RUN_MANIFEST_PATH.stem + f".{shard}.json") if shard else cfg.RUN_MANIFEST_PATH)
    for i, pdf in enumerate(pdfs, 1):
        try:
            status, doc_id = extract_pdf(pdf, force=args.force)
            run[status] += 1
            print(f"[{i:3d}/{len(pdfs)}] {status:6s} {doc_id:45s} {pdf.name}")
        except Exception as exc:  # one bad PDF never blocks the batch (§6.1.8)
            run["extraction_errors"].append({"pdf": cfg.rel_source(pdf), "error": f"{type(exc).__name__}: {exc}"})
            print(f"[{i:3d}/{len(pdfs)}] ERROR  {pdf.name}: {exc}", file=sys.stderr)

    cfg.EXTRACTED_ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(run, indent=1), encoding="utf-8")
    print(f"\ndone: {run['ok']} extracted, {run['cached']} cached, {len(run['extraction_errors'])} errors")
    return 1 if run["extraction_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
