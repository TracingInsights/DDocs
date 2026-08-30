"""Shared paths, constants, and helpers for the FIA regulations pipeline.

Per regs_pipeline/PLAN.md §3 (verified PDF facts), §4 (layout), §6 (extraction
rules). Imported by ``extract_markdown.py``, ``build_articles.py``,
``convert_to_parquet.py`` and ``verify.py``.
"""

from __future__ import annotations

import datetime
import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths & constants (PLAN.md §4)
# ---------------------------------------------------------------------------

PIPELINE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PIPELINE_DIR.parent
REGS_DIR = REPO_ROOT / "regs"
REGS_MANIFEST_PATH = REGS_DIR / "manifest.json"
EXTRACTED_ROOT = PIPELINE_DIR / "extracted"
DATA_DIR = PIPELINE_DIR / "data"
WORK_DB_DIR = DATA_DIR / "_work"
RUN_MANIFEST_PATH = EXTRACTED_ROOT / "_run_manifest.json"

#: Semantic tags emitted into markdown (PLAN.md §2 decision 3).
TAGS = ["CHANGED", "REMOVED", "GOVERNANCE", "REFERENCE", "COMMENT"]

#: Tag precedence, outer → inner (PLAN.md §6.2). REMOVED always wins outermost.
TAG_PRECEDENCE = ["REMOVED", "GOVERNANCE", "REFERENCE", "COMMENT", "CHANGED"]

# ---------------------------------------------------------------------------
# Color classification (PLAN.md §3.1, §6.3)
# ---------------------------------------------------------------------------
#
# Span colors are matched exactly first, then by nearest neighbour in RGB
# space within COLOR_TOLERANCE (colors drift across generator versions, e.g.
# 0xff00ff vs 0xff40ff pink, 0x002d5f vs 0x002060 navy — both observed).

COLOR_CLASSES: dict[str, tuple[float, float, float]] = {
    "black": (0.0, 0.0, 0.0),
    "pink": (1.0, 0.0, 1.0),
    "red": (0.75, 0.0, 0.0),
    "orange": (1.0, 0.6, 0.0),
    "green": (0.0, 0.69, 0.31),
    "navy": (0.0, 0.18, 0.37),
    "white": (1.0, 1.0, 1.0),
    "gray": (0.65, 0.65, 0.65),  # form-field / diagram text in appendices
    "blue": (0.0, 0.0, 1.0),     # hyperlinks (e.g. federalreserve.gov refs)
    "purple": (0.44, 0.19, 0.63),  # 0x7030A0 — 2022+ change marker
    "fuchsia": (0.957, 0.243, 0.973),  # 0xF43EF8 — 2026 definitions styling
}

COLOR_EXACT: dict[int, str] = {
    0x000000: "black",
    0xFF00FF: "pink",
    0xFF40FF: "pink",  # section-d variant
    0xB02418: "red",
    0xC00000: "red",
    0xFF9900: "orange",
    0x00B050: "green",
    0x002D5F: "navy",
    0x002060: "navy",  # section-d variant
    0xFFFFFF: "white",
    0xA6A6A6: "gray",
    0x0000FF: "blue",  # hyperlink text
    0x00B0F0: "blue",  # hyperlink text, Office-theme variant
    0x0070C0: "blue",  # hyperlink text, older Office theme
    0xFF97FF: "pink",  # pale pink changed text (PU sporting regs)
    0xFF33CC: "pink",  # pink variant (2018-era generator)
    0xFF99FF: "pink",  # pale pink variant (2022-era generator)
    0xFF0000: "red",   # pure red (2020-21 notes; legend decides the tag)
    0x008000: "green",  # pure green (Office standard palette)
    0x0563C1: "blue",   # Word hyperlink theme color
    0x747474: "gray",   # dark gray structural text (2026 section-c)
    0x808080: "gray",   # mid gray
    0x7030A0: "purple",  # Office "Purple, Accent 4" (2022+ change marker)
    0x9933FF: "purple",  # purple variant (2027 section-c legend)
    0xF43EF8: "fuchsia",  # cross-book reference styling (2026 definitions)
    0xCC00CC: "pink",    # magenta table marks (2026 section-c)
    0x92D050: "green",   # Office "Green, Accent 6" (editorial notes)
    0x4EA72E: "green",   # green variant (2026 section-c notes)
    0x538135: "green",   # dark green (Office theme)
    0x70AD47: "green",   # Office "Green, Accent 6, Lighter 40%"? (notes)
    0x467886: "blue",    # teal hyperlinks (2026 section-a)
    0x5B9BD5: "blue",    # Office "Blue, Accent 1"
    0x365F91: "navy",    # dark blue heading variant
    0x7F7F7F: "gray",    # mid gray table text
    0x7F807F: "gray",
    0xD1D1D1: "gray",
    0xD9D9D9: "gray",
    0xDADADA: "gray",
    0xC45911: "orange",  # burnt orange (Office "Orange, Accent 2, Darker 50%")
    0xC65911: "orange",
    0x833C0B: "orange",
    0xEF6950: "orange",  # salmon — editorial residue, whitespace-only
}

#: Max squared RGB distance for the nearest-neighbour fallback.
COLOR_TOLERANCE = 0.06

#: Default colour-class → tag. Overridden per-document by the cover-page
#: convention legend when one is parsed (PLAN.md §6.3): e.g. 2024 docs use
#: dark red for "changes previously approved" → CHANGED, not GOVERNANCE.
DEFAULT_COLOR_TAGS: dict[str, str | None] = {
    "black": None,
    "pink": "CHANGED",
    "red": "GOVERNANCE",
    "orange": "REFERENCE",
    "green": "COMMENT",
    "navy": None,  # structural headings / page furniture
    "white": None,  # page furniture (banner text)
    "gray": None,
    "blue": None,  # hyperlinks — informational, not a change marker
    "purple": "CHANGED",  # FIA purple marks changes (2027 legend: "changes relative to …")
    "fuchsia": None,  # 2026 definitions styling — presentational, not a change marker
}


def classify_color(color_int: int) -> tuple[str, bool]:
    """Map a span color int to ``(class, exact)``.

    Exact ints first; fallback = nearest neighbour in RGB space within
    COLOR_TOLERANCE. Unclassifiable → ``("unknown", False)`` (counted in
    ``stats.unknown_color_spans``, surfaced by verify.py).
    """
    if color_int in COLOR_EXACT:
        return COLOR_EXACT[color_int], True
    r = ((color_int >> 16) & 0xFF) / 255.0
    g = ((color_int >> 8) & 0xFF) / 255.0
    b = (color_int & 0xFF) / 255.0
    best, best_d = None, COLOR_TOLERANCE
    for name, (cr, cg, cb) in COLOR_CLASSES.items():
        d = (r - cr) ** 2 + (g - cg) ** 2 + (b - cb) ** 2
        if d < best_d:
            best, best_d = name, d
    if best is None:
        return "unknown", False
    return best, False


# ---------------------------------------------------------------------------
# Strikethrough / highlight geometry (PLAN.md §3.2)
# ---------------------------------------------------------------------------

STRIKE_MAX_H = 1.7  # pt — strike rects are ~0.4–0.6 pt tall, often paired
STRIKE_MIN_W = 3.0  # pt
STRIKE_COVER = 0.6  # fraction of span width that must be struck

HIGHLIGHT_MIN_H = 6.0  # pt — highlight rects are line-height tall
HIGHLIGHT_MIN_W = 8.0  # pt
HIGHLIGHT_COVER = 0.6  # fraction of span bbox covered by highlight

# ---------------------------------------------------------------------------
# Furniture stripping (PLAN.md §6.1 step 4)
# ---------------------------------------------------------------------------

#: Margin zones as fractions of page height (header above, footer below).
HEADER_ZONE = 0.06
FOOTER_ZONE = 0.90

#: Furniture text patterns (applied to margin-zone spans).
RE_FOOTER_PAGE_NUM = re.compile(r"^([A-F])?(\d{1,3})(?:\s*/\s*(\d{1,4}))?$")
RE_COPYRIGHT = re.compile(r"©\s*\d{4}\s+F[ée]d[ée]ration", re.IGNORECASE)
RE_FURNITURE_ISSUE = re.compile(r"^Issue\s+\d+[A-Za-z]?$", re.IGNORECASE)
RE_SECTION_BANNER = re.compile(r"^SECTION\s+[A-F]\s*:", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Article structure (PLAN.md §3.3, §6.4)
# ---------------------------------------------------------------------------

#: Number token: optional section letter + 1–3 dotted components (B1.1, 5.4, 5.12.7).
RE_NUM_TOKEN = re.compile(r"^([A-Z]?)(\d+(?:\.\d+){0,3})$")
#: Level-2 number: optional letter + exactly two components (B1.1 / 5.4).
RE_L2_TOKEN = re.compile(r"^([A-Z]?)(\d{1,2}\.\d{1,2})$")
#: Level-3 clause number: optional letter + exactly three components (B1.1.1 / 5.12.7).
RE_L3_TOKEN = re.compile(r"^([A-Z]?)(\d{1,2}\.\d{1,2}\.\d{1,2})$")
#: Level-1 headings, all families.
RE_L1_ARTICLE = re.compile(r"^(ARTICLE|APPENDIX)\s+([A-Z]?\d+)\s*:?(.*)$", re.IGNORECASE)
RE_L1_MODERN = re.compile(r"^(\d{1,2})\)\s*(.*)$")  # 2020-2025 sporting: "6) WORLD CHAMPIONSHIP"
RE_L1_FIN = re.compile(r"^(\d{1,2})\.\s+([A-Z].*)$")  # financial: "1. GENERAL PRINCIPLES"
RE_L1_PREAMBLE = re.compile(r"^PREAMBLE$", re.IGNORECASE)
#: Lettered / roman sub-items (kept inline, start a new paragraph).
RE_ITEM = re.compile(r"^(\(?([a-z]|[ivx]{1,4})[.)])\s*$|^([a-z][.)])\s")
#: TOC markers.
RE_CONTENTS_MARKER = re.compile(r"^(ART\s+)?CONTENTS\s*(SUMMARY)?\s*:?$", re.IGNORECASE)
RE_LEADERS = re.compile(r"\.{4,}")

# ---------------------------------------------------------------------------
# Cover fields
# ---------------------------------------------------------------------------

RE_TITLE_YEAR = re.compile(r"\b(20\d{2})\b")
RE_ISSUE = re.compile(r"\bISS?UE?\s*[:.]?\s*(\d{1,3})\b", re.IGNORECASE)
RE_VERSION_ISSUE = re.compile(r"^Version:$", re.IGNORECASE)
RE_STATUS = re.compile(r"^Status:$", re.IGNORECASE)
RE_DATE_FIELD = re.compile(r"^Date:$", re.IGNORECASE)
RE_WMSC_FIELD = re.compile(r"^WMSC approval date:$", re.IGNORECASE)
RE_PUBLISHED_ON = re.compile(r"PUBLISHED\s+ON\s+(.+)$", re.IGNORECASE)
RE_CONVENTION = re.compile(r"^Convention:$", re.IGNORECASE)
RE_DMY = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
RE_LONG_DATE = re.compile(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$")

#: Filename hints.
RE_FN_YEAR = re.compile(r"\b(20\d{2})\b")
RE_FN_ISSUE = re.compile(r"\biss(?:ue)?-?(\d{1,3})\b", re.IGNORECASE)
RE_FN_VARIANT = re.compile(r"-v(\d+)-", re.IGNORECASE)
RE_FN_LEADING_SEQ = re.compile(r"^(\d+)-")
RE_FN_SECTION = re.compile(r"section-([a-f])-", re.IGNORECASE)

# ---------------------------------------------------------------------------
# regulation_type mapping (PLAN.md §5.1)
# ---------------------------------------------------------------------------

SECTION_TYPE = {
    "A": "general",
    "B": "sporting",
    "C": "technical",
    "D": "financial-teams",
    "E": "financial-pu",
    "F": "operational",
}

SECTION_SLUG = {
    "A": "section-a-general",
    "B": "section-b-sporting",
    "C": "section-c-technical",
    "D": "section-d-financial-f1-teams",
    "E": "section-e-financial-pu-manufacturers",
    "F": "section-f-operational",
}

# ---------------------------------------------------------------------------
# Text normalization (PLAN.md §3.4, §6.6)
# ---------------------------------------------------------------------------
#
# The 'ffi' ligature extracts as different wrong characters depending on the
# font subset: '=' (10.1pt Aptos TOC), '`' (11pt Aptos body), 'W' (Aptos-Bold
# headings). Verified contexts only (PLAN.md §6.6); every replacement counted.

RE_LIGATURE_FI = re.compile("ﬁ")
RE_LIGATURE_FL = re.compile("ﬂ")
RE_LIGATURE_EQ = re.compile(r"(?<=[A-Za-z])=(?=[A-Za-z])")  # O=icials -> Officials
RE_LIGATURE_BT = re.compile(r"(?<=[A-Za-z])`(?=[A-Za-z])")  # o`icials -> officials
RE_LIGATURE_W = re.compile(r"\b([Oo])W(icial|icials|ice|ices|icer|icers)\b")  # OWicials -> Officials

#: The corrupted glyph stands for the 'ff' ligature in every verified context
#: ('O=icials'/'o`icials'/'OWicials' -> 'Officials', 'O`icer' -> 'Officer').
FF_GLYPH = "ff"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


#: Bump whenever extraction logic changes in a way that affects output —
#: cached sidecars with an older version are re-extracted (§6.1 step 1).
EXTRACTOR_VERSION = 5


def normalize_text(text: str, stats: dict[str, int] | None = None) -> str:
    """Apply the verified ligature-bug replacement table (PLAN.md §6.6).

    Smart quotes / em-dashes are kept as-is. Every replacement is counted in
    ``stats`` (keys ``ligature_fixes`` and per-rule ``ligature_<rule>``).
    """
    def _count(rule: str, n: int) -> None:
        if stats is not None and n:
            stats["ligature_fixes"] = stats.get("ligature_fixes", 0) + n
            stats[f"ligature_{rule}"] = stats.get(f"ligature_{rule}", 0) + n

    n = len(RE_LIGATURE_FI.findall(text)); text = RE_LIGATURE_FI.sub("fi", text); _count("fi", n)
    n = len(RE_LIGATURE_FL.findall(text)); text = RE_LIGATURE_FL.sub("fl", text); _count("fl", n)
    n = len(RE_LIGATURE_EQ.findall(text)); text = RE_LIGATURE_EQ.sub(FF_GLYPH, text); _count("eq", n)
    n = len(RE_LIGATURE_BT.findall(text)); text = RE_LIGATURE_BT.sub(FF_GLYPH, text); _count("bt", n)
    text, n = RE_LIGATURE_W.subn(r"\1ff\2", text); _count("w", n)
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"\n{3,}", "\n\n", text)  # collapse >2 consecutive blank lines
    return text


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def parse_date(value: Any) -> str | None:
    """Parse 'dd/mm/yyyy', 'dd Month yyyy', or ISO into an ISO date string."""
    if not isinstance(value, str):
        return None
    value = value.strip().rstrip(".")
    m = RE_DMY.match(value)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime.date(y, mo, d).isoformat()
        except ValueError:
            return None
    m = RE_LONG_DATE.match(value)
    if m and m.group(2).lower() in MONTHS:
        try:
            return datetime.date(int(m.group(3)), MONTHS[m.group(2).lower()], int(m.group(1))).isoformat()
        except ValueError:
            return None
    try:
        return datetime.date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def now_utc() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds")


def rel_source(path: Path) -> str:
    """Path of a PDF relative to the repo root (stored as ``source_pdf``)."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)
