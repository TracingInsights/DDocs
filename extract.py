#!/usr/bin/env python3
"""Extract FIA PDF documents into Markdown plus structured JSON sidecars."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

console = Console()

DOCUMENTS_DIR = Path("documents")
EXTRACTED_DIR = Path("extracted")
MANIFEST_NAME = "manifest.json"

STRUCTURED_DOC_TYPES = {
    "decision",
    "offence",
    "infringement",
    "summons",
    "curfew",
    "stewards-communication",
    "steward-substitution",
    "timetable-change",
    "replacement-driver",
}
FIELD_LABELS = {
    "alleged breach",
    "article",
    "breach",
    "car",
    "no / driver",
    "driver",
    "competitor",
    "competitors",
    "competitor representative",
    "time",
    "session",
    "fact",
    "offence",
    "infringement",
    "decision",
    "reason",
    "reasons",
    "matter",
    "hearing",
    "hearing time",
    "representative",
    "summons",
}

DOC_TYPE_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("circuit-map", ("circuit-map", "emergency-exit-map", "pit-lane-map")),
    ("pit-lane-drawing", ("pit-lane-drawing",)),
    ("decision", ("decision",)),
    ("summons", ("summons",)),
    ("offence", ("offence",)),
    ("infringement", ("infringement", "drs-infringement")),
    ("classification", ("classification",)),
    ("starting-grid", ("starting-grid", "starting_grid")),
    ("scrutineering", ("scrutineering",)),
    ("entry-list", ("entry-list", "entry_list")),
    ("race-director-note", ("race-directors", "race-director")),
    ("championship-points", ("championship-points",)),
    ("pu-elements", ("pu-elements", "power-unit")),
    ("deleted-lap-times", ("deleted-lap-times",)),
    ("sc2-sc1-times", ("sc2-sc1-times", "sc2-1-time")),
    ("curfew", ("curfew", "cufrew")),
    ("parc-ferme", ("parc-fermé", "parc-ferme")),
    ("procedure", ("procedure", "procedures-relating-to")),
    ("car-presentation", ("car-presentation-submissions",)),
    ("competition-visa", ("competition-visa",)),
    ("pirelli-preview", ("pirelli-preview",)),
    ("drivers-meeting", ("drivers-meeting",)),
    ("garage-doors", ("garage-doors",)),
    ("heat-hazard", ("heat-hazard",)),
    ("post-race-checks", ("post-race-checks",)),
    (
        "technical-check",
        (
            "failed-rear-wing",
            "front-and-rear-wing-monitoring-cameras",
            "rearward-skids",
            "set-up-sheets",
            "skid-wear",
            "tyre-operating-procedures",
            "weight-of-car",
        ),
    ),
    ("stewards-communication", ("stewards-communication",)),
    ("steward-substitution", ("steward-substitution",)),
    ("timetable", ("timetable",)),
    ("timetable-change", ("change-to-timetable",)),
    ("replacement-driver", ("replacement-driver",)),
)

ALL_DOC_TYPES = frozenset(doc_type for doc_type, _patterns in DOC_TYPE_PATTERNS) | {"other"}
TABLE_IMAGELESS_DOC_TYPES = {
    "championship-points",
    "classification",
    "deleted-lap-times",
    "entry-list",
    "competition-visa",
    "pu-elements",
    "starting-grid",
}
PAGE_RENDERED_DOC_TYPES = {
    "car-presentation",
    "circuit-map",
    "pit-lane-drawing",
    "pirelli-preview",
    "procedure",
}
PAGE_RENDER_SCALE = 2.0


@dataclass(frozen=True)
class PdfJob:
    source_path: Path
    relative_path: str
    source_hash: str


def load_manifest(output_dir: Path = EXTRACTED_DIR) -> dict[str, Any]:
    manifest_path = output_dir / MANIFEST_NAME
    if not manifest_path.exists():
        return {}
    with manifest_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"{manifest_path} must contain a JSON object")
    return data


def save_manifest(manifest: dict[str, Any], output_dir: Path = EXTRACTED_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / MANIFEST_NAME
    tmp_path = manifest_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(manifest_path)


def compute_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def discover_pdfs(
    documents_dir: Path = DOCUMENTS_DIR,
    *,
    year: int | None = None,
    event: str | None = None,
) -> list[Path]:
    root = documents_dir
    if year is not None:
        root = root / str(year)
    if event is not None:
        if year is None:
            raise ValueError("--event requires --year")
        root = root / event
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.pdf") if path.is_file())


def make_job(path: Path, documents_dir: Path = DOCUMENTS_DIR) -> PdfJob:
    relative_path = path.relative_to(Path.cwd()).as_posix() if path.is_absolute() else path.as_posix()
    if not relative_path.startswith(f"{documents_dir.as_posix()}/"):
        relative_path = path.relative_to(documents_dir.parent).as_posix()
    return PdfJob(source_path=path, relative_path=relative_path, source_hash=compute_hash(path))


def needs_extraction(job: PdfJob, manifest: dict[str, Any], *, force: bool = False) -> bool:
    if force:
        return True
    entry = manifest.get(job.relative_path)
    if not entry:
        return True
    if entry.get("source_hash") != job.source_hash:
        return True
    return entry.get("success") is not True


def detect_doc_type(pdf_path: Path) -> str:
    name = pdf_path.stem.lower()
    for doc_type, patterns in DOC_TYPE_PATTERNS:
        if any(pattern in name for pattern in patterns):
            return doc_type
    return "other"


def humanize_title(stem: str) -> str:
    text = re.sub(r"[_-]+", " ", stem).strip()
    text = re.sub(r"\s+", " ", text)
    return text.title()


def event_name_from_slug(event_slug: str) -> str:
    return event_slug.replace("-", " ").title()


def normalize_event_name(value: str | None) -> str | None:
    value = clean_value(value)
    if value and value.isupper():
        return value.title()
    return value


def normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    value = re.sub(r"\s+", " ", value.strip())
    value = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", value, flags=re.IGNORECASE)
    for fmt in ("%d %B %Y", "%d %b %Y", "%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    return value


def clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip(" :-\t\r\n")
    return value or None


def _lines(text: str) -> list[str]:
    result: list[str] = []
    for line in text.replace("\xa0", " ").splitlines():
        line = line.strip()
        if not line:
            continue
        # Strip markdown heading markers and bold markers for field parsing.
        line = re.sub(r"^#{1,6}\s+", "", line)
        line = re.sub(r"\*{1,2}(.+?)\*{1,2}", r"\1", line)
        line = line.strip()
        if line:
            result.append(line)
    return result


def parse_header_fields(text: str) -> dict[str, Any]:
    lines = _lines(text)
    header: dict[str, Any] = {
        "event_name": None,
        "doc_number": None,
        "date": None,
        "time": None,
        "from": None,
        "to": None,
    }

    for line in lines[:20]:
        if re.search(r"\bgrand prix\b", line, re.IGNORECASE):
            event_name = re.sub(r"^#+\s*", "", line)
            header["event_name"] = normalize_event_name(re.sub(r"^\d{4}\s+", "", event_name, flags=re.IGNORECASE))
            break

    joined = "\n".join(lines[:60])
    from_to_match = re.search(
        r"\b(?:From|FROM)[ \t]+([^\n]+?)[ \t]+(?:To|TO)[ \t]+([^\n]+?)"
        r"(?=\s+(?:Document|DOCUMENT|Date|DATE|Time|TIME)\b|\n|$)",
        joined,
    )
    if from_to_match:
        header["from"] = clean_value(from_to_match.group(1))
        header["to"] = clean_value(from_to_match.group(2))

    patterns = {
        "from": r"\b(?:From|FROM)\s+(.+?)(?=\s+(?:Document|DOCUMENT)\b|\n|$)",
        "to": r"\b(?:To|TO)\s+(.+?)(?=\s+(?:Date|DATE|Document|DOCUMENT)\b|\n|$)",
        "doc_number": r"\b(?:Document|DOCUMENT)\s+([A-Za-z0-9./-]+)",
        "date": r"\b(?:Date|DATE)\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})",
        "time": r"\b(?:Time|TIME)\s+(\d{1,2}:\d{2})",
    }
    for key, pattern in patterns.items():
        if key in {"from", "to"} and header.get(key):
            continue
        match = re.search(pattern, joined)
        if match:
            header[key] = clean_value(match.group(1))

    header["date"] = normalize_date(header["date"])
    return header


_INLINE_LABEL_RE = re.compile(
    r"(?<!\w)(?:No\s*/\s*Driver|Competitor|Infringement|Decision|Session|Offence|Reasons?|Fact|Time)"
    r"(?=\s*:?\s(?-i:[A-Z0-9]))",
    re.IGNORECASE,
)


def _split_inline_labels(text: str) -> list[str]:
    """Split a single line at inline field-label boundaries.

    E.g. ``"Session Race Fact Something happened."`` becomes
    ``["Session Race", "Fact Something happened."]``.
    """
    positions = [m.start() for m in _INLINE_LABEL_RE.finditer(text)]
    if len(positions) <= 1:
        return [text]
    parts: list[str] = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        chunk = text[start:end].strip()
        if chunk:
            parts.append(chunk)
    # Anything before the first label match is prepended.
    if positions[0] > 0:
        prefix = text[: positions[0]].strip()
        if prefix:
            parts.insert(0, prefix)
    return parts or [text]


def _line_field_map(text: str) -> dict[str, str]:
    fields: dict[str, list[str]] = {}
    current: str | None = None
    label_pattern = re.compile(
        r"^(No\s*/\s*Driver|Competitor|Time|Session|Fact|Offence|Infringement|Decision|Reasons?)\b\s*:?\s*(.*)$",
        re.IGNORECASE,
    )
    raw_lines = _lines(text)
    # Expand lines: split any line that contains multiple inline labels.
    expanded: list[str] = []
    for line in raw_lines:
        expanded.extend(_split_inline_labels(line))

    _STOP_RE = re.compile(
        r"^(?:The Stewards\s*:?(?:$|Competitors)|Competitors are reminded|"
        r"Decisions? of the Stewards|Note\s+Competitors)",
        re.IGNORECASE,
    )
    for line in expanded:
        if _STOP_RE.match(line):
            current = None
            continue
        match = label_pattern.match(line)
        if match:
            current = match.group(1).lower().replace("  ", " ")
            if current == "reasons":
                current = "reason"
            fields[current] = [match.group(2).strip()] if match.group(2).strip() else []
            continue
        lowered = line.lower().rstrip(":")
        if lowered in FIELD_LABELS:
            current = "reason" if lowered == "reasons" else lowered
            fields.setdefault(current, [])
            continue
        if current and line:
            fields[current].append(line)
    # Strip trailing boilerplate that may appear inline.
    _BOILERPLATE_RE = re.compile(
        r"\s*(?:Competitors are reminded|Decisions? of the Stewards|Note\s+Competitors|The Stewards$).*",
        re.IGNORECASE | re.DOTALL,
    )
    result: dict[str, str] = {}
    for key, parts in fields.items():
        value = clean_value(" ".join(parts)) or ""
        value = _BOILERPLATE_RE.sub("", value).strip()
        result[key] = clean_value(value) or ""
    return result


def extract_rules(text: str) -> list[str]:
    pattern = re.compile(
        r"\b(?:Article|Appendix|Chapter)\s+[0-9A-Za-z.()/-]+(?:\s+of\s+the\s+FIA\s+Formula\s+One\s+[^.,;\n]+)?",
        re.IGNORECASE,
    )
    rules = {clean_value(match.group(0)) for match in pattern.finditer(text)}
    return sorted(rule for rule in rules if rule)


def extract_penalty(decision: str | None) -> tuple[str | None, int | None]:
    if not decision:
        return None, None
    penalty_points_match = re.search(r"(\d+)\s+penalty\s+point", decision, re.IGNORECASE)
    penalty_points = int(penalty_points_match.group(1)) if penalty_points_match else None
    if re.search(r"no\s+further\s+action", decision, re.IGNORECASE):
        return None, penalty_points
    fine = re.search(r"(?:€|EUR)\s?\d[\d,]*(?:\.\d+)?", decision, re.IGNORECASE)
    if fine:
        return fine.group(0).replace("EUR", "€").replace(" ", ""), penalty_points
    seconds = re.search(r"(\d+)\s*(?:second|seconds|s)\s+time\s+penalt", decision, re.IGNORECASE)
    if seconds:
        return f"{seconds.group(1)}s time penalty", penalty_points
    grid = re.search(r"(\d+)\s+place\s+grid\s+penalt", decision, re.IGNORECASE)
    if grid:
        return f"{grid.group(1)} place grid penalty", penalty_points
    return clean_value(decision), penalty_points


def extract_stewards(text: str) -> list[str]:
    match = re.search(r"The Stewards\s*:?\s*(.+?)(?:\n\s*(?:Document|Date|Time)\b|$)", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    raw = re.sub(r"\s+", " ", match.group(1)).strip()
    names = re.split(r"\s*,\s*|\s{2,}| and ", raw)
    cleaned = [name.strip(" .") for name in names if 2 <= len(name.strip(" .")) <= 80]
    return cleaned[:8]


def parse_decision_fields(text: str) -> dict[str, Any]:
    fields = _line_field_map(text)
    driver = None
    car_number: int | None = None
    no_driver = fields.get("no / driver")
    if no_driver:
        match = re.match(r"(?P<car>\d+)\s*[-–]\s*(?P<driver>.+)", no_driver)
        if match:
            car_number = int(match.group("car"))
            driver = clean_value(match.group("driver"))
        else:
            driver = clean_value(no_driver)
    if car_number is None:
        car_match = re.search(r"\bCar\s+(\d+)\b", text, re.IGNORECASE)
        if car_match:
            car_number = int(car_match.group(1))

    offence = fields.get("offence") or fields.get("infringement")
    decision = fields.get("decision")
    penalty, penalty_points = extract_penalty(decision)

    return {
        "driver": driver,
        "car_number": car_number,
        "team": clean_value(fields.get("competitor")),
        "session": clean_value(fields.get("session")),
        "incident_time": clean_value(fields.get("time")),
        "fact": clean_value(fields.get("fact")),
        "offence": clean_value(offence),
        "decision": clean_value(decision),
        "reason": clean_value(fields.get("reason")),
        "penalty": penalty,
        "penalty_points": penalty_points,
        "stewards": extract_stewards(text),
        "rules_referenced": extract_rules(text),
    }


def base_metadata(
    pdf_path: Path,
    text: str,
    result: Any,
    job: PdfJob,
    images_count: int,
    tables_count: int,
) -> dict[str, Any]:
    parts = Path(job.relative_path).parts
    year = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
    event_slug = parts[2] if len(parts) > 2 else None
    header = parse_header_fields(text)
    doc_type = detect_doc_type(pdf_path)
    metadata: dict[str, Any] = {
        "source_pdf": job.relative_path,
        "source_hash": job.source_hash,
        "extracted_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "year": year,
        "event_slug": event_slug,
        "event_name": header.get("event_name") or (event_name_from_slug(event_slug) if event_slug else None),
        "doc_type": doc_type,
        "doc_number": header.get("doc_number"),
        "date": header.get("date"),
        "time": header.get("time"),
        "from": header.get("from"),
        "to": header.get("to"),
        "title": humanize_title(pdf_path.stem),
        "pages": get_page_count(result),
        "images_extracted": images_count,
        "tables_extracted": tables_count,
    }
    if doc_type in STRUCTURED_DOC_TYPES:
        metadata.update(parse_decision_fields(text))
    return metadata


def get_page_count(result: Any) -> int | None:
    try:
        count = result.get_page_count()
        if count:
            return int(count)
    except Exception:
        pass
    metadata = getattr(result, "metadata", {}) or {}
    count = metadata.get("page_count") if hasattr(metadata, "get") else None
    return int(count) if count else None


def is_header_image(bbox: tuple[float, float, float, float], page_width: float, page_height: float) -> bool:
    x0, y0, x1, y1 = bbox
    width = max(0.0, x1 - x0)
    height = max(0.0, y1 - y0)
    in_header_band = y0 <= page_height * 0.15
    full_width_banner = width >= page_width * 0.70 and height <= page_height * 0.20
    scaled_banner = width >= page_width * 0.45 and height <= page_height * 0.16
    top_logo_strip = y0 <= page_height * 0.10 and height <= page_height * 0.04 and width >= page_width * 0.20
    return in_header_band and (full_width_banner or scaled_banner or top_logo_strip)


def image_bbox_dimensions(bbox: tuple[float, float, float, float]) -> tuple[float, float]:
    x0, y0, x1, y1 = bbox
    return max(0.0, x1 - x0), max(0.0, y1 - y0)


def is_tiny_non_content_image(
    bbox: tuple[float, float, float, float],
    page_width: float,
    page_height: float,
) -> bool:
    width, height = image_bbox_dimensions(bbox)
    if width <= 0 or height <= 0:
        return True
    area = width * height
    page_area = page_width * page_height
    # Driver country flags in FIA result tables are typically ~13 x 8 pt.
    if width <= 36 and height <= 30:
        return True
    # Some vector-rendered decorations are thin slivers; they are not useful
    # standalone images for retrieval or vision embedding.
    if min(width, height) <= 4 and area <= page_area * 0.01:
        return True
    return area <= page_area * 0.0006


def is_low_value_native_image(
    bbox: tuple[float, float, float, float],
    page_width: float,
    page_height: float,
    native_width: int,
    native_height: int,
    byte_count: int,
    image_bytes: bytes = b"",
    doc_type: str | None = None,
) -> bool:
    if native_width <= 0 or native_height <= 0:
        return True
    if native_width <= 64 and native_height <= 64:
        return True
    if native_width * native_height <= 4096:
        return True
    if byte_count <= 800 and max(native_width, native_height) <= 200:
        return True

    width, height = image_bbox_dimensions(bbox)
    if width <= 0 or height <= 0:
        return True
    aspect = max(native_width / native_height, native_height / native_width)
    displayed_aspect = max(width / height, height / width)
    if height <= page_height * 0.035 and (aspect >= 6 or displayed_aspect >= 8):
        return True
    if min(native_width, native_height) <= 80 and min(width, height) <= 30:
        return True
    if min(native_width, native_height) < 120:
        return True

    stats = image_content_stats(image_bytes)
    if stats:
        if bbox[1] <= page_height * 0.15 and stats["white_ratio"] >= 0.55 and height <= page_height * 0.16:
            return True
        if aspect >= 3 and stats["dark_ratio"] >= 0.50:
            return True
        if doc_type != "car-presentation" and stats["light_ratio"] >= 0.45 and stats["saturation"] <= 10:
            return True
    return False


def image_content_stats(image_bytes: bytes) -> dict[str, float] | None:
    if not image_bytes:
        return None
    try:
        from PIL import Image
    except ImportError:
        return None
    try:
        import io

        with Image.open(io.BytesIO(image_bytes)) as image:
            image = image.convert("RGB")
            image.thumbnail((120, 120))
            pixels = list(image.getdata())
    except Exception:
        return None
    if not pixels:
        return None
    total = len(pixels)
    white = sum(1 for red, green, blue in pixels if red > 240 and green > 240 and blue > 240)
    light = sum(1 for red, green, blue in pixels if red > 220 and green > 220 and blue > 220)
    dark = sum(1 for red, green, blue in pixels if red < 40 and green < 40 and blue < 40)
    saturation = sum(max(red, green, blue) - min(red, green, blue) for red, green, blue in pixels) / total
    return {
        "white_ratio": white / total,
        "light_ratio": light / total,
        "dark_ratio": dark / total,
        "saturation": saturation,
    }


def _bbox_union(boxes: list[tuple[float, float, float, float]]) -> tuple[float, float, float, float]:
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )


def _bbox_iou(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> float:
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    ix0, iy0 = max(ax0, bx0), max(ay0, by0)
    ix1, iy1 = min(ax1, bx1), min(ay1, by1)
    iw, ih = max(0.0, ix1 - ix0), max(0.0, iy1 - iy0)
    intersection = iw * ih
    if intersection <= 0:
        return 0.0
    area_a = max(0.0, ax1 - ax0) * max(0.0, ay1 - ay0)
    area_b = max(0.0, bx1 - bx0) * max(0.0, by1 - by0)
    union = area_a + area_b - intersection
    return intersection / union if union else 0.0


def _bbox_gap(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> tuple[float, float]:
    horizontal_gap = max(0.0, max(a[0], b[0]) - min(a[2], b[2]))
    vertical_gap = max(0.0, max(a[1], b[1]) - min(a[3], b[3]))
    return horizontal_gap, vertical_gap


def _overlap_ratio(a0: float, a1: float, b0: float, b1: float) -> float:
    overlap = max(0.0, min(a1, b1) - max(a0, b0))
    shortest = min(max(0.0, a1 - a0), max(0.0, b1 - b0))
    return overlap / shortest if shortest else 0.0


def _bbox_should_merge(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
    page_width: float,
    page_height: float,
) -> bool:
    if _bbox_iou(a, b) >= 0.92:
        return True
    horizontal_gap, vertical_gap = _bbox_gap(a, b)
    tolerance = max(3.0, min(page_width, page_height) * 0.01)
    vertical_overlap = _overlap_ratio(a[1], a[3], b[1], b[3])
    horizontal_overlap = _overlap_ratio(a[0], a[2], b[0], b[2])
    return (
        horizontal_gap <= tolerance
        and vertical_overlap >= 0.60
        or vertical_gap <= tolerance
        and horizontal_overlap >= 0.60
    )


def merge_image_bboxes(
    bboxes: list[tuple[float, float, float, float]],
    page_width: float,
    page_height: float,
) -> list[tuple[float, float, float, float]]:
    clusters: list[list[tuple[float, float, float, float]]] = []
    for bbox in sorted(bboxes, key=lambda box: (box[1], box[0], box[3], box[2])):
        for cluster in clusters:
            if any(_bbox_should_merge(bbox, existing, page_width, page_height) for existing in cluster):
                cluster.append(bbox)
                break
        else:
            clusters.append([bbox])

    changed = True
    while changed:
        changed = False
        merged: list[list[tuple[float, float, float, float]]] = []
        for cluster in clusters:
            cluster_box = _bbox_union(cluster)
            for existing in merged:
                existing_box = _bbox_union(existing)
                if _bbox_should_merge(cluster_box, existing_box, page_width, page_height):
                    existing.extend(cluster)
                    changed = True
                    break
            else:
                merged.append(cluster)
        clusters = merged

    return [_bbox_union(cluster) for cluster in clusters]


def clamp_bbox_to_page(
    bbox: tuple[float, float, float, float],
    page_width: float,
    page_height: float,
) -> tuple[float, float, float, float]:
    x0, y0, x1, y1 = bbox
    return (
        max(0.0, min(page_width, x0)),
        max(0.0, min(page_height, y0)),
        max(0.0, min(page_width, x1)),
        max(0.0, min(page_height, y1)),
    )


def _cleanup_doc_images(images_dir: Path, doc_slug: str) -> None:
    if not images_dir.exists():
        return
    for path in images_dir.glob(f"{doc_slug}_p*.*"):
        if path.is_file():
            path.unlink()


def _existing_image_hashes(images_dir: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    if not images_dir.exists():
        return hashes
    for path in sorted(images_dir.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            continue
        hashes.setdefault(digest, f"images/{path.name}")
    return hashes


def _best_placement(
    placements: Iterable[tuple[int, tuple[float, float, float, float], float, float]],
) -> tuple[int, tuple[float, float, float, float], float, float]:
    return max(placements, key=lambda item: image_bbox_dimensions(item[1])[0] * image_bbox_dimensions(item[1])[1])


def _image_extension(ext: str | None) -> str:
    ext = (ext or "png").lower().strip(".")
    if ext == "jpg":
        return "jpeg"
    if not re.fullmatch(r"[a-z0-9]+", ext):
        return "png"
    return ext


def should_render_pages(doc_type: str | None) -> bool:
    return doc_type in PAGE_RENDERED_DOC_TYPES


def substantial_image_area_ratio(page: Any) -> float:
    page_width = float(page.rect.width)
    page_height = float(page.rect.height)
    page_area = page_width * page_height
    if page_area <= 0:
        return 0.0

    total_area = 0.0
    for info in page.get_image_info(xrefs=True):
        raw_bbox = info.get("bbox", (0, 0, 0, 0))
        if len(raw_bbox) != 4:
            continue
        bbox = clamp_bbox_to_page(tuple(float(value) for value in raw_bbox), page_width, page_height)
        if is_header_image(bbox, page_width, page_height):
            continue
        if is_tiny_non_content_image(bbox, page_width, page_height):
            continue
        width, height = image_bbox_dimensions(bbox)
        total_area += width * height
    return total_area / page_area


def rendered_page_numbers(document: Any, doc_type: str | None) -> list[int]:
    if not should_render_pages(doc_type):
        return []
    if doc_type in {"circuit-map", "pit-lane-drawing", "pirelli-preview"}:
        # FIA cover pages are normal text memos. The following pages are the
        # visual payload for these document families and often mix vectors,
        # masks, annotations, and image fragments that native extraction splits
        # or renders with the wrong background.
        return list(range(2, len(document) + 1))

    if doc_type == "car-presentation":
        page_numbers: list[int] = []
        for page_index, page in enumerate(document, start=1):
            if page_index == 1:
                continue
            text_length = len(page.get_text("text").strip())
            # These packets alternate submission tables with visual reference
            # pages. Do not turn pages that still have extractable table text
            # into full-page images.
            if text_length == 0 and substantial_image_area_ratio(page) >= 0.25:
                page_numbers.append(page_index)
        return page_numbers

    page_numbers: list[int] = []
    if doc_type == "procedure":
        for page_index, page in enumerate(document, start=1):
            if page_index == 1:
                continue
            page_width = float(page.rect.width)
            page_height = float(page.rect.height)
            text_length = len(page.get_text("text").strip())
            image_count = len(page.get_image_info(xrefs=True))
            drawing_count = len(page.get_drawings())
            is_landscape = page_width > page_height
            has_visual_layout = (
                image_count >= 3 or drawing_count >= 20 or substantial_image_area_ratio(page) >= 0.25
            )
            if is_landscape and has_visual_layout and text_length <= 600:
                page_numbers.append(page_index)
    return page_numbers


def extract_rendered_pages(
    document: Any,
    images_dir: Path,
    doc_slug: str,
    *,
    doc_type: str | None = None,
) -> list[dict[str, Any]]:
    try:
        import fitz
    except ImportError:
        return []

    saved: list[dict[str, Any]] = []
    page_numbers = rendered_page_numbers(document, doc_type)
    if not page_numbers:
        return saved

    images_dir.mkdir(parents=True, exist_ok=True)
    matrix = fitz.Matrix(PAGE_RENDER_SCALE, PAGE_RENDER_SCALE)
    for page_index in page_numbers:
        page = document[page_index - 1]
        page_width = float(page.rect.width)
        page_height = float(page.rect.height)
        try:
            pixmap = page.get_pixmap(matrix=matrix, colorspace=fitz.csRGB, alpha=False, annots=True)
            image_bytes = pixmap.tobytes("png")
        except Exception:
            continue

        digest = hashlib.sha256(image_bytes).hexdigest()
        filename = f"{doc_slug}_p{page_index}_page.png"
        target = images_dir / filename
        tmp = target.with_name(f".{target.stem}.tmp.png")
        try:
            tmp.write_bytes(image_bytes)
            tmp.replace(target)
        except Exception:
            if tmp.exists():
                tmp.unlink()
            continue

        saved.append(
            {
                "path": f"images/{filename}",
                "page": page_index,
                "bbox": [0.0, 0.0, round(page_width, 2), round(page_height, 2)],
                "display_width": round(page_width, 2),
                "display_height": round(page_height, 2),
                "width": pixmap.width,
                "height": pixmap.height,
                "sha256": digest,
                "deduplicated": False,
                "rendered_page": True,
                "render_scale": PAGE_RENDER_SCALE,
            }
        )
    return saved


def extract_images(
    pdf_path: Path,
    images_dir: Path,
    doc_slug: str,
    *,
    doc_type: str | None = None,
) -> list[dict[str, Any]]:
    try:
        import fitz
    except ImportError:
        return []

    _cleanup_doc_images(images_dir, doc_slug)
    if doc_type in TABLE_IMAGELESS_DOC_TYPES:
        return []

    event_hashes = _existing_image_hashes(images_dir)
    doc_hashes: set[str] = set()
    saved: list[dict[str, Any]] = []
    document = fitz.open(pdf_path)
    try:
        if should_render_pages(doc_type):
            return extract_rendered_pages(document, images_dir, doc_slug, doc_type=doc_type)

        placements: dict[int, list[tuple[int, tuple[float, float, float, float], float, float]]] = {}
        for page_index, page in enumerate(document, start=1):
            page_width = float(page.rect.width)
            page_height = float(page.rect.height)
            image_infos = page.get_image_info(xrefs=True)
            for info in image_infos:
                xref = int(info.get("xref") or 0)
                if xref <= 0:
                    continue
                raw_bbox = info.get("bbox", (0, 0, 0, 0))
                if len(raw_bbox) != 4:
                    continue
                bbox = clamp_bbox_to_page(tuple(float(value) for value in raw_bbox), page_width, page_height)
                if is_header_image(bbox, page_width, page_height):
                    continue
                if is_tiny_non_content_image(bbox, page_width, page_height):
                    continue
                placements.setdefault(xref, []).append((page_index, bbox, page_width, page_height))

        image_number = 0
        for xref, xref_placements in sorted(placements.items(), key=lambda item: (item[1][0][0], item[0])):
            page_index, bbox, page_width, page_height = _best_placement(xref_placements)
            try:
                base_image = document.extract_image(xref)
            except Exception:
                continue
            image_bytes = base_image.get("image") or b""
            native_width = int(base_image.get("width") or 0)
            native_height = int(base_image.get("height") or 0)
            if is_low_value_native_image(
                bbox,
                page_width,
                page_height,
                native_width,
                native_height,
                len(image_bytes),
                image_bytes,
                doc_type,
            ):
                continue
            digest = hashlib.sha256(image_bytes).hexdigest()
            if digest in doc_hashes:
                continue
            doc_hashes.add(digest)

            width, height = image_bbox_dimensions(bbox)
            if digest in event_hashes:
                saved.append(
                    {
                        "path": event_hashes[digest],
                        "page": page_index,
                        "bbox": [round(value, 2) for value in bbox],
                        "display_width": round(width, 2),
                        "display_height": round(height, 2),
                        "width": native_width,
                        "height": native_height,
                        "sha256": digest,
                        "deduplicated": True,
                    }
                )
                continue

            image_number += 1
            images_dir.mkdir(parents=True, exist_ok=True)
            ext = _image_extension(base_image.get("ext"))
            filename = f"{doc_slug}_p{page_index}_img{image_number}.{ext}"
            target = images_dir / filename
            tmp = target.with_name(f".{target.stem}.tmp.{ext}")
            try:
                tmp.write_bytes(image_bytes)
                tmp.replace(target)
                event_hashes[digest] = f"images/{filename}"
                saved.append(
                    {
                        "path": f"images/{filename}",
                        "page": page_index,
                        "bbox": [round(value, 2) for value in bbox],
                        "display_width": round(width, 2),
                        "display_height": round(height, 2),
                        "width": native_width,
                        "height": native_height,
                        "sha256": digest,
                        "deduplicated": False,
                    }
                )
            except Exception:
                if tmp.exists():
                    tmp.unlink()
                    continue
    finally:
        document.close()
    return saved


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    write_text_atomic(path, json.dumps(data, indent=2, sort_keys=True) + "\n")


def clean_table_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]*\n[ \t]*", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def table_non_empty_cells(row: list[str]) -> list[str]:
    return [cell for cell in row if cell]


UPDATE_TABLE_HEADER = ["No.", "Updated component", "Primary reason for update", "Geometric differences", "Description"]

CHAMPIONSHIP_ROUND_CODES = [
    "AUS",
    "CHN",
    "JPN",
    "USA",
    "CAN",
    "MCO",
    "ESP",
    "AUT",
    "GBR",
    "BEL",
    "HUN",
    "NLD",
    "ITA",
    "ESP",
    "AZE",
    "SGP",
    "USA",
    "MEX",
    "BRA",
    "USA",
    "QAT",
    "UAE",
]


def _collapse_duplicate_halves(text: str) -> str:
    words = text.split()
    if len(words) % 2 == 0:
        half = len(words) // 2
        if words[:half] == words[half:]:
            return " ".join(words[:half])
    return text


def _join_cells(cells: Iterable[str]) -> str:
    parts: list[str] = []
    for cell in cells:
        if cell and cell not in parts:
            parts.append(cell)
    return _collapse_duplicate_halves(clean_value(" ".join(parts)) or "")


def _normalize_update_table_rows(rows: list[list[str]], max_cols: int) -> list[list[str]]:
    normalized = [UPDATE_TABLE_HEADER]

    if max_cols >= 12:
        groups = [(0, 1), (1, 4), (4, 7), (7, 11), (11, max_cols)]
    elif max_cols >= 7:
        groups = [(0, 1), (1, 2), (2, 3), (3, 5), (5, max_cols)]
    else:
        groups = [(0, 1), (1, 2), (2, 3), (3, 4), (4, max_cols)]

    for row in rows:
        cells = [_join_cells(row[start:end]) for start, end in groups]
        first = cells[0]
        lower_row = " ".join(row).lower()
        header_tokens = ("updated", "component", "primary reason", "brief description")
        if not first and any(token in lower_row for token in header_tokens):
            continue
        if first.isdigit():
            cells[2] = _normalize_update_reason(cells[2])
            normalized.append(cells)
        elif len(normalized) > 1:
            for index, cell in enumerate(cells[1:], start=1):
                if cell:
                    normalized[-1][index] = _collapse_duplicate_halves(f"{normalized[-1][index]} {cell}".strip())
            normalized[-1][2] = _normalize_update_reason(normalized[-1][2])
        elif cells[-1]:
            normalized.append(["", "", "", "", cells[-1]])

    return normalized if len(normalized) > 1 else []


def _normalize_update_reason(value: str) -> str:
    known_reasons = (
        "Performance - Local Load",
        "Performance – Local Load",
        "Performance - Flow Conditioning",
        "Performance – Flow Conditioning",
        "Circuit specific - Cooling Range",
        "Circuit specific - Balance Range",
        "Performance - Drag reduction",
        "Performance – Drag reduction",
        "Reliability",
    )
    compact = re.sub(r"[\s–-]+", "", value).lower()
    for reason in known_reasons:
        reason_compact = re.sub(r"[\s–-]+", "", reason).lower()
        if compact and compact == reason_compact * 2:
            return reason
    return value


def _normalize_championship_table_rows(rows: list[list[str]]) -> list[list[str]]:
    data_rows = [row for row in rows if table_non_empty_cells(row) and table_non_empty_cells(row)[0].isdigit()]
    if not data_rows:
        return []

    active_cols = max(
        (max((index for index, cell in enumerate(row) if cell), default=0) + 1 for row in data_rows),
        default=0,
    )
    active_cols = max(active_cols, 7)
    header = ["Pos", "Driver / Entrant", "Total"] + CHAMPIONSHIP_ROUND_CODES[: max(0, active_cols - 3)]
    return [header[:active_cols]] + [row[:active_cols] + [""] * max(0, active_cols - len(row)) for row in data_rows]


TIMETABLE_TOKEN_REPLACEMENTS = {
    "TRACKOPENTOF1PASSHOLDERS": "TRACK OPEN TO F1 PASS HOLDERS",
    "TEAMCURFEWENDS": "TEAM CURFEW ENDS",
    "TEAMCURFEWSTARTS": "TEAM CURFEW STARTS",
    "SECURITYBRIEFING": "SECURITY BRIEFING",
    "SKYFILMINGACTIVITY": "SKY FILMING ACTIVITY",
    "TRACKCLOSED": "TRACK CLOSED",
    "SYSTEMSCHECKS": "SYSTEMS CHECKS",
    "TRACKRESTRICTED": "TRACK RESTRICTED",
    "TOFIA/F1ONLY": "TO FIA/F1 ONLY",
    "TRACKCOMPLETELYCLEAR": "TRACK COMPLETELY CLEAR",
    "HIGHSPEEDTRACKTEST": "HIGH SPEED TRACK TEST",
    "FIASAFETY": "FIA SAFETY",
    "MEDICALCARS": "MEDICAL CARS",
    "FAMILIARISATION": "FAMILIARISATION",
    "PIRELLIHOTLAPS": "PIRELLI HOT LAPS",
    "CLUBLAPS": "CLUB LAPS",
    "DRIVERSANDTEAMMANAGERSMEETING": "DRIVERS AND TEAM MANAGERS MEETING",
    "F1CARCOVERSEALSREMOVED": "F1 CAR COVER SEALS REMOVED",
    "F1CARPRESENTATION": "F1 CAR PRESENTATION",
    "F1EXPERIENCESTRACKTOUR": "F1 EXPERIENCES TRACK TOUR",
    "F1EXPERIENCESTROPHYPHOTO": "F1 EXPERIENCES TROPHY PHOTO",
    "F1EXPERIENCESPITLANEWALK": "F1 EXPERIENCES PIT LANE WALK",
    "TRACKINSPECTION&SAFETYCARTEST": "TRACK INSPECTION & SAFETY CAR TEST",
    "PROMOTERPITLANEWALK": "PROMOTER PIT LANE WALK",
    "PRACTICESESSION": "PRACTICE SESSION",
    "TEAMMANAGERS'MEETING": "TEAM MANAGERS' MEETING",
    "DRIVERS'MEETING": "DRIVERS' MEETING",
    "TEAMS'PRESSCONFERENCE": "TEAMS' PRESS CONFERENCE",
    "DRIVERS’PRESSCONFERENCE": "DRIVERS’ PRESS CONFERENCE",
    "TAGHEUERMEDIALAP": "TAG HEUER MEDIA LAP",
    "CHAMPIONSCLUBGRIDWALK": "CHAMPIONS CLUB GRID WALK",
    "REDBULLPARACHUTEJUMP": "RED BULL PARACHUTE JUMP",
    "AIRDISPLAY": "AIR DISPLAY",
    "PITLANEOPEN": "PIT LANE OPEN",
    "FIRSTRACE": "FIRST RACE",
    "SECONDRACE": "SECOND RACE",
    "FEATURERACE": "FEATURE RACE",
    "LAPSOR": "LAPS OR",
    "MINS": "MINS",
    "TRACK&MEDICALINSPECTION": "TRACK & MEDICAL INSPECTION",
    "DRIVERS'PARADE": "DRIVERS' PARADE",
    "FORMULA1": "FORMULA 1",
    "FIAFORMULA2": "FIA FORMULA 2",
    "MCLARENTROPHYAMERICA": "MCLAREN TROPHY AMERICA",
    "PORSCHECARRERACUP": "PORSCHE CARRERA CUP",
    "NORTHAMERICA": "NORTH AMERICA",
    "PADDOCKCLUB": "PADDOCK CLUB",
    "PROMOTERACTIVITY": "PROMOTER ACTIVITY",
    "F1EXPERIENCES": "F1 EXPERIENCES",
    "ONLINEMEETING": "ONLINE MEETING",
    "PRESSCONFERENCEROOM": "PRESS CONFERENCE ROOM",
    "PITLANE": "PIT LANE",
    "PITLANEWALK": "PIT LANE WALK",
    "TEAMCURFEW": "TEAM CURFEW",
    "TRACKOPEN": "TRACK OPEN",
    "F1PASSHOLDERS": "F1 PASS HOLDERS",
    "MEDICALINSPECTION": "MEDICAL INSPECTION",
    "TRACKINSPECTION": "TRACK INSPECTION",
    "SAFETYCAR": "SAFETY CAR",
    "SYSTEMCHECKS": "SYSTEM CHECKS",
    "PRESSCONFERENCE": "PRESS CONFERENCE",
    "DRIVERS": "DRIVERS",
    "TEAMMANAGERS": "TEAM MANAGERS",
    "FIRSTPRACTICESESSION": "FIRST PRACTICE SESSION",
    "SECONDPRACTICESESSION": "SECOND PRACTICE SESSION",
    "QUALIFYINGSESSION": "QUALIFYING SESSION",
    "SPRINTQUALIFYING": "SPRINT QUALIFYING",
    "SPRINTRACE": "SPRINT RACE",
    "GRANDPRIX": "GRAND PRIX",
    "GRIDPROCEDURE": "GRID PROCEDURE",
    "GRIDPRESENTATION": "GRID PRESENTATION",
    "NATIONALANTHEM": "NATIONAL ANTHEM",
    "FORMATIONLAP": "FORMATION LAP",
    "TROPHYPHOTO": "TROPHY PHOTO",
    "CHAMPIONSCLUB": "CHAMPIONS CLUB",
    "COMMUNITYPITLANEWALK": "COMMUNITY PIT LANE WALK",
    "OPENINGNIGHTEVENT": "OPENING NIGHT EVENT",
    "PRECISIONDRIVECLUB": "PRECISION DRIVE CLUB",
    "PARTNERLAPS": "PARTNER LAPS",
    "HIGHSPEED": "HIGH SPEED",
    "LOWSPEED": "LOW SPEED",
    "TRACKTOUR": "TRACK TOUR",
    "MEDICALINTERVENTIONEXERCISE": "MEDICAL INTERVENTION EXERCISE",
}


def _clean_timetable_cell(value: str) -> str:
    text = value
    for source in sorted(TIMETABLE_TOKEN_REPLACEMENTS, key=len, reverse=True):
        text = re.sub(source, TIMETABLE_TOKEN_REPLACEMENTS[source], text)
    text = re.sub(r"(?<=[A-Z])&(?=[A-Z])", " & ", text)
    text = re.sub(r"(?<=[A-Z])(?=\()", " ", text)
    text = re.sub(r"(?<=\))(?=[A-Z])", " ", text)
    text = re.sub(r"(?<=[A-Z])(?=\d)", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Z])", " ", text)
    text = re.sub(r"\bF\s+([12])\b", r"F\1", text)
    text = re.sub(r"FIA/F\s+1", "FIA/F1", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\s*-\s*", " - ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_timetable_rows(rows: list[list[str]]) -> list[list[str]]:
    normalized = [["Start", "End", "Series / Group", "Location", "Activity"]]
    for row in rows:
        padded = (row + [""] * 5)[:5]
        if not re.match(r"^\d{1,2}:\d{2}\*?$", padded[0]):
            continue
        normalized.append([_clean_timetable_cell(cell) for cell in padded])
    return normalized if len(normalized) > 1 else []


def normalize_pdf_table(raw_table: list[list[Any]], *, doc_type: str | None = None) -> list[list[str]]:
    rows = [[clean_table_cell(cell) for cell in row] for row in raw_table]
    if not rows:
        return []
    max_cols = max((len(row) for row in rows), default=0)
    rows = [row + [""] * (max_cols - len(row)) for row in rows]
    rows = [row for row in rows if any(row)]
    if not rows:
        return []

    if doc_type in {"competition-visa", "timetable"} and max_cols == 5:
        timetable = _normalize_timetable_rows(rows)
        if timetable:
            return timetable

    if doc_type == "championship-points":
        championship = _normalize_championship_table_rows(rows)
        if championship:
            return championship

    flattened_start = " ".join(" ".join(row) for row in rows[:2]).lower()
    is_update_table = (
        "updated" in flattened_start
        and "component" in flattened_start
        and "primary reason" in flattened_start
        and "brief description" in flattened_start
    )
    if doc_type == "car-presentation" and is_update_table:
        return _normalize_update_table_rows(rows, max_cols)

    first_cells = table_non_empty_cells(rows[0])
    is_numbered_update_continuation = first_cells and first_cells[0].isdigit() and 4 <= len(first_cells) <= 6
    is_text_update_continuation = (
        len(first_cells) == 1
        and any(table_non_empty_cells(row) and table_non_empty_cells(row)[0].isdigit() for row in rows[1:])
    )
    if doc_type == "car-presentation" and (is_numbered_update_continuation or is_text_update_continuation):
        # Continuation pages in FIA car-presentation packs often omit the
        # column header row. Add the stable header so they do not render with
        # the first update as the Markdown table header.
        return _normalize_update_table_rows(rows, max_cols)

    keep_cols = [
        col_index
        for col_index in range(max_cols)
        if any(row[col_index] for row in rows)
    ]
    rows = [[row[col_index] for col_index in keep_cols] for row in rows]

    folded: list[list[str]] = []
    for row in rows:
        non_empty_indexes = [index for index, cell in enumerate(row) if cell]
        if folded and not row[0] and 0 < len(non_empty_indexes) <= 2:
            for index in non_empty_indexes:
                folded[-1][index] = f"{folded[-1][index]} {row[index]}".strip()
        else:
            folded.append(row)
    rows = folded

    # Drop single-column memo/header blocks. They are already represented by
    # normal text extraction and do not benefit from Markdown table formatting.
    effective_cols = max((sum(1 for cell in row if cell) for row in rows), default=0)
    if len(rows) < 2 or effective_cols < 2:
        return []
    return rows


def markdown_escape_table_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


def table_to_markdown(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    max_cols = max(len(row) for row in rows)
    padded = [row + [""] * (max_cols - len(row)) for row in rows]
    header = [markdown_escape_table_cell(cell) for cell in padded[0]]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in range(max_cols)) + " |",
    ]
    for row in padded[1:]:
        lines.append("| " + " | ".join(markdown_escape_table_cell(cell) for cell in row) + " |")
    return "\n".join(lines)


def extract_tables(pdf_path: Path, *, doc_type: str | None = None) -> list[dict[str, Any]]:
    try:
        import pdfplumber
    except ImportError:
        return []

    tables: list[dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                try:
                    found_tables = page.find_tables()
                except Exception:
                    found_tables = []
                for table in found_tables:
                    raw_table = table.extract()
                    rows = normalize_pdf_table(raw_table, doc_type=doc_type)
                    if not rows:
                        continue
                    tables.append(
                        {
                            "page": page_index,
                            "bbox": [round(float(value), 2) for value in table.bbox],
                            "rows": rows,
                        }
                    )
                if found_tables:
                    continue
                for raw_table in page.extract_tables() or []:
                    rows = normalize_pdf_table(raw_table, doc_type=doc_type)
                    if not rows:
                        continue
                    tables.append(
                        {
                            "page": page_index,
                            "bbox": None,
                            "rows": rows,
                        }
                    )
    except Exception:
        return []
    return tables


def is_update_table_rows(rows: list[list[str]]) -> bool:
    return bool(rows) and rows[0] == UPDATE_TABLE_HEADER


def merge_table_continuations(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for table in tables:
        rows = [list(row) for row in table.get("rows", [])]
        if (
            merged
            and is_update_table_rows(rows)
            and len(rows) > 1
            and not rows[1][0]
            and is_update_table_rows(merged[-1].get("rows", []))
        ):
            previous_rows = merged[-1]["rows"]
            if len(previous_rows) > 1 and rows[1][-1]:
                previous_rows[-1][-1] = f"{previous_rows[-1][-1]} {rows[1][-1]}".strip()
            previous_rows.extend(rows[2:])
            continue
        merged.append({**table, "rows": rows})
    return merged


def _bbox_contains_point(
    bbox: tuple[float, float, float, float],
    x: float,
    y: float,
    *,
    margin: float = 2.0,
) -> bool:
    x0, y0, x1, y1 = bbox
    return x0 - margin <= x <= x1 + margin and y0 - margin <= y <= y1 + margin


def _table_bboxes_for_page(tables: list[dict[str, Any]], page: int) -> list[tuple[float, float, float, float]]:
    bboxes: list[tuple[float, float, float, float]] = []
    for table in tables:
        if table.get("page") != page or not table.get("bbox"):
            continue
        bbox = table["bbox"]
        if len(bbox) == 4:
            bboxes.append(tuple(float(value) for value in bbox))
    return bboxes


def _words_to_markdown(words: list[dict[str, Any]]) -> str:
    if not words:
        return ""
    lines: list[tuple[float, str]] = []
    current_top: float | None = None
    current_words: list[dict[str, Any]] = []
    for word in sorted(words, key=lambda item: (float(item["top"]), float(item["x0"]))):
        top = float(word["top"])
        if current_top is None or abs(top - current_top) <= 3.0:
            current_words.append(word)
            current_top = top if current_top is None else (current_top + top) / 2
            continue
        text = " ".join(str(item["text"]) for item in sorted(current_words, key=lambda item: float(item["x0"]))).strip()
        if text:
            lines.append((current_top, text))
        current_words = [word]
        current_top = top
    if current_words and current_top is not None:
        text = " ".join(str(item["text"]) for item in sorted(current_words, key=lambda item: float(item["x0"]))).strip()
        if text:
            lines.append((current_top, text))

    paragraphs: list[str] = []
    current: list[str] = []
    previous_top: float | None = None
    for top, text in lines:
        gap = 0.0 if previous_top is None else top - previous_top
        if current and gap > 14:
            paragraphs.append(" ".join(current).strip())
            current = []
        current.append(text)
        previous_top = top
    if current:
        paragraphs.append(" ".join(current).strip())
    return "\n\n".join(paragraph for paragraph in paragraphs if paragraph)


def _words_to_markdown_blocks(words: list[dict[str, Any]]) -> list[tuple[float, str]]:
    if not words:
        return []
    lines: list[tuple[float, str]] = []
    current_top: float | None = None
    current_words: list[dict[str, Any]] = []
    for word in sorted(words, key=lambda item: (float(item["top"]), float(item["x0"]))):
        top = float(word["top"])
        if current_top is None or abs(top - current_top) <= 3.0:
            current_words.append(word)
            current_top = top if current_top is None else (current_top + top) / 2
            continue
        text = " ".join(str(item["text"]) for item in sorted(current_words, key=lambda item: float(item["x0"]))).strip()
        if text:
            lines.append((current_top, text))
        current_words = [word]
        current_top = top
    if current_words and current_top is not None:
        text = " ".join(str(item["text"]) for item in sorted(current_words, key=lambda item: float(item["x0"]))).strip()
        if text:
            lines.append((current_top, text))

    blocks: list[tuple[float, str]] = []
    current: list[str] = []
    current_start: float | None = None
    previous_top: float | None = None
    for top, text in lines:
        gap = 0.0 if previous_top is None else top - previous_top
        if current and gap > 14:
            start = current_start if current_start is not None else previous_top or top
            blocks.append((start, " ".join(current).strip()))
            current = []
            current_start = None
        if current_start is None:
            current_start = top
        current.append(text)
        previous_top = top
    if current:
        blocks.append((current_start if current_start is not None else 0.0, " ".join(current).strip()))
    return [(y, text) for y, text in blocks if text]


def _image_reference_markdown(image: dict[str, Any]) -> str:
    return f"![Extracted image from page {image['page']}]({image['path']})"


def compose_markdown_with_layout(
    pdf_path: Path,
    fallback_markdown: str,
    tables: list[dict[str, Any]],
    images: list[dict[str, Any]],
) -> str:
    if not tables and not images:
        return fallback_markdown
    try:
        import pdfplumber
    except ImportError:
        return fallback_markdown

    layout_tables = tables
    tables = merge_table_continuations(tables)

    images_by_page: dict[int, list[dict[str, Any]]] = {}
    for image in images:
        images_by_page.setdefault(int(image["page"]), []).append(image)

    tables_by_page: dict[int, list[dict[str, Any]]] = {}
    for table in tables:
        tables_by_page.setdefault(int(table["page"]), []).append(table)

    page_markdown: list[str] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                table_bboxes = _table_bboxes_for_page(layout_tables, page_index)
                rendered_page_images = [
                    image
                    for image in images_by_page.get(page_index, [])
                    if image.get("rendered_page")
                ]
                if rendered_page_images:
                    page_markdown.extend(_image_reference_markdown(image) for image in rendered_page_images)
                    continue

                words = []
                for word in page.extract_words(x_tolerance=1.5, y_tolerance=3, use_text_flow=False) or []:
                    center_x = (float(word["x0"]) + float(word["x1"])) / 2
                    center_y = (float(word["top"]) + float(word["bottom"])) / 2
                    if any(_bbox_contains_point(bbox, center_x, center_y) for bbox in table_bboxes):
                        continue
                    words.append(word)

                elements: list[tuple[float, int, str]] = []
                for y, text in _words_to_markdown_blocks(words):
                    elements.append((y, 0, text))
                for table in tables_by_page.get(page_index, []):
                    table_markdown = table_to_markdown(table["rows"])
                    if not table_markdown:
                        continue
                    bbox = table.get("bbox")
                    y = float(bbox[1]) if bbox and len(bbox) == 4 else float(page.height)
                    elements.append((y, 1, table_markdown))
                for image in images_by_page.get(page_index, []):
                    bbox = image.get("bbox") or [0, float(page.height), 0, float(page.height)]
                    y = float(bbox[1]) if len(bbox) == 4 else float(page.height)
                    elements.append((y, 2, _image_reference_markdown(image)))

                for _y, _kind, content in sorted(elements, key=lambda item: (item[0], item[1])):
                    content = content.strip()
                    if content:
                        page_markdown.append(content)
    except Exception:
        return fallback_markdown

    return clean_markdown("\n\n".join(page_markdown)) or fallback_markdown


def extraction_config() -> Any:
    from kreuzberg import ExtractionConfig, OutputFormat, PageConfig, PdfConfig

    return ExtractionConfig(
        output_format=OutputFormat.MARKDOWN,
        pdf_options=PdfConfig(extract_images=False, extract_metadata=True),
        pages=PageConfig(extract_pages=True),
    )


def _pymupdf_markdown(pdf_path: Path) -> str:
    """Fallback text extraction via PyMuPDF when kreuzberg fails."""
    import fitz

    doc = fitz.open(pdf_path)
    pages: list[str] = []
    try:
        for page in doc:
            pages.append(page.get_text("text"))
    finally:
        doc.close()
    return "\n\n".join(pages)


def extract_pdf(job: PdfJob, *, output_dir: Path = EXTRACTED_DIR) -> dict[str, Any]:
    from kreuzberg import extract_file_sync

    config = extraction_config()
    result: Any = None
    try:
        result = extract_file_sync(job.source_path, config=config)
        markdown = clean_markdown(getattr(result, "content", "") or "")
    except Exception as exc:
        # Fallback: use PyMuPDF for PDFs kreuzberg can't handle.
        console.print(f"[yellow]kreuzberg failed, using PyMuPDF fallback:[/yellow] {exc}")
        markdown = clean_markdown(_pymupdf_markdown(job.source_path))

    rel_parts = Path(job.relative_path).parts
    if len(rel_parts) < 4:
        raise ValueError(f"Unexpected PDF path layout: {job.relative_path}")
    year, event_slug = rel_parts[1], rel_parts[2]
    event_output_dir = output_dir / year / event_slug
    doc_slug = job.source_path.stem

    doc_type = detect_doc_type(job.source_path)
    images = extract_images(job.source_path, event_output_dir / "images", doc_slug, doc_type=doc_type)
    raw_tables = extract_tables(job.source_path, doc_type=doc_type)
    tables = merge_table_continuations(raw_tables)
    metadata = base_metadata(job.source_path, markdown, result, job, len(images), len(tables))
    if images:
        metadata["images"] = images
    if tables:
        metadata["tables"] = [
            {
                "page": table["page"],
                "bbox": table.get("bbox"),
                "rows": len(table["rows"]),
                "columns": max((len(row) for row in table["rows"]), default=0),
            }
            for table in tables
        ]
    markdown = compose_markdown_with_layout(job.source_path, markdown, raw_tables, images)

    write_text_atomic(event_output_dir / f"{doc_slug}.md", markdown.rstrip() + "\n")
    write_json_atomic(event_output_dir / f"{doc_slug}.json", metadata)

    return {
        "extracted_at": metadata["extracted_at"],
        "source_hash": job.source_hash,
        "pages": metadata.get("pages"),
        "images_extracted": metadata.get("images_extracted", 0),
        "tables_extracted": metadata.get("tables_extracted", 0),
        "doc_type": metadata.get("doc_type"),
        "success": True,
        "error": None,
    }


def clean_markdown(markdown: str) -> str:
    markdown = markdown.replace("\r\n", "\n").replace("\r", "\n")
    markdown = markdown.replace("(cid:120)", "-")
    markdown = re.sub(r"\(cid:\d+\)", " ", markdown)
    markdown = re.sub(
        r"\b(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)(\d{2})([A-Z]+)(\d{4})\b",
        r"\1 \2 \3 \4",
        markdown,
    )
    markdown = re.sub(r"\n{3,}", "\n\n", markdown)
    return markdown.strip()


def append_image_references(markdown: str, images: list[dict[str, Any]]) -> str:
    lines = [markdown.rstrip(), "", "## Extracted Images"]
    for image in images:
        path = image["path"]
        page = image["page"]
        lines.append(f"![Extracted image from page {page}]({path})")
    return "\n".join(lines).strip()


def failure_entry(job: PdfJob, error: Exception) -> dict[str, Any]:
    return {
        "extracted_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_hash": job.source_hash,
        "pages": None,
        "images_extracted": 0,
        "tables_extracted": 0,
        "doc_type": detect_doc_type(job.source_path),
        "success": False,
        "error": str(error),
    }


def select_jobs(args: argparse.Namespace, manifest: dict[str, Any]) -> list[PdfJob]:
    pdfs = discover_pdfs(args.documents_dir, year=args.year, event=args.event)
    jobs = [make_job(path, args.documents_dir) for path in pdfs]
    jobs = [job for job in jobs if needs_extraction(job, manifest, force=args.force)]
    if args.limit is not None:
        jobs = jobs[: args.limit]
    return jobs


def count_new(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.output_dir)
    return len(select_jobs(args, manifest))


def write_github_output(extracted_count: int) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    with open(github_output, "a", encoding="utf-8") as f:
        f.write(f"extracted_count={extracted_count}\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, help="Only process one season")
    parser.add_argument("--event", help="Only process one event slug; requires --year")
    parser.add_argument("--force", action="store_true", help="Re-extract even when the manifest hash matches")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List PDFs that would be extracted without writing outputs",
    )
    parser.add_argument("--limit", type=int, help="Maximum PDFs to process")
    parser.add_argument("--count-new", action="store_true", help="Print the number of PDFs needing extraction and exit")
    parser.add_argument("--documents-dir", type=Path, default=DOCUMENTS_DIR)
    parser.add_argument("--output-dir", type=Path, default=EXTRACTED_DIR)
    args = parser.parse_args(argv)
    if args.event and args.year is None:
        parser.error("--event requires --year")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be greater than 0")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    manifest = load_manifest(args.output_dir)
    jobs = select_jobs(args, manifest)

    if args.count_new:
        print(len(jobs))
        return 0

    if args.dry_run:
        for job in jobs:
            console.print(job.relative_path)
        console.print(f"[bold]{len(jobs)} PDF(s) need extraction[/bold]")
        return 0

    extracted_count = 0
    if not jobs:
        console.print("[green]No PDFs need extraction.[/green]")
        write_github_output(0)
        return 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Extracting PDFs", total=len(jobs))
        for job in jobs:
            progress.update(task, description=f"Extracting {Path(job.relative_path).name}")
            try:
                manifest[job.relative_path] = extract_pdf(job, output_dir=args.output_dir)
                extracted_count += 1
            except Exception as exc:
                manifest[job.relative_path] = failure_entry(job, exc)
                console.print(f"[red]Failed[/red] {job.relative_path}: {exc}")
            finally:
                save_manifest(manifest, args.output_dir)
                progress.advance(task)

    write_github_output(extracted_count)
    console.print(f"[green]Extracted {extracted_count} PDF(s).[/green]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
