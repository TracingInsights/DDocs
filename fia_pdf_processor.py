import os
import json
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

import fitz  # PyMuPDF
import pdfplumber
import re
import markdown2
from PIL import Image
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# Try to import marker, handle if not installed
try:
    from marker.convert import convert_single_pdf
    from marker.models import load_all_models
    HAS_MARKER = True
except ImportError:
    HAS_MARKER = False

# ============================================================================
# CONFIGURATION
# ============================================================================
# Year to process
YEAR = 2026

# Event slug to process (e.g., 'miami-grand-prix')
EVENT_SLUG = "miami-grand-prix"

# Process only a single document for quality testing?
SINGLE_DOC_TEST = True 

# If SINGLE_DOC_TEST is True, which document to process? (Partial match supported)
# Leave empty to pick the first one found.
TEST_DOC_PATTERN = "decision-car-14" 

# Use Marker-PDF (slow but high quality) or fallback to PyMuPDF text?
USE_MARKER = True

# Output directory for processed files
OUTPUT_DIR = Path("processed_documents")

# Source documents directory
DOCS_DIR = Path("documents")
# ============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
console = Console()

class FIAPDFProcessor:
    def __init__(self):
        self.output_base = OUTPUT_DIR
        self.output_base.mkdir(parents=True, exist_ok=True)
        self.models = None
        if HAS_MARKER and USE_MARKER:
            log.info("Loading Marker-PDF models (this may take a minute)...")
            self.models = load_all_models()

    def detect_page_orientation(self, page: fitz.Page) -> str:
        rect = page.rect
        return "landscape" if rect.width > rect.height else "portrait"

    def detect_doc_type(self, filename: str) -> str:
        f = filename.lower()
        if "decision" in f: return "decision"
        if "summons" in f: return "summons"
        if "infringement" in f: return "infringement"
        if "classification" in f: return "classification"
        if "starting-grid" in f: return "starting-grid"
        if "notes" in f or "race-directors" in f: return "notes"
        if "scrutineering" in f: return "scrutineering"
        if "procedure" in f: return "procedure"
        return "generic"

    def get_tailwind_template(self, content_html: str, metadata: Dict, images: List[Dict]) -> str:
        doc_type = metadata.get("type", "generic").upper()
        
        # Find header image if exists
        header_img = None
        for img in images:
            if "page1_img0" in img["filename"]:
                header_img = f"images/{img['filename']}"
                break

        m = metadata.get("extracted_meta", {})
        stewards = m.get("Stewards", ["Nish Shetty", "Natalie Corsmit", "Vitantonio Liuzzi", "Steve Pence"])
        if isinstance(stewards, str): stewards = [s.strip() for s in stewards.split(",")]

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{metadata.get('title', 'FIA Document')}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=Titillium+Web:wght@400;700&display=swap" rel="stylesheet">
    <style>
        body {{ font-family: 'Inter', sans-serif; background: #525659; padding: 40px 0; color: #000; -webkit-print-color-adjust: exact; }}
        .a4-page {{ width: 210mm; min-height: 297mm; background: white; margin: 0 auto; padding: 12mm 18mm; box-shadow: 0 0 15px rgba(0,0,0,0.3); position: relative; box-sizing: border-box; }}
        .header-img {{ width: 100%; margin-bottom: 5px; }}
        
        .event-header {{ font-family: 'Inter', sans-serif; text-align: center; margin-bottom: 15px; }}
        .event-title {{ font-weight: 800; font-size: 26px; text-transform: uppercase; margin-bottom: 2px; }}
        .event-date {{ font-weight: 700; font-size: 15px; margin-bottom: 15px; }}
        .header-separator {{ border-bottom: 1px solid #000; width: 100%; margin-bottom: 15px; }}
        
        .meta-table {{ width: 100%; border-collapse: collapse; margin-bottom: 25px; table-layout: fixed; }}
        .meta-table td {{ border: 0.5px solid #000; padding: 4px 10px; font-family: 'Inter', sans-serif; font-size: 13px; vertical-align: top; }}
        .meta-label {{ font-weight: 700; width: 80px; text-transform: none; }}
        .meta-value {{ font-weight: 400; }}
        .meta-doc-num {{ font-weight: 700; text-align: center; font-size: 16px; }}
        
        .content-body {{ font-family: 'Inter', sans-serif; font-size: 13.5px; line-height: 1.6; text-align: justify; margin-top: 10px; }}
        .decision-item {{ display: flex; margin-bottom: 8px; align-items: flex-start; }}
        .decision-label {{ width: 140px; font-weight: 700; font-size: 13.5px; flex-shrink: 0; }}
        .decision-text {{ flex-grow: 1; }}
        
        .signature-section {{ margin-top: 50px; display: grid; grid-template-cols: repeat(2, 1fr); gap: 40px 100px; }}
        .sig-box {{ border-top: 0.5px solid #000; text-align: left; padding-top: 5px; font-family: 'Inter', sans-serif; font-weight: 400; font-size: 12px; min-height: 40px; }}
        
        .footer-text {{ margin-top: 40px; font-size: 9px; text-align: left; color: #000; font-family: 'Inter', sans-serif; border-top: 0.5px solid #000; padding-top: 10px; }}
        
        @media print {{
            body {{ background: white; padding: 0; }}
            .a4-page {{ margin: 0; box-shadow: none; border: none; width: 100%; min-height: 100vh; }}
            .no-print {{ display: none; }}
        }}
    </style>
</head>
<body>
    <div class="a4-page">
        <!-- Logo -->
        {f'<img src="{header_img}" class="header-img">' if header_img else '<div class="h-20 bg-gray-100 flex items-center justify-center font-bold">FIA HEADER</div>'}
        
        <!-- Event Header -->
        <div class="event-header">
            <div class="event-title">{metadata.get('event', '').upper()} {metadata.get('year', '')}</div>
            <div class="event-date">{m.get('EventDates', '01 - 03 May 2026')}</div>
            <div class="header-separator"></div>
        </div>

        <!-- Metadata Table -->
        <table class="meta-table">
            <tr>
                <td class="meta-label">From</td>
                <td class="meta-value">The Stewards</td>
                <td class="meta-label" style="width:100px;">Document</td>
                <td class="meta-doc-num">{m.get('Document', '0')}</td>
            </tr>
            <tr>
                <td class="meta-label">To</td>
                <td class="meta-value">{m.get('To', 'The Team Manager')}</td>
                <td class="meta-label">Date</td>
                <td style="text-align:center;">{m.get('Date', '')}</td>
            </tr>
            <tr>
                <td colspan="2" style="border:none;"></td>
                <td class="meta-label">Time</td>
                <td style="text-align:center;">{m.get('Time', '')}</td>
            </tr>
        </table>

        <div class="content-body">
            {content_html}
        </div>

        <!-- Signatures -->
        <div class="signature-section">
            {" ".join([f'<div class="sig-box">{name}</div>' for name in stewards])}
        </div>

        <div class="footer-text">
            Decisions of the Stewards are taken independently of the FIA and are based solely on the relevant regulations, guidelines and evidence presented.<br><br>
            Official Document of the FIA Formula One World Championship<br>
            © {metadata.get('year')} Fédération Internationale de l'Automobile
        </div>
    </div>
</body>
</html>'''


    def process_single_pdf(self, pdf_path: Path, event_output_dir: Path) -> Dict:
        filename = pdf_path.name
        doc_slug = pdf_path.stem
        doc_output_dir = event_output_dir / doc_slug
        doc_output_dir.mkdir(parents=True, exist_ok=True)
        images_dir = doc_output_dir / "images"
        images_dir.mkdir(exist_ok=True)

        result = {
            "filename": filename,
            "title": doc_slug.replace("-", " ").title(),
            "type": self.detect_doc_type(filename),
            "processed_at": datetime.now().isoformat(),
            "year": YEAR,
            "event": EVENT_SLUG.replace("-", " ").title(),
            "event_slug": EVENT_SLUG,
            "success": False,
            "extracted_meta": {}
        }

        try:
            doc = fitz.open(pdf_path)
            result["pages"] = len(doc)
            result["orientations"] = {i+1: self.detect_page_orientation(page) for i, page in enumerate(doc)}
            
            # Stage 1: Extraction & Metadata Discovery
            first_page_text = doc[0].get_text()
            
            # Better Metadata Extraction
            for label in ["Document", "Date", "Time", "To"]:
                pattern = rf"{label}\s+(.+)"
                match = re.search(pattern, first_page_text)
                if match:
                    val = match.group(1).strip()
                    # Clean up some common artifacts
                    val = val.split("The Stewards")[0].strip()
                    result["extracted_meta"][label] = val

            # Extract Event Dates (usually on first page, e.g. "01 - 03 May 2026")
            date_range_match = re.search(r"(\d{2}\s*-\s*\d{2}\s+[A-Za-z]+\s+\d{4})", first_page_text)
            if date_range_match:
                result["extracted_meta"]["EventDates"] = date_range_match.group(1)

            # Extract Stewards (usually at the end of the doc)
            last_page_text = doc[-1].get_text()
            steward_names = []
            # Look for common steward names or the pattern after 'Decisions of the Stewards'
            # Or just look for the list of names usually at the bottom
            sig_pattern = r"(?:Decisions of the Stewards are taken independently|presented\.)\s+(.+)"
            sig_match = re.search(sig_pattern, last_page_text, re.DOTALL)
            if sig_match:
                potential_names = sig_match.group(1).strip().split("\n")
                steward_names = [n.strip() for n in potential_names if n.strip() and len(n.strip()) > 3 and not n.strip().startswith("©")]
                result["extracted_meta"]["Stewards"] = steward_names[:4] # Usually 4 stewards

            # Images
            extracted_images = []
            for i, page in enumerate(doc):
                for img_idx, img in enumerate(page.get_images()):
                    try:
                        xref = img[0]
                        pix = doc.extract_image(xref)
                        img_name = f"page{i+1}_img{img_idx}.{pix['ext']}"
                        (images_dir / img_name).write_bytes(pix["image"])
                        extracted_images.append({"page": i+1, "filename": img_name, "format": pix["ext"]})
                    except Exception as e: log.warning(f"Image fail: {e}")
            
            # Stage 2: Markdown Extraction
            markdown_content = ""
            if HAS_MARKER and USE_MARKER:
                out_md, _, _ = convert_single_pdf(str(pdf_path), self.models)
                markdown_content = out_md
            else:
                markdown_content = "\n\n".join([p.get_text() for p in doc])

            # Normalize whitespace and non-breaking spaces
            markdown_content = markdown_content.replace("\xa0", " ")
            
            # Cleanup redundant header info from body
            content_start_patterns = [
                "The Stewards, having received",
                "The Stewards, having considered",
                "having received a report",
                "The Stewards determine",
                "determined the following"
            ]
            clean_md = markdown_content
            for pattern in content_start_patterns:
                idx = clean_md.lower().find(pattern.lower())
                if idx != -1:
                    clean_md = clean_md[idx:].strip()
                    break

            # Format Decision Labels
            # Use alphanumeric markers to avoid markdown interpreting underscores as italics
            labels = ["No / Driver", "Competitor", "Infringement", "Decision", "Offence", "Session", "Reason", "Time", "Fact", "Date"]
            
            formatted_md = clean_md
            for label in labels:
                pattern = rf"(^|\n)[ ]*({label})\s*(:)?\s+"
                formatted_md = re.sub(pattern, rf"\n\nMARKERSTART\2MARKERSEP", formatted_md, flags=re.IGNORECASE)

            content_html = markdown2.markdown(formatted_md, extras=["tables", "fenced-code-blocks", "smarty-pants"])

            # Now replace markers with actual HTML
            content_html = content_html.replace("MARKERSTART", '</div></div><div class="decision-item">MARKERLABEL')
            content_html = content_html.replace("MARKERSEP", 'MARKERVALUE')
            
            for label in labels:
                content_html = re.sub(rf"MARKERLABEL({label})MARKERVALUE", 
                                      rf'<div class="decision-label">\1</div><div class="decision-text">', 
                                      content_html, flags=re.IGNORECASE)

            # Fix the first and last tags
            if '<div class="decision-item">' in content_html:
                content_html = content_html.replace('</div></div><div class="decision-item">', '<div class="decision-item">', 1)
                content_html += "</div></div>"

            # Final cleanup
            content_html = content_html.replace("<p></div></div>", "</div></div>")
            content_html = content_html.replace("</div></div></p>", "</div></div>")
            content_html = re.sub(r"<p>\s*</p>", "", content_html)
            content_html = content_html.replace("MARKERLABEL", "")
            content_html = content_html.replace("MARKERVALUE", "")
            
            # Remove steward names if they appear at the end of content_html
            if steward_names:
                for name in steward_names:
                    # Remove from anywhere in body to be safe, especially if at the end
                    content_html = re.sub(rf"<p>{re.escape(name)}</p>", "", content_html, flags=re.IGNORECASE)
                    content_html = content_html.replace(name, "")

            html_output = self.get_tailwind_template(content_html, result, extracted_images)

            (doc_output_dir / "document.html").write_text(html_output, encoding="utf-8")
            (doc_output_dir / "document.md").write_text(markdown_content, encoding="utf-8")
            (doc_output_dir / "metadata.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            
            result["success"] = True
            log.info(f"  [✓] Success: {doc_slug}")
            
        except Exception as e:
            log.error(f"Error: {e}")
            result["error"] = str(e)
        finally:
            if 'doc' in locals(): doc.close()
        
        return result

    def create_json_file(self, path: Path, data: Any):
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def run(self):
        event_dir = DOCS_DIR / str(YEAR) / EVENT_SLUG
        if not event_dir.exists():
            log.error(f"Event directory not found: {event_dir}")
            return

        pdf_files = sorted(list(event_dir.glob("*.pdf")))
        if SINGLE_DOC_TEST:
            if TEST_DOC_PATTERN:
                filtered = [f for f in pdf_files if TEST_DOC_PATTERN in f.name]
                pdf_files = [filtered[0]] if filtered else ([pdf_files[0]] if pdf_files else [])
            elif pdf_files: pdf_files = [pdf_files[0]]

        if not pdf_files: return

        event_output_dir = self.output_base / str(YEAR) / EVENT_SLUG
        event_output_dir.mkdir(parents=True, exist_ok=True)

        results = []
        for pdf in pdf_files:
            res = self.process_single_pdf(pdf, event_output_dir)
            results.append(res)

        log.info(f"Done. Processed {len(results)} files.")

if __name__ == "__main__":
    processor = FIAPDFProcessor()
    processor.run()
