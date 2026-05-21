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
SINGLE_DOC_TEST = False 

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
        if "presentation" in f or "submission" in f: return "presentation"
        if "scrutineering" in f: return "scrutineering"
        if "procedure" in f: return "procedure"
        return "generic"

    def get_tailwind_template(self, content_html: str, metadata: Dict, images: List[Dict], appeals_note: str = "") -> str:
        header_img = None
        for img in images:
            if "page1_img0" in img["filename"]:
                header_img = f"images/{img['filename']}"
                break

        m = metadata.get("extracted_meta", {})
        stewards = m.get("Stewards", [])
        if isinstance(stewards, str): stewards = [s.strip() for s in stewards.split(",")]

        year = metadata.get('year', '')
        event = metadata.get('event', '')
        event_title = f"{year} {event}".upper().strip()

        sig_boxes = " ".join([f'<div class="sig-box">{name}</div>' for name in stewards]) if stewards else ""
        appeals_html = f'<p class="appeals-note">{appeals_note}</p>' if appeals_note else ''

        orientations = metadata.get("orientations", {})
        is_landscape = any(o == "landscape" for o in orientations.values())
        a4_class = "a4-page landscape" if is_landscape else "a4-page"

        from_val = m.get('From', 'The Stewards')
        is_decision = metadata.get("type") in ["decision", "infringement", "summons"]

        stewards_html = ""
        if stewards and is_decision:
            stewards_html = f'''
        <div class="signature-container">
            <div class="signature-grid">
                {sig_boxes}
            </div>
            <div class="stewards-title">The Stewards</div>
        </div>'''

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{metadata.get('title', 'FIA Document')}</title>
    <style>
        body {{ font-family: Arial, Helvetica, sans-serif; background: #525659; padding: 40px 0; color: #000; -webkit-print-color-adjust: exact; }}
        .a4-page {{ width: 100%; max-width: 210mm; min-height: 297mm; background: white; margin: 0 auto; padding: 14mm 20mm 18mm 20mm; box-shadow: 0 0 15px rgba(0,0,0,0.3); position: relative; box-sizing: border-box; }}
        .a4-page.landscape {{ max-width: 297mm; }}

        .header-img {{ width: 100%; margin-bottom: 8px; }}

        .event-header {{ text-align: center; margin-bottom: 4px; }}
        .event-title {{ font-weight: 700; font-size: 16px; margin-bottom: 2px; }}
        .event-date {{ font-weight: 400; font-size: 12px; }}

        .meta-table {{ width: 100%; border-collapse: collapse; border-top: 1px solid #000; border-bottom: 1px solid #000; margin-top: 6px; margin-bottom: 0; }}
        .meta-table td {{ padding: 2px 4px; font-size: 11.5px; vertical-align: top; border: none; }}
        .meta-label {{ font-weight: 700; width: 32px; white-space: nowrap; }}
        .meta-value {{ font-weight: 400; }}
        .meta-right-label {{ font-weight: 700; text-align: right; width: 75px; white-space: nowrap; }}
        .meta-right-value {{ font-weight: 400; padding-left: 8px; width: 80px; }}

        .content-body {{ overflow-x: auto; font-size: 11.5px; line-height: 1.5; margin-top: 12px; }}
        .content-body table {{ border-collapse: collapse; width: 100%; margin-bottom: 12px; }}
        .content-body th, .content-body td {{ border: 1px solid #000; padding: 4px; font-size: 11.5px; text-align: left; }}
        .intro-text {{ margin-bottom: 10px; }}
        .intro-text p {{ margin: 0; }}

        .decision-item {{ display: flex; margin-bottom: 4px; align-items: flex-start; }}
        .decision-label {{ width: 100px; font-weight: 700; flex-shrink: 0; }}
        .decision-text {{ flex-grow: 1; text-align: justify; }}
        .decision-text p {{ margin: 0; }}

        .body-note {{ margin-left: 100px; font-size: 11.5px; line-height: 1.5; margin-top: 10px; text-align: justify; }}

        .signature-container {{ margin-top: 40px; }}
        .signature-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 30px 60px; margin-bottom: 6px; }}
        .sig-box {{ text-align: left; font-size: 11.5px; padding-top: 20px; }}
        .stewards-title {{ font-size: 11.5px; font-weight: 700; margin-top: 4px; }}

        .footer-text {{ margin-top: 20px; font-size: 8px; text-align: left; color: #000; border-top: 0.5px solid #000; padding-top: 5px; line-height: 1.4; }}

        /* Table Styling */
        .table-wrapper {{ width: 100%; overflow-x: auto; margin: 20px 0; border: 1px solid #000; }}
        .fia-table {{ width: 100%; border-collapse: collapse; font-size: 11pt; line-height: 1.4; }}
        .fia-table th {{ background-color: #B9CDE5; border: 1px solid #000; padding: 8px; text-align: center; font-weight: bold; }}
        .fia-table td {{ border: 1px solid #000; padding: 8px; vertical-align: top; }}
        .fia-table tr:nth-child(even) {{ background-color: #f9f9f9; }}

        @media print {{
            body {{ background: white; padding: 0; }}
            .a4-page {{ margin: 0; box-shadow: none; border: none; width: 100%; min-height: 100vh; padding: 12mm 18mm; }}
        }}
    </style>
</head>
<body>
    <div class="{a4_class}">
        <!-- Logo -->
        {f'<img src="{header_img}" class="header-img">' if header_img else '<div style="height:55px;background:#f0f0f0;display:flex;align-items:center;justify-content:center;font-weight:700;">FIA HEADER</div>'}

        <!-- Event Header -->
        <div class="event-header">
            <div class="event-title">{event_title}</div>
            <div class="event-date">{m.get('EventDates', '')}</div>
        </div>

        <!-- Metadata Table (no colons, bordered top+bottom) -->
        <table class="meta-table">
            <tr>
                <td class="meta-label">From</td>
                <td class="meta-value">{from_val}</td>
                <td class="meta-right-label">Document</td>
                <td class="meta-right-value">{m.get('Document', '')}</td>
            </tr>
            <tr>
                <td class="meta-label">To</td>
                <td class="meta-value">{m.get('To', 'The Team Manager,')}</td>
                <td class="meta-right-label">Date</td>
                <td class="meta-right-value">{m.get('Date', '')}</td>
            </tr>
            <tr>
                <td colspan="2"></td>
                <td class="meta-right-label">Time</td>
                <td class="meta-right-value">{m.get('Time', '')}</td>
            </tr>
        </table>

        <div class="content-body">
            {content_html}
            {f'<div class="body-note">{appeals_note}</div>' if appeals_note else ''}
            <div class="body-note" style="margin-top:8px;">Decisions of the Stewards are taken independently of the FIA and are based solely on the relevant regulations, guidelines and evidence presented.</div>
        </div>

        <!-- Signatures conditionally rendered -->
        {stewards_html}

        <div class="footer-text">
            Official Document of the FIA Formula One World Championship<br>
            &copy; {metadata.get('year')} F&eacute;d&eacute;ration Internationale de l&apos;Automobile
        </div>
    </div>
</body>
</html>'''


    def render_tables_to_html(self, doc: fitz.Document) -> str:
        """Extracts all tables from the PDF and renders them as styled HTML using PyMuPDF."""
        html_output = []
        try:
            for page in doc:
                tabs = page.find_tables()
                if not tabs:
                    continue
                
                for table in tabs:
                    # table.extract() returns a list of lists of strings
                    extracted_data = table.extract()
                    if not extracted_data or len(extracted_data) < 1:
                        continue
                        
                    # Filter out very small or empty tables
                    if len(extracted_data) == 1 and (not extracted_data[0] or not any(extracted_data[0])):
                        continue
                        
                    html = ['<div class="table-wrapper"><table class="fia-table">']
                    
                    # Handle headers (first row)
                    headers = extracted_data[0]
                    if headers:
                        html.append('<thead><tr>')
                        for header in headers:
                            header_text = str(header).replace('\n', '<br>') if header else ""
                            html.append(f'<th>{header_text}</th>')
                        html.append('</tr></thead>')
                    
                    # Handle body
                    html.append('<tbody>')
                    for row in extracted_data[1:]:
                        if not any(row): continue # Skip empty rows
                        html.append('<tr>')
                        for cell in row:
                            cell_text = str(cell).replace('\n', '<br>') if cell else ""
                            html.append(f'<td>{cell_text}</td>')
                        html.append('</tr>')
                    html.append('</tbody>')
                    html.append('</table></div>')
                    html_output.append("".join(html))
        except Exception as e:
            log.error(f"Error extracting tables with PyMuPDF: {e}")
        return "".join(html_output)

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
            for label in ["From", "Document", "Date", "Time", "To"]:
                pattern = rf"(?:^|\n){label}\s+(.+)"
                match = re.search(pattern, first_page_text, re.IGNORECASE)
                if match:
                    val = match.group(1).strip()
                    # Clean up some common artifacts
                    if "The Stewards" in val and label != "From":
                        val = val.split("The Stewards")[0].strip()
                    if label == "From" and "Document" in val:
                        val = val.split("Document")[0].strip()
                    if label == "To" and "Date" in val:
                        val = val.split("Date")[0].strip()
                    result["extracted_meta"][label] = val

            # Extract Event Dates (usually on first page, e.g. "01 - 03 May 2026")
            date_range_match = re.search(r"(\d{2}\s*-\s*\d{2}\s+[A-Za-z]+\s+\d{4})", first_page_text)
            if date_range_match:
                result["extracted_meta"]["EventDates"] = date_range_match.group(1)

            # Extract Stewards (usually at the end of the doc)
            last_page_text = doc[-1].get_text()
            steward_names = []
            
            # Common pattern: names are after "The Stewards" or at the very end
            # We look for lines with 2-3 capitalized words, usually 4 names
            potential_stewards = []
            lines = last_page_text.split("\n")
            for line in reversed(lines):
                line = line.strip()
                if not line or len(line) < 5 or "©" in line or "FIA" in line: continue
                # Match "First Last" or "First Middle Last"
                if re.match(r"^[A-Z][a-z]+(\s+[A-Z][a-z]+){1,2}$", line):
                    potential_stewards.insert(0, line)
                if len(potential_stewards) >= 4: break
            
            if potential_stewards:
                result["extracted_meta"]["Stewards"] = potential_stewards
                steward_names = potential_stewards
            else:
                steward_names = []
                result["extracted_meta"]["Stewards"] = steward_names

            # Debug layout
            print(f"\nDEBUG: Analyzing {pdf_path}")
            page = doc[0]
            text_dict = page.get_text("dict")
            for block in text_dict["blocks"]:
                if "lines" in block:
                    for line in block["lines"]:
                        for span in line["spans"]:
                            if "Stewards" in span["text"] or "Document" in span["text"] or "From" in span["text"]:
                                print(f"DEBUG TEXT: '{span['text']}' at {span['bbox']}")
            
            drawings = page.get_drawings()
            for d in drawings:
                width = d.get("width", 0)
                if width is not None and width > 0:
                    print(f"DEBUG DRAWING: {d['type']} width {width} at {d.get('rect', d.get('pt1'))}")

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

            # Manual Parsing of content instead of pure markdown
            labels = ["No / Driver", "Competitor", "Time", "Session", "Fact", "Offence", "Infringement", "Decision", "Reason", "Date"]
            
            is_decision = result["type"] in ["decision", "infringement", "summons", "presentation"]
            appeals_note = ""

            if is_decision:
                # Pre-extract the appeals boilerplate before label parsing
                # This prevents "Competitors are reminded..." from matching "Competitor" label
                appeals_pattern = r"Competitors are reminded.*?(?=\n\n|\Z)"
                appeals_match = re.search(appeals_pattern, clean_md, re.DOTALL | re.IGNORECASE)
                if appeals_match:
                    appeals_note = appeals_match.group(0).strip()
                    clean_md = clean_md[:appeals_match.start()].strip()

                # Also strip any trailing "Decisions of the Stewards..." footer text
                footer_pattern = r"Decisions of the Stewards are taken independently.*"
                clean_md = re.sub(footer_pattern, "", clean_md, flags=re.IGNORECASE | re.DOTALL).strip()

                # Find the intro text (before the first label)
                # Use word-boundary aware matching: label must be followed by colon, space, or end-of-string
                intro_text = clean_md
                first_label_idx = float('inf')
                for label in labels:
                    # Match label as a whole word (not a prefix of another word)
                    m_obj = re.search(rf'(?:^|\n){re.escape(label)}(?=\s*[:\s]|$)', clean_md, re.IGNORECASE)
                    if m_obj and m_obj.start() < first_label_idx:
                        first_label_idx = m_obj.start()

                if first_label_idx != float('inf'):
                    intro_text = clean_md[:first_label_idx].strip()
                    items_content = clean_md[first_label_idx:].strip()
                else:
                    items_content = clean_md
                    intro_text = ""

                # Split by labels — require label to be followed by colon or whitespace (not a letter)
                item_data = []
                current_label = None
                current_text = ""

                lines = items_content.split("\n")
                for line in lines:
                    found_new_label = False
                    stripped = line.strip()
                    for label in labels:
                        # Match label at start of stripped line, followed by colon/space but NOT a letter
                        # This prevents "Competitor" matching "Competitors are reminded..."
                        if re.match(rf'^{re.escape(label)}(?=[:\s]|$)(?![a-zA-Z])', stripped, re.IGNORECASE):
                            if current_label:
                                item_data.append((current_label, current_text.strip()))
                            current_label = label
                            current_text = re.sub(rf'^{re.escape(label)}\s*[:]?\s*', '', stripped, flags=re.IGNORECASE)
                            found_new_label = True
                            break

                    if not found_new_label:
                        current_text += "\n" + line

                if current_label:
                    item_data.append((current_label, current_text.strip()))

                # Now build the HTML
                content_html = ""
                if intro_text:
                    # Cleanup intro text
                    clean_intro = intro_text
                    if steward_names:
                        for name in steward_names:
                            clean_intro = clean_intro.replace(name, "").strip()
                            
                    # For presentation docs, truncate the text before the table starts
                    if result["type"] == "presentation":
                        table_start_idx = clean_intro.lower().find("\nupdated")
                        if table_start_idx != -1:
                            clean_intro = clean_intro[:table_start_idx].strip()
                            
                    intro_md = markdown2.markdown(clean_intro, extras=["tables"])
                    content_html += f'<div class="intro-text">{intro_md}</div>'
                
                for label, text in item_data:
                    # Remove steward names if they leaked into the text
                    if steward_names:
                        for name in steward_names:
                            text = text.replace(name, "").strip()
                    
                    # Cleanup footer noise
                    noise_patterns = [
                        r"Decisions of the Stewards are taken independently.*",
                        r"Official Document of the FIA.*",
                        r"©.*Fédération Internationale de l'Automobile"
                    ]
                    for noise in noise_patterns:
                        text = re.sub(noise, "", text, flags=re.IGNORECASE | re.DOTALL).strip()

                    if not text: continue
                    
                    # Format the text with markdown if it's long (like Reason)
                    formatted_text = text
                    if len(text) > 100 or "\n" in text:
                        formatted_text = markdown2.markdown(text, extras=["tables"]).strip()
                        # Remove the wrapping <p> if it's just one
                        if formatted_text.startswith("<p>") and formatted_text.count("<p>") == 1:
                            formatted_text = formatted_text.replace("<p>", "").replace("</p>", "")
                    
                    content_html += f'''
                    <div class="decision-item">
                        <div class="decision-label">{label}</div>
                        <div class="decision-text">{formatted_text}</div>
                    </div>'''
                
                # Extract and inject real HTML tables for presentations or complex documents
                if result["type"] == "presentation" or "table" in clean_md.lower():
                    tables_html = self.render_tables_to_html(doc)
                    if tables_html:
                        content_html += f'<div class="extracted-tables-section"><h3>Document Tables</h3>{tables_html}</div>'

                # Final check for any leftover noise
                content_html = content_html.replace("<p></p>", "")
                content_html = re.sub(r"<p>\s*</p>", "", content_html)
                if steward_names:
                    for name in steward_names:
                        # Remove the name if it appears as a paragraph or at the end
                        content_html = re.sub(rf"<p>\s*{re.escape(name)}\s*</p>", "", content_html, flags=re.IGNORECASE)
                        # Also try to remove from the end of the text
                        content_html = content_html.replace(name, "")

                # Cleanup trailing noise like "Decisions of the Stewards..."
                noise_patterns = [
                    r"Decisions of the Stewards are taken independently.*",
                    r"Official Document of the FIA.*",
                    r"©.*Fédération Internationale de l'Automobile"
                ]
                for noise in noise_patterns:
                    content_html = re.sub(noise, "", content_html, flags=re.IGNORECASE | re.DOTALL)

                html_output = self.get_tailwind_template(content_html, result, extracted_images, appeals_note=appeals_note)

            else:
                # Real HTML and Table extraction for generic documents
                # Use markdown conversion but ensure tables are explicitly handled
                content_html = markdown2.markdown(markdown_content, extras=["tables"]).strip()
                
                # Force extract tables using PyMuPDF for 100% accuracy
                tables_html = self.render_tables_to_html(doc)
                if tables_html and tables_html not in content_html:
                    content_html += f'<div class="extracted-tables-section"><h3>Extracted Tables</h3>{tables_html}</div>'

                html_output = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{result.get('title', 'Document')}</title>
    <style>
        body {{ font-family: Arial, sans-serif; background: #525659; padding: 40px 0; }}
        .a4-page {{ background: white; box-shadow: 0 0 15px rgba(0,0,0,0.3); margin: 0 auto; width: 210mm; min-height: 297mm; padding: 20mm; box-sizing: border-box; }}
        .table-wrapper {{ width: 100%; overflow-x: auto; margin: 20px 0; border: 1px solid #000; }}
        .fia-table {{ width: 100%; border-collapse: collapse; font-size: 10pt; }}
        .fia-table th {{ background-color: #B9CDE5; border: 1px solid #000; padding: 6px; text-align: center; }}
        .fia-table td {{ border: 1px solid #000; padding: 6px; vertical-align: top; }}
    </style>
</head>
<body>
    <div class="a4-page">
        <h1>{result.get('title')}</h1>
        {content_html}
    </div>
</body>
</html>'''

            result["success"] = True
            (doc_output_dir / "document.html").write_text(html_output, encoding="utf-8")
            (doc_output_dir / "document.md").write_text(markdown_content, encoding="utf-8")
            (doc_output_dir / "metadata.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
            
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
