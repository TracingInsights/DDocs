"""FIA F1 PDF Processor - Hybrid Extraction with Marker + PyMuPDF + pdfplumber

Processes FIA decision documents with high-quality extraction preserving layout and styling.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import io

import fitz  # PyMuPDF
import pdfplumber
from PIL import Image
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.prompt import Confirm
from rich.table import Table

# ============================================================================
# CONFIGURATION - Edit these constants to select what to process
# ============================================================================

# Years to process (list of integers or ["all"] for all years)
YEARS = [2026]

# Events to process (list of event slugs or ["all"] for all events in selected years)
# Examples: ["miami-grand-prix", "chinese-grand-prix"] or ["all"]
EVENTS = ["miami-grand-prix"]

# Processing mode: "batch" or "interactive"
MODE = "batch"  # "batch" = process all at once, "interactive" = confirm each file

# Output directory for processed files
OUTPUT_DIR = "processed_documents"

# ============================================================================

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
console = Console()


def detect_page_orientation(page: fitz.Page) -> str:
    """Detect if page is portrait or landscape."""
    rect = page.rect
    return "landscape" if rect.width > rect.height else "portrait"


def extract_images_pymupdf(pdf_path: Path, output_dir: Path) -> list[dict]:
    """Extract high-quality images using PyMuPDF."""
    images = []
    doc = fitz.open(pdf_path)
    
    for page_num, page in enumerate(doc, 1):
        image_list = page.get_images()
        for img_idx, img in enumerate(image_list):
            try:
                xref = img[0]
                base_image = doc.extract_image(xref)
                img_bytes = base_image["image"]
                ext = base_image["ext"]
                
                img_filename = f"page{page_num}_img{img_idx}.{ext}"
                img_path = output_dir / img_filename
                img_path.write_bytes(img_bytes)
                
                images.append({
                    "page": page_num,
                    "filename": img_filename,
                    "format": ext,
                    "size": len(img_bytes)
                })
            except Exception as e:
                log.warning(f"Failed to extract image {img_idx} from page {page_num}: {e}")
    
    doc.close()
    return images


def extract_text_pymupdf(pdf_path: Path) -> tuple[str, dict]:
    """Extract text with formatting using PyMuPDF."""
    doc = fitz.open(pdf_path)
    full_text = []
    metadata = {
        "pages": len(doc),
        "orientations": {},
        "fonts": set(),
    }
    
    for page_num, page in enumerate(doc, 1):
        orientation = detect_page_orientation(page)
        metadata["orientations"][page_num] = orientation
        
        # Auto-rotate landscape pages for better text extraction
        if orientation == "landscape":
            page.set_rotation(90)
        
        blocks = page.get_text("dict")["blocks"]
        page_text = []
        
        for block in blocks:
            if block["type"] == 0:  # Text block
                for line in block.get("lines", []):
                    line_text = []
                    for span in line.get("spans", []):
                        text = span["text"].strip()
                        if text:
                            font = span["font"]
                            metadata["fonts"].add(font)
                            
                            # Bold for headers/important text
                            if "Bold" in font or span["size"] > 14:
                                text = f"**{text}**"
                            
                            line_text.append(text)
                    
                    if line_text:
                        page_text.append(" ".join(line_text))
        
        if page_text:
            full_text.append(f"\n## Page {page_num}\n\n" + "\n\n".join(page_text))
    
    doc.close()
    metadata["fonts"] = list(metadata["fonts"])
    return "\n\n".join(full_text), metadata


def extract_tables_pdfplumber(pdf_path: Path) -> list[dict]:
    """Extract tables using pdfplumber."""
    tables = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            page_tables = page.extract_tables()
            for table_idx, table in enumerate(page_tables):
                if table:
                    tables.append({
                        "page": page_num,
                        "index": table_idx,
                        "rows": len(table),
                        "cols": len(table[0]) if table else 0,
                        "data": table
                    })
    
    return tables


def format_table_markdown(table_data: list) -> str:
    """Convert table data to markdown format."""
    if not table_data:
        return ""
    
    lines = []
    # Header
    lines.append("| " + " | ".join(str(cell or "") for cell in table_data[0]) + " |")
    lines.append("| " + " | ".join("---" for _ in table_data[0]) + " |")
    
    # Rows
    for row in table_data[1:]:
        lines.append("| " + " | ".join(str(cell or "") for cell in row) + " |")
    
    return "\n".join(lines)


def generate_html(markdown_text: str, images: list[dict], tables: list[dict], metadata: dict, pdf_name: str) -> str:
    """Generate HTML with FIA styling."""
    html_parts = [f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{pdf_name}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Titillium+Web:wght@400;600;700&display=swap');
        
        body {{
            font-family: 'Titillium Web', Arial, sans-serif;
            margin: 40px auto;
            max-width: 1200px;
            background: #f5f5f5;
            line-height: 1.6;
        }}
        
        .document {{
            background: white;
            padding: 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .fia-header {{
            text-align: center;
            border-bottom: 3px solid #E10600;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        
        h1, h2 {{
            color: #E10600;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }}
        
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        
        th {{
            background-color: #E10600;
            color: white;
            font-weight: 600;
        }}
        
        img {{
            max-width: 100%;
            height: auto;
            margin: 20px 0;
        }}
        
        .metadata {{
            font-size: 12px;
            color: #666;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <div class="document">
        <div class="fia-header">
            <h1>{pdf_name}</h1>
        </div>
        <div class="content">
"""]
    
    # Add markdown content (converted to HTML paragraphs)
    for para in markdown_text.split("\n\n"):
        if para.strip():
            html_parts.append(f"            <p>{para}</p>")
    
    # Add tables
    if tables:
        html_parts.append("            <h2>Tables</h2>")
        for table in tables:
            html_parts.append(f"            <h3>Page {table['page']}, Table {table['index'] + 1}</h3>")
            html_parts.append(f"            {format_table_markdown(table['data'])}")
    
    # Add images
    if images:
        html_parts.append("            <h2>Images</h2>")
        for img in images:
            html_parts.append(f'            <img src="images/{img["filename"]}" alt="Page {img["page"]}, Image" />')
    
    # Add metadata
    html_parts.append(f"""        </div>
        <div class="metadata">
            <strong>Document Metadata:</strong><br>
            Pages: {metadata.get('pages', 'N/A')}<br>
            Processed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
            Images Extracted: {len(images)}<br>
            Tables Extracted: {len(tables)}
        </div>
    </div>
</body>
</html>""")
    
    return "\n".join(html_parts)


def process_pdf(pdf_path: Path, output_base: Path) -> dict:
    """Process a single PDF with hybrid extraction."""
    pdf_name = pdf_path.stem
    output_dir = output_base / pdf_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    images_dir = output_dir / "images"
    images_dir.mkdir(exist_ok=True)
    
    result = {
        "filename": pdf_path.name,
        "title": pdf_name.replace("-", " ").title(),
        "processed_at": datetime.now().isoformat(),
        "source_path": str(pdf_path),
        "output_path": str(output_dir),
        "success": False,
        "error": None
    }
    
    try:
        # Extract using hybrid approach
        console.print(f"  [cyan]→[/cyan] Extracting text with PyMuPDF...")
        text, metadata = extract_text_pymupdf(pdf_path)
        
        console.print(f"  [cyan]→[/cyan] Extracting images...")
        images = extract_images_pymupdf(pdf_path, images_dir)
        
        console.print(f"  [cyan]→[/cyan] Extracting tables with pdfplumber...")
        tables = extract_tables_pdfplumber(pdf_path)
        
        # Generate outputs
        console.print(f"  [cyan]→[/cyan] Generating HTML...")
        html = generate_html(text, images, tables, metadata, pdf_name)
        (output_dir / f"{pdf_name}.html").write_text(html, encoding="utf-8")
        
        console.print(f"  [cyan]→[/cyan] Saving markdown...")
        (output_dir / f"{pdf_name}.md").write_text(text, encoding="utf-8")
        
        console.print(f"  [cyan]→[/cyan] Saving metadata...")
        result.update({
            "success": True,
            "pages": metadata["pages"],
            "images_extracted": len(images),
            "tables_extracted": len(tables),
            "orientations": metadata["orientations"],
            "fonts": metadata["fonts"]
        })
        
        (output_dir / "metadata.json").write_text(
            json.dumps(result, indent=2),
            encoding="utf-8"
        )
        
        console.print(f"  [green]✓[/green] Processed successfully")
        
    except Exception as e:
        result["error"] = str(e)
        log.error(f"Failed to process {pdf_path.name}: {e}")
        console.print(f"  [red]✗[/red] Failed: {e}")
    
    return result


def create_index_json(event_dir: Path, results: list[dict]) -> None:
    """Create index.json for an event following existing format."""
    index_data = []
    
    for i, result in enumerate(sorted(results, key=lambda x: x["filename"]), 1):
        if result["success"]:
            entry = {
                "f": result["filename"],
                "t": result["title"],
                "n": i
            }
            # Add processed timestamp if available
            if "processed_at" in result:
                entry["p"] = datetime.fromisoformat(result["processed_at"]).strftime("%d.%m.%y %H:%M")
            
            index_data.append(entry)
    
    index_path = event_dir / "index.json"
    index_path.write_text(json.dumps(index_data, separators=(",", ":")), encoding="utf-8")
    console.print(f"[green]✓[/green] Created {index_path}")


def create_manifest_json(output_base: Path, all_results: dict) -> None:
    """Create manifest.json following existing format."""
    manifest = {}
    
    for (year, event), results in all_results.items():
        for result in results:
            if result["success"]:
                # Create a pseudo-URL key (following manifest pattern)
                url_key = f"processed://{year}/{event}/{result['filename']}"
                
                manifest[url_key] = {
                    "year": year,
                    "event": event,
                    "title": result["title"],
                    "source": "processed_pdf",
                    "path": result["output_path"],
                    "processed_at": result["processed_at"],
                    "pages": result.get("pages", 0),
                    "images": result.get("images_extracted", 0),
                    "tables": result.get("tables_extracted", 0)
                }
    
    manifest_path = output_base / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    console.print(f"[green]✓[/green] Created {manifest_path}")


def get_available_years(docs_dir: Path) -> list[int]:
    """Get all available years from documents directory."""
    years = []
    for item in docs_dir.iterdir():
        if item.is_dir() and item.name.isdigit():
            years.append(int(item.name))
    return sorted(years, reverse=True)


def get_available_events(docs_dir: Path, year: int) -> list[tuple[str, str]]:
    """Get all available events for a year. Returns [(slug, name), ...]"""
    year_dir = docs_dir / str(year)
    if not year_dir.exists():
        return []
    
    events = []
    for item in year_dir.iterdir():
        if item.is_dir() and not item.name.startswith("."):
            # Try to get proper name from index.json
            index_file = docs_dir / str(year) / "index.json"
            if index_file.exists():
                try:
                    index_data = json.loads(index_file.read_text())
                    for entry in index_data:
                        if entry["s"] == item.name:
                            events.append((item.name, entry["n"]))
                            break
                    else:
                        events.append((item.name, item.name.replace("-", " ").title()))
                except:
                    events.append((item.name, item.name.replace("-", " ").title()))
            else:
                events.append((item.name, item.name.replace("-", " ").title()))
    
    return sorted(events)


def main():
    docs_dir = Path("documents")
    output_base = Path(OUTPUT_DIR)
    output_base.mkdir(exist_ok=True)
    
    console.print("\n[bold cyan]FIA F1 PDF Processor[/bold cyan]")
    console.print("[dim]Hybrid extraction with PyMuPDF + pdfplumber[/dim]\n")
    
    # Resolve years
    available_years = get_available_years(docs_dir)
    years_to_process = available_years if "all" in [str(y).lower() for y in YEARS] else YEARS
    
    console.print(f"[yellow]Years to process:[/yellow] {', '.join(map(str, years_to_process))}")
    
    all_results = {}
    total_processed = 0
    total_failed = 0
    
    for year in years_to_process:
        if year not in available_years:
            console.print(f"[red]✗[/red] Year {year} not found, skipping")
            continue
        
        # Resolve events
        available_events = get_available_events(docs_dir, year)
        events_to_process = available_events if "all" in [e.lower() for e in EVENTS] else [
            (slug, name) for slug, name in available_events if slug in EVENTS
        ]
        
        if not events_to_process:
            console.print(f"[yellow]![/yellow] No events found for {year}")
            continue
        
        console.print(f"\n[bold yellow]Processing {year}[/bold yellow]")
        
        for event_slug, event_name in events_to_process:
            event_dir = docs_dir / str(year) / event_slug
            pdf_files = list(event_dir.glob("*.pdf"))
            
            if not pdf_files:
                console.print(f"  [dim]No PDFs in {event_name}[/dim]")
                continue
            
            console.print(f"\n[bold]{event_name}[/bold] ({len(pdf_files)} PDFs)")
            
            output_event_dir = output_base / str(year) / event_slug
            output_event_dir.mkdir(parents=True, exist_ok=True)
            
            event_results = []
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                
                if MODE == "interactive":
                    for pdf_file in pdf_files:
                        if not Confirm.ask(f"  Process {pdf_file.name}?"):
                            continue
                        
                        console.print(f"\n[bold]Processing:[/bold] {pdf_file.name}")
                        result = process_pdf(pdf_file, output_event_dir)
                        event_results.append(result)
                        
                        if result["success"]:
                            total_processed += 1
                        else:
                            total_failed += 1
                
                else:  # batch mode
                    task = progress.add_task(f"Processing {event_name}...", total=len(pdf_files))
                    
                    for pdf_file in pdf_files:
                        progress.update(task, description=f"Processing {pdf_file.name}")
                        result = process_pdf(pdf_file, output_event_dir)
                        event_results.append(result)
                        
                        if result["success"]:
                            total_processed += 1
                        else:
                            total_failed += 1
                        
                        progress.advance(task)
            
            # Create event index
            create_index_json(output_event_dir, event_results)
            all_results[(year, event_slug)] = event_results
    
    # Create global manifest
    if all_results:
        create_manifest_json(output_base, all_results)
    
    # Summary
    console.print("\n[bold green]Processing Complete![/bold green]")
    
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Count", justify="right")
    
    table.add_row("Successfully Processed", str(total_processed))
    table.add_row("Failed", str(total_failed))
    table.add_row("Total", str(total_processed + total_failed))
    
    console.print(table)
    console.print(f"\n[cyan]Output directory:[/cyan] {output_base.absolute()}")


if __name__ == "__main__":
    main()
