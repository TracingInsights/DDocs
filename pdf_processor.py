#!/usr/bin/env python3
"""
FIA F1 Decision Document PDF Processor

Processes FIA F1 decision document PDFs using a hybrid approach:
- Marker for initial markdown conversion
- PyMuPDF for high-quality image extraction and precise styling
- pdfplumber for table extraction
- Custom CSS for FIA branding

Usage:
    python pdf_processor.py
"""

import asyncio
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import datetime

# Core dependencies
try:
    import fitz  # PyMuPDF
    import pdfplumber
    from PIL import Image
    from jinja2 import Template
    from rich.console import Console
    from rich.prompt import Prompt, Confirm
    from rich.table import Table
    from rich.progress import Progress, TaskID
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: uv add pymupdf pdfplumber pillow jinja2 rich")
    exit(1)

# Optional marker dependency
try:
    from marker import convert_single_pdf
    MARKER_AVAILABLE = True
except ImportError:
    MARKER_AVAILABLE = False
    print("Warning: marker-pdf not available. Install with: uv add marker-pdf")

# Setup logging and console
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
console = Console()

class FIAPDFProcessor:
    """Main PDF processor class using hybrid approach"""
    
    def __init__(self, documents_dir: Path = Path("documents")):
        self.documents_dir = documents_dir
        self.processed_dir = Path("processed_documents")
        self.processed_dir.mkdir(exist_ok=True)
        
    def get_available_years(self) -> List[int]:
        """Get available years from documents directory"""
        years = []
        for item in self.documents_dir.iterdir():
            if item.is_dir() and item.name.isdigit():
                years.append(int(item.name))
        return sorted(years, reverse=True)
    
    def get_available_events(self, year: int) -> List[Tuple[str, str]]:
        """Get available events for a year (slug, name)"""
        year_dir = self.documents_dir / str(year)
        if not year_dir.exists():
            return []
        
        events = []
        for item in year_dir.iterdir():
            if item.is_dir():
                # Try to get proper name from index.json
                index_file = item / "index.json"
                if index_file.exists():
                    try:
                        with open(index_file) as f:
                            data = json.load(f)
                        # Get first document to extract event name from manifest
                        if data and len(data) > 0:
                            events.append((item.name, item.name.replace("-", " ").title()))
                    except:
                        events.append((item.name, item.name.replace("-", " ").title()))
                else:
                    events.append((item.name, item.name.replace("-", " ").title()))
        
        return sorted(events)
    
    def get_pdfs_in_event(self, year: int, event_slug: str) -> List[Path]:
        """Get all PDF files in an event directory"""
        event_dir = self.documents_dir / str(year) / event_slug
        if not event_dir.exists():
            return []
        
        pdfs = []
        for item in event_dir.rglob("*.pdf"):
            pdfs.append(item)
        
        return sorted(pdfs)
    
    def detect_document_type(self, pdf_path: Path) -> str:
        """Detect FIA document type from filename/content"""
        name = pdf_path.name.lower()
        
        if "decision" in name:
            return "decision"
        elif "summons" in name:
            return "summons"
        elif "infringement" in name:
            return "infringement"
        elif "classification" in name:
            return "classification"
        elif "scrutineering" in name:
            return "scrutineering"
        elif "timetable" in name:
            return "timetable"
        elif "entry-list" in name:
            return "entry_list"
        elif "procedure" in name:
            return "procedure"
        elif "notes" in name:
            return "notes"
        else:
            return "document"
    
    def extract_fia_metadata(self, pdf_path: Path, doc: fitz.Document) -> Dict:
        """Extract FIA-specific metadata from PDF"""
        metadata = {
            'filename': pdf_path.name,
            'document_type': self.detect_document_type(pdf_path),
            'title': pdf_path.stem.replace('-', ' ').title(),
            'pages': doc.page_count,
            'file_size': pdf_path.stat().st_size,
            'created': datetime.datetime.fromtimestamp(pdf_path.stat().st_ctime).isoformat(),
        }
        
        # Try to extract more metadata from first page
        if doc.page_count > 0:
            first_page = doc[0]
            text = first_page.get_text()
            
            # Look for decision numbers
            decision_match = re.search(r'(?:Decision|Doc\.?)\s*(?:No\.?)?\s*(\d+)', text, re.IGNORECASE)
            if decision_match:
                metadata['decision_number'] = decision_match.group(1)
            
            # Look for dates
            date_patterns = [
                r'(\d{1,2}[./]\d{1,2}[./]\d{4})',
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{1,2}\s+\w+\s+\d{4})'
            ]
            for pattern in date_patterns:
                date_match = re.search(pattern, text)
                if date_match:
                    metadata['document_date'] = date_match.group(1)
                    break
            
            # Look for competitors/cars
            car_matches = re.findall(r'Car\s+(\d+)', text, re.IGNORECASE)
            if car_matches:
                metadata['cars'] = list(set(car_matches))
        
        return metadata
    
    def extract_with_pymupdf(self, pdf_path: Path, output_dir: Path) -> Dict:
        """Extract content using PyMuPDF with precise styling"""
        doc = fitz.open(pdf_path)
        metadata = self.extract_fia_metadata(pdf_path, doc)
        
        images_dir = output_dir / "images"
        images_dir.mkdir(exist_ok=True)
        
        html_parts = []
        
        # Add HTML header with FIA styling
        html_parts.append(self.get_fia_html_header(metadata))
        
        for page_num, page in enumerate(doc):
            # Handle mixed orientation pages
            rect = page.rect
            is_landscape = rect.width > rect.height
            
            page_class = "landscape-page" if is_landscape else "portrait-page"
            html_parts.append(f'<div class="page {page_class}" data-page="{page_num + 1}">')
            
            # Extract text with formatting
            blocks = page.get_text("dict")["blocks"]
            
            for block in blocks:
                if block["type"] == 0:  # Text block
                    html_parts.append(self.process_text_block(block))
                elif block["type"] == 1:  # Image block
                    img_path = self.extract_image(page, block, images_dir, page_num)
                    if img_path:
                        html_parts.append(f'<img src="images/{img_path.name}" class="document-image" />')
            
            html_parts.append('</div>')
        
        html_parts.append("</body></html>")
        doc.close()
        
        return {
            'html': "\n".join(html_parts),
            'metadata': metadata,
            'images': list(images_dir.glob("*")) if images_dir.exists() else []
        }
    
    def extract_with_pdfplumber(self, pdf_path: Path) -> Dict:
        """Extract tables and structured data using pdfplumber"""
        tables_data = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Extract tables
                tables = page.extract_tables()
                for table_num, table in enumerate(tables):
                    if table and len(table) > 1:  # Skip empty or single-row tables
                        tables_data.append({
                            'page': page_num + 1,
                            'table_id': f"table_{page_num}_{table_num}",
                            'data': table,
                            'html': self.table_to_html(table)
                        })
        
        return {'tables': tables_data}
    
    def extract_with_marker(self, pdf_path: Path, output_dir: Path) -> Optional[Dict]:
        """Extract using Marker if available"""
        if not MARKER_AVAILABLE:
            return None
        
        try:
            markdown_text, images, metadata = convert_single_pdf(
                str(pdf_path),
                output_dir=str(output_dir)
            )
            return {
                'markdown': markdown_text,
                'images': images,
                'metadata': metadata
            }
        except Exception as e:
            log.warning(f"Marker extraction failed for {pdf_path}: {e}")
            return None
    
    def process_text_block(self, block: Dict) -> str:
        """Process a text block with formatting preservation"""
        html_parts = []
        
        for line in block.get("lines", []):
            line_html = '<div class="text-line">'
            
            for span in line.get("spans", []):
                text = span["text"].strip()
                if not text:
                    continue
                
                font = span["font"]
                size = span["size"]
                color = span["color"]
                
                # Convert color to hex
                color_hex = f"#{color:06x}" if isinstance(color, int) else "#000000"
                
                # Build style
                style_parts = [f'font-size: {size}px', f'color: {color_hex}']
                
                if "Bold" in font or "bold" in font.lower():
                    style_parts.append('font-weight: 700')
                if "Italic" in font or "italic" in font.lower():
                    style_parts.append('font-style: italic')
                
                style = '; '.join(style_parts)
                line_html += f'<span style="{style}">{self.escape_html(text)}</span>'
            
            line_html += '</div>'
            html_parts.append(line_html)
        
        return '\n'.join(html_parts)
    
    def extract_image(self, page: fitz.Page, block: Dict, images_dir: Path, page_num: int) -> Optional[Path]:
        """Extract image from page"""
        try:
            img_index = block["number"]
            pix = page.get_pixmap(clip=block["bbox"])
            img_path = images_dir / f"page{page_num+1}_img{img_index}.png"
            pix.save(str(img_path))
            return img_path
        except Exception as e:
            log.warning(f"Failed to extract image: {e}")
            return None
    
    def table_to_html(self, table: List[List[str]]) -> str:
        """Convert table data to HTML"""
        if not table:
            return ""
        
        html = ['<table class="fia-table">']
        
        # First row as header
        if table:
            html.append('<thead><tr>')
            for cell in table[0]:
                html.append(f'<th>{self.escape_html(str(cell or ""))}</th>')
            html.append('</tr></thead>')
        
        # Remaining rows as body
        if len(table) > 1:
            html.append('<tbody>')
            for row in table[1:]:
                html.append('<tr>')
                for cell in row:
                    html.append(f'<td>{self.escape_html(str(cell or ""))}</td>')
                html.append('</tr>')
            html.append('</tbody>')
        
        html.append('</table>')
        return '\n'.join(html)
    
    def escape_html(self, text: str) -> str:
        """Escape HTML special characters"""
        return (text.replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;')
                   .replace('"', '&quot;')
                   .replace("'", '&#x27;'))
    
    def get_fia_html_header(self, metadata: Dict) -> str:
        """Generate HTML header with FIA styling"""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{metadata.get('title', 'FIA Document')}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Titillium+Web:wght@400;600;700&display=swap');
        
        body {{
            font-family: 'Titillium Web', Arial, sans-serif;
            margin: 0;
            padding: 40px;
            background: white;
            color: #000;
            line-height: 1.6;
        }}
        
        .fia-header {{
            text-align: center;
            border-bottom: 3px solid #E10600;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        
        .fia-logo {{
            max-width: 200px;
            margin-bottom: 20px;
        }}
        
        .document-title {{
            font-size: 28px;
            font-weight: 700;
            color: #000;
            margin: 20px 0;
        }}
        
        .document-metadata {{
            font-size: 14px;
            color: #666;
            margin: 10px 0;
        }}
        
        .page {{
            margin: 30px 0;
            padding: 20px;
            border: 1px solid #ddd;
            background: white;
        }}
        
        .landscape-page {{
            max-width: 100%;
            overflow-x: auto;
        }}
        
        .portrait-page {{
            max-width: 800px;
            margin: 30px auto;
        }}
        
        .text-line {{
            margin: 5px 0;
        }}
        
        .document-image {{
            max-width: 100%;
            height: auto;
            margin: 20px 0;
            border: 1px solid #ddd;
        }}
        
        .fia-table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
            font-size: 14px;
        }}
        
        .fia-table th,
        .fia-table td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        
        .fia-table th {{
            background-color: #E10600;
            color: white;
            font-weight: 600;
        }}
        
        .fia-table tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        
        .decision-box {{
            border: 2px solid #E10600;
            padding: 20px;
            margin: 20px 0;
            background: #FFF5F5;
        }}
        
        .metadata-box {{
            background: #f5f5f5;
            padding: 15px;
            margin: 20px 0;
            border-left: 4px solid #E10600;
        }}
        
        @media print {{
            body {{ margin: 0; padding: 20px; }}
            .page {{ break-after: page; }}
        }}
    </style>
</head>
<body>
    <div class="fia-header">
        <div class="document-title">{metadata.get('title', 'FIA Document')}</div>
        <div class="document-metadata">
            <div>Document Type: {metadata.get('document_type', 'Unknown').title()}</div>
            <div>Pages: {metadata.get('pages', 'Unknown')}</div>
            <div>File: {metadata.get('filename', 'Unknown')}</div>
        </div>
    </div>
"""
    
    def hybrid_process_pdf(self, pdf_path: Path, output_dir: Path) -> Dict:
        """Process PDF using hybrid approach"""
        console.print(f"[blue]Processing: {pdf_path.name}[/blue]")
        
        # Stage 1: PyMuPDF for precise extraction
        pymupdf_result = self.extract_with_pymupdf(pdf_path, output_dir)
        
        # Stage 2: pdfplumber for tables
        pdfplumber_result = self.extract_with_pdfplumber(pdf_path)
        
        # Stage 3: Marker (if available)
        marker_result = self.extract_with_marker(pdf_path, output_dir)
        
        # Combine results
        final_html = pymupdf_result['html']
        
        # Add tables if found
        if pdfplumber_result['tables']:
            tables_html = '<div class="extracted-tables">'
            tables_html += '<h2>Extracted Tables</h2>'
            for table in pdfplumber_result['tables']:
                tables_html += f'<h3>Page {table["page"]} - Table {table["table_id"]}</h3>'
                tables_html += table['html']
            tables_html += '</div>'
            
            # Insert before closing body tag
            final_html = final_html.replace('</body>', f'{tables_html}</body>')
        
        # Save HTML
        html_path = output_dir / f"{pdf_path.stem}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(final_html)
        
        # Save metadata
        metadata_path = output_dir / f"{pdf_path.stem}_metadata.json"
        combined_metadata = {
            **pymupdf_result['metadata'],
            'tables_count': len(pdfplumber_result['tables']),
            'images_count': len(pymupdf_result['images']),
            'marker_available': marker_result is not None,
            'processing_timestamp': datetime.datetime.now().isoformat()
        }
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(combined_metadata, f, indent=2)
        
        return {
            'html_path': html_path,
            'metadata_path': metadata_path,
            'metadata': combined_metadata,
            'images': pymupdf_result['images'],
            'tables': pdfplumber_result['tables']
        }
    
    def create_index_files(self, output_dir: Path, processed_files: List[Dict]):
        """Create index.json and manifest.json files"""
        
        # Create index.json (compact format)
        index_data = []
        for file_info in processed_files:
            index_data.append({
                'f': file_info['metadata']['filename'],
                't': file_info['metadata']['title'],
                'type': file_info['metadata']['document_type'],
                'pages': file_info['metadata']['pages'],
                'size': file_info['metadata']['file_size']
            })
        
        index_path = output_dir / "index.json"
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, separators=(',', ':'))
        
        # Create manifest.json (detailed format)
        manifest_data = {}
        for file_info in processed_files:
            key = file_info['metadata']['filename']
            manifest_data[key] = file_info['metadata']
        
        manifest_path = output_dir / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest_data, f, indent=2)
        
        console.print(f"[green]Created index files: {index_path} and {manifest_path}[/green]")
    
    def interactive_selection(self):
        """Interactive year and event selection"""
        console.print("[bold blue]FIA F1 PDF Document Processor[/bold blue]")
        console.print("Select year and event to process PDFs\n")
        
        # Get available years
        years = self.get_available_years()
        if not years:
            console.print("[red]No years found in documents directory[/red]")
            return None, None
        
        # Display years table
        years_table = Table(title="Available Years")
        years_table.add_column("Index", style="cyan")
        years_table.add_column("Year", style="green")
        
        for i, year in enumerate(years):
            years_table.add_row(str(i + 1), str(year))
        
        console.print(years_table)
        
        # Select year
        while True:
            try:
                year_choice = Prompt.ask("Select year (number)", default="1")
                year_idx = int(year_choice) - 1
                if 0 <= year_idx < len(years):
                    selected_year = years[year_idx]
                    break
                else:
                    console.print("[red]Invalid selection[/red]")
            except ValueError:
                console.print("[red]Please enter a number[/red]")
        
        # Get available events for selected year
        events = self.get_available_events(selected_year)
        if not events:
            console.print(f"[red]No events found for year {selected_year}[/red]")
            return None, None
        
        # Display events table
        events_table = Table(title=f"Available Events for {selected_year}")
        events_table.add_column("Index", style="cyan")
        events_table.add_column("Event", style="green")
        events_table.add_column("Slug", style="yellow")
        
        for i, (slug, name) in enumerate(events):
            events_table.add_row(str(i + 1), name, slug)
        
        console.print(events_table)
        
        # Select event
        while True:
            try:
                event_choice = Prompt.ask("Select event (number)", default="1")
                event_idx = int(event_choice) - 1
                if 0 <= event_idx < len(events):
                    selected_event = events[event_idx][0]  # slug
                    break
                else:
                    console.print("[red]Invalid selection[/red]")
            except ValueError:
                console.print("[red]Please enter a number[/red]")
        
        return selected_year, selected_event
    
    def run(self):
        """Main execution function"""
        # Interactive selection
        year, event_slug = self.interactive_selection()
        if not year or not event_slug:
            return
        
        # Get PDFs to process
        pdfs = self.get_pdfs_in_event(year, event_slug)
        if not pdfs:
            console.print(f"[red]No PDFs found for {year} {event_slug}[/red]")
            return
        
        console.print(f"\n[green]Found {len(pdfs)} PDFs to process[/green]")
        
        # Show PDFs and get confirmation
        pdfs_table = Table(title=f"PDFs in {year} {event_slug}")
        pdfs_table.add_column("Index", style="cyan")
        pdfs_table.add_column("Filename", style="green")
        pdfs_table.add_column("Size", style="yellow")
        
        for i, pdf_path in enumerate(pdfs):
            size_mb = pdf_path.stat().st_size / (1024 * 1024)
            pdfs_table.add_row(str(i + 1), pdf_path.name, f"{size_mb:.1f} MB")
        
        console.print(pdfs_table)
        
        if not Confirm.ask(f"\nProcess all {len(pdfs)} PDFs?"):
            return
        
        # Create output directory
        output_dir = self.processed_dir / str(year) / event_slug
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process PDFs with progress bar
        processed_files = []
        
        with Progress() as progress:
            task = progress.add_task("[green]Processing PDFs...", total=len(pdfs))
            
            for pdf_path in pdfs:
                try:
                    result = self.hybrid_process_pdf(pdf_path, output_dir)
                    processed_files.append(result)
                    console.print(f"[green]✓[/green] {pdf_path.name}")
                except Exception as e:
                    console.print(f"[red]✗[/red] {pdf_path.name}: {e}")
                    log.error(f"Failed to process {pdf_path}: {e}")
                
                progress.advance(task)
        
        # Create index files
        if processed_files:
            self.create_index_files(output_dir, processed_files)
            
            # Create year-level index
            year_output_dir = self.processed_dir / str(year)
            year_events = [event_slug]  # Could be extended to include all processed events
            year_index = [{"s": event_slug, "n": event_slug.replace("-", " ").title()}]
            
            year_index_path = year_output_dir / "index.json"
            with open(year_index_path, 'w', encoding='utf-8') as f:
                json.dump(year_index, f, separators=(',', ':'))
        
        console.print(f"\n[bold green]Processing complete![/bold green]")
        console.print(f"Output directory: {output_dir}")
        console.print(f"Processed {len(processed_files)} PDFs successfully")


def main():
    """Entry point"""
    processor = FIAPDFProcessor()
    processor.run()


if __name__ == "__main__":
    main()