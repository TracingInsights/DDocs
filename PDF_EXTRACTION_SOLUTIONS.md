# FIA F1 Decision Documents - PDF Extraction Solutions

## Overview
Extract text and images from FIA F1 decision document PDFs while preserving the exact format and styling of official documents.

## 🥇 Solution 1: Marker + Custom CSS (RECOMMENDED)
**Best for:** High-quality conversion with minimal manual work

### Installation
```bash
# Install marker (converts PDFs to markdown with layout preservation)
pip install marker-pdf

# Also install for image extraction
pip install pymupdf pillow
```

### Implementation Approach
- Marker converts PDFs to markdown while preserving layout
- Extract images separately with PyMuPDF
- Apply custom CSS to match FIA styling
- Generate HTML with exact fonts/colors

### Pros
- Excellent layout preservation
- Handles complex multi-column layouts
- Extracts images automatically
- Fast processing

### Cons
- May need CSS tweaking per document type
- Requires post-processing for perfect styling

---

## 🥈 Solution 2: PyMuPDF (fitz) + HTML Generation
**Best for:** Pixel-perfect reproduction with full control

```python
import fitz  # PyMuPDF
from pathlib import Path

def extract_pdf_with_styling(pdf_path: Path, output_dir: Path):
    """Extract text, images, fonts, and styling from FIA PDF"""
    doc = fitz.open(pdf_path)
    html_parts = []
    
    html_parts.append("""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
@import url('https://fonts.googleapis.com/css2?family=Titillium+Web:wght@400;600;700&display=swap');

body {
    font-family: 'Titillium Web', Arial, sans-serif;
    margin: 40px;
    background: white;
}

.fia-header {
    text-align: center;
    border-bottom: 3px solid #E10600;
    padding-bottom: 20px;
    margin-bottom: 30px;
}

.fia-logo {
    max-width: 200px;
}

.document-title {
    font-size: 24px;
    font-weight: 700;
    color: #000;
    margin: 20px 0;
}

.metadata {
    font-size: 12px;
    color: #666;
    margin: 10px 0;
}

.content-block {
    margin: 15px 0;
    line-height: 1.6;
}

.decision-box {
    border: 2px solid #E10600;
    padding: 20px;
    margin: 20px 0;
    background: #FFF5F5;
}

table {
    border-collapse: collapse;
    width: 100%;
    margin: 20px 0;
}

th, td {
    border: 1px solid #ddd;
    padding: 12px;
    text-align: left;
}

th {
    background-color: #E10600;
    color: white;
    font-weight: 600;
}
</style>
</head>
<body>""")

    images_dir = output_dir / "images"
    images_dir.mkdir(exist_ok=True)
    
    for page_num, page in enumerate(doc):
        # Extract text with formatting
        blocks = page.get_text("dict")["blocks"]
        
        for block in blocks:
            if block["type"] == 0:  # Text block
                for line in block.get("lines", []):
                    line_html = '<div class="content-block">'
                    for span in line.get("spans", []):
                        text = span["text"]
                        font = span["font"]
                        size = span["size"]
                        color = span["color"]
                        
                        # Convert color int to hex
                        color_hex = f"#{color:06x}" if isinstance(color, int) else "#000000"
                        
                        style = f'font-size: {size}px; color: {color_hex};'
                        if "Bold" in font:
                            style += ' font-weight: 700;'
                        if "Italic" in font:
                            style += ' font-style: italic;'
                        
                        line_html += f'<span style="{style}">{text}</span>'
                    line_html += '</div>'
                    html_parts.append(line_html)
                    
            elif block["type"] == 1:  # Image block
                img_index = block["number"]
                pix = page.get_pixmap(clip=block["bbox"])
                img_path = images_dir / f"page{page_num+1}_img{img_index}.png"
                pix.save(str(img_path))
                html_parts.append(f'<img src="images/{img_path.name}" style="max-width: 100%;" />')
    
    html_parts.append("</body></html>")
    
    output_html = output_dir / f"{pdf_path.stem}.html"
    output_html.write_text("\n".join(html_parts), encoding="utf-8")
    
    doc.close()
    return output_html
```

### Pros
- Pixel-perfect text extraction with exact fonts/sizes/colors
- Extracts embedded images
- Full control over HTML generation
- Can detect tables, headers, etc.

### Cons
- Requires manual CSS styling to match FIA docs
- More code to maintain

---

## 🥉 Solution 3: pdfplumber + Tailwind CSS
**Best for:** Modern web-first approach with responsive design

```python
import pdfplumber
from pathlib import Path

def extract_with_pdfplumber(pdf_path: Path):
    """Extract structured data from FIA PDFs"""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Extract text with layout
            text = page.extract_text(layout=True)
            
            # Extract tables
            tables = page.extract_tables()
            
            # Extract images
            images = page.images
            
            # Get character-level details
            chars = page.chars  # Font, size, position for each char
            
            # Detect visual elements
            lines = page.lines
            rects = page.rects
```

### Pros
- Excellent table extraction
- Character-level formatting data
- Good for structured documents
- Works well with Tailwind for styling

### Cons
- Less precise than PyMuPDF for complex layouts
- May struggle with multi-column layouts

---

## 🏆 Solution 4: PDF.js + Canvas Rendering (Web-Based)
**Best for:** Browser-based viewing with perfect fidelity

```javascript
// Render PDF exactly as it appears using PDF.js
import * as pdfjsLib from 'pdfjs-dist';

async function renderPDF(pdfUrl) {
    const pdf = await pdfjsLib.getDocument(pdfUrl).promise;
    
    for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {
        const page = await pdf.getPage(pageNum);
        const viewport = page.getViewport({ scale: 1.5 });
        
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        canvas.height = viewport.height;
        canvas.width = viewport.width;
        
        await page.render({
            canvasContext: context,
            viewport: viewport
        }).promise;
        
        // Extract text layer for searchability
        const textContent = await page.getTextContent();
        
        // Overlay transparent text for copy/paste
    }
}
```

### Pros
- Perfect visual fidelity (renders exactly like PDF)
- Searchable and selectable text
- No conversion needed
- Works in browser

### Cons
- Not true HTML/CSS
- Requires JavaScript
- Larger file sizes

---

## 🎯 Solution 5: Hybrid Approach (BEST OVERALL)
**Combine multiple tools for optimal results**

```python
import fitz  # PyMuPDF
import pdfplumber
from marker import convert_single_pdf
from pathlib import Path
import json

async def extract_fia_document(pdf_path: Path, output_dir: Path):
    """Multi-stage extraction:
    1. Marker for initial markdown conversion
    2. PyMuPDF for images and precise styling
    3. pdfplumber for tables
    4. Custom CSS for FIA branding
    """
    
    # Stage 1: Convert to markdown with Marker
    markdown_text, images, metadata = convert_single_pdf(
        str(pdf_path),
        output_dir=str(output_dir)
    )
    
    # Stage 2: Extract images with PyMuPDF (higher quality)
    doc = fitz.open(pdf_path)
    images_dir = output_dir / "images"
    images_dir.mkdir(exist_ok=True)
    
    for page_num, page in enumerate(doc):
        image_list = page.get_images()
        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            img_path = images_dir / f"page{page_num+1}_img{img_index}.{base_image['ext']}"
            img_path.write_bytes(image_bytes)
    
    # Stage 3: Extract tables with pdfplumber
    tables_data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if tables:
                tables_data.extend(tables)
    
    # Stage 4: Generate HTML with FIA styling
    html = generate_fia_styled_html(
        markdown_text=markdown_text,
        images=list(images_dir.glob("*")),
        tables=tables_data,
        metadata=extract_metadata(doc)
    )
    
    output_html = output_dir / f"{pdf_path.stem}.html"
    output_html.write_text(html, encoding="utf-8")
    
    doc.close()
    return output_html

def generate_fia_styled_html(markdown_text, images, tables, metadata):
    """Generate HTML with exact FIA styling"""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{metadata.get('title', 'FIA Document')}</title>
    <link rel="stylesheet" href="fia-styles.css">
</head>
<body>
    <div class="fia-document">
        <header class="fia-header">
            <img src="fia-logo.svg" alt="FIA" class="fia-logo">
            <h1>{metadata.get('title', '')}</h1>
            <div class="metadata">
                <span>Document: {metadata.get('doc_number', '')}</span>
                <span>Date: {metadata.get('date', '')}</span>
                <span>Event: {metadata.get('event', '')}</span>
            </div>
        </header>
        <main class="content">
            {markdown_text}
        </main>
    </div>
</body>
</html>"""
```

## Enhanced Features for FIA Documents

### Document Type Detection
```python
def detect_fia_document_type(pdf_path):
    """Detect document type for template selection"""
    # Decision documents vs. technical regulations vs. bulletins
    # Each needs different CSS templates
    pass
```

### Metadata Extraction Enhancement
```python
def extract_fia_metadata(doc):
    """Extract FIA-specific metadata"""
    return {
        'decision_number': extract_decision_number(text),
        'event': extract_event_name(text),
        'date': extract_decision_date(text),
        'classification': extract_classification(text),  # Stewards, Technical, etc.
        'competitors': extract_competitors(text),
        'articles': extract_regulation_references(text)
    }
```

### Quality Assurance Layer
```python
def validate_conversion(original_pdf, generated_html):
    """Compare visual similarity and content completeness"""
    # Use image comparison for layout validation
    # Text diff for content accuracy
    pass
```

## Technical Considerations

### Styling Preservation
- Extract font families, sizes, weights from PDF metadata
- Preserve color schemes (FIA branding colors)
- Maintain spacing, margins, and layout proportions
- Handle special FIA document elements (letterheads, signatures, stamps)

### Content Structure
- Decision number and classification
- Date and event information
- Regulatory references
- Evidence sections
- Decision rationale
- Penalties/outcomes
- Signatures and official stamps

### Output Formats
1. **HTML + CSS**: Web-friendly, searchable, responsive
2. **Markdown + Assets**: Version control friendly
3. **JSON + Metadata**: Structured data for analysis
4. **LaTeX**: For high-quality PDF regeneration

## Recommended Tech Stack
- **Python**: `pymupdf`, `pdfplumber`, `beautifulsoup4`
- **Image Processing**: `Pillow`, `opencv-python`
- **Web**: `jinja2` for templating
- **CLI Tools**: `qpdf` for PDF manipulation, `imagemagick` for image processing

## Workflow Strategy
1. **Batch Processing**: Handle multiple years/events systematically
2. **Template Detection**: Identify different FIA document types and create type-specific processors
3. **Quality Assurance**: Visual comparison tools to ensure accuracy
4. **Metadata Extraction**: Parse decision numbers, dates, classifications for indexing

## Special Considerations for FIA Documents
- **Watermarks and Official Stamps**: Need special treatment to maintain document authenticity
- **Multi-language Support**: Some documents may contain multiple languages
- **Legal Compliance**: Maintain exact formatting for regulatory/legal content
- **Version Control**: Track changes and maintain document history

## Why Hybrid Approach Wins

### Advantages over Single Solutions:
- **Solution 1 (Marker only)**: Great start but needs the image quality boost
- **Solution 2 (PyMuPDF only)**: Too much manual CSS work for complex layouts  
- **Solution 3 (pdfplumber only)**: Struggles with FIA's multi-column formats
- **Solution 4 (PDF.js)**: Perfect fidelity but not true HTML conversion

The hybrid approach gets the best of all worlds while maintaining the official document integrity that's crucial for legal/regulatory content.