# FIA F1 PDF Processor

High-quality PDF extraction tool for FIA Formula 1 decision documents using a hybrid approach (PyMuPDF + pdfplumber).

## Features

- **Hybrid Extraction**: Combines PyMuPDF for text/images and pdfplumber for tables
- **Auto-Orientation Detection**: Automatically detects and adjusts landscape pages
- **High-Quality Images**: Extracts embedded images at original resolution
- **Table Extraction**: Preserves table structure in markdown format
- **FIA Styling**: Generates HTML with official FIA branding colors
- **Batch & Interactive Modes**: Process all files at once or review each one
- **Structured Output**: Creates index.json and manifest.json following existing format

## Installation

```bash
# Install dependencies
uv sync

# Or manually install
uv pip install pymupdf pdfplumber pillow rich
```

## Configuration

Edit the constants at the top of `process_pdfs.py`:

```python
# Years to process (list of integers or ["all"])
YEARS = [2026]

# Events to process (list of event slugs or ["all"])
EVENTS = ["miami-grand-prix"]

# Processing mode: "batch" or "interactive"
MODE = "batch"

# Output directory
OUTPUT_DIR = "processed_documents"
```

## Usage

### Basic Usage

```bash
# Process configured years/events
uv run python process_pdfs.py
```

### Configuration Examples

```python
# Process all 2026 events
YEARS = [2026]
EVENTS = ["all"]

# Process multiple specific events
YEARS = [2026]
EVENTS = ["miami-grand-prix", "chinese-grand-prix", "japanese-grand-prix"]

# Process multiple years
YEARS = [2024, 2025, 2026]
EVENTS = ["all"]

# Interactive mode (confirm each file)
MODE = "interactive"
```

## Output Structure

```
processed_documents/
├── 2026/
│   ├── miami-grand-prix/
│   │   ├── decision-car-3-alleged-incident/
│   │   │   ├── decision-car-3-alleged-incident.html
│   │   │   ├── decision-car-3-alleged-incident.md
│   │   │   ├── metadata.json
│   │   │   └── images/
│   │   │       ├── page1_img0.png
│   │   │       └── page2_img0.png
│   │   ├── index.json
│   │   └── ...
│   └── ...
└── manifest.json
```

## Output Files

### Per Document
- **`{name}.html`**: Styled HTML with FIA branding
- **`{name}.md`**: Clean markdown text
- **`metadata.json`**: Processing metadata (pages, images, tables, fonts, orientations)
- **`images/`**: Extracted images at original quality

### Per Event
- **`index.json`**: List of all processed documents
  ```json
  [
    {"f": "decision.pdf", "t": "Decision", "n": 1, "p": "03.05.26 23:22"}
  ]
  ```

### Global
- **`manifest.json`**: Complete processing manifest
  ```json
  {
    "processed://2026/miami-grand-prix/decision.pdf": {
      "year": 2026,
      "event": "miami-grand-prix",
      "title": "Decision",
      "source": "processed_pdf",
      "path": "processed_documents/2026/miami-grand-prix/decision",
      "processed_at": "2026-05-05T11:47:00",
      "pages": 3,
      "images": 2,
      "tables": 1
    }
  }
  ```

## Features in Detail

### Auto-Orientation Detection
- Automatically detects portrait vs landscape pages
- Rotates landscape pages 90° for optimal text extraction
- Preserves original orientation metadata

### Image Extraction
- Extracts images at original resolution
- Supports PNG, JPEG, and other formats
- Preserves image quality without re-encoding

### Table Extraction
- Detects and extracts tables from PDFs
- Converts to markdown table format
- Preserves table structure and cell content

### HTML Generation
- FIA official colors (#E10600 red)
- Titillium Web font (FIA standard)
- Responsive design
- Embedded images and tables
- Processing metadata footer

## Troubleshooting

### Missing Dependencies
```bash
uv pip install pymupdf pdfplumber pillow rich
```

### Permission Errors
Ensure you have write access to the output directory:
```bash
mkdir -p processed_documents
chmod 755 processed_documents
```

### Memory Issues with Large PDFs
Process in smaller batches by limiting events:
```python
EVENTS = ["miami-grand-prix"]  # Process one at a time
```

## Advanced Usage

### Custom Output Directory
```python
OUTPUT_DIR = "/path/to/custom/output"
```

### Processing Specific Document Types
Filter PDFs before processing by modifying the glob pattern in the script:
```python
# Only process decision documents
pdf_files = list(event_dir.glob("decision-*.pdf"))

# Only process infringement documents
pdf_files = list(event_dir.glob("infringement-*.pdf"))
```

## Performance

- **Speed**: ~2-5 seconds per PDF (depends on size and complexity)
- **Memory**: ~100-500MB per PDF (depends on images)
- **Disk**: Original PDF size × 2-3 (includes images and HTML)

## Comparison with Other Tools

| Feature | This Tool | Marker Only | PyMuPDF Only |
|---------|-----------|-------------|--------------|
| Text Quality | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Image Quality | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Table Extraction | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| Layout Preservation | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| Speed | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

## License

Same as parent project.
