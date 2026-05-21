# Quick Start - FIA PDF Processor

## Installation

```bash
# Install dependencies
uv sync
```

## Basic Usage

### 1. Edit Configuration (Top of `process_pdfs.py`)

```python
# Years to process
YEARS = [2026]

# Events to process  
EVENTS = ["miami-grand-prix"]

# Processing mode
MODE = "batch"  # or "interactive"

# Output directory
OUTPUT_DIR = "processed_documents"
```

### 2. Run the Processor

```bash
uv run python process_pdfs.py
```

## Configuration Examples

### Process All 2026 Events
```python
YEARS = [2026]
EVENTS = ["all"]
MODE = "batch"
```

### Process Multiple Specific Events
```python
YEARS = [2026]
EVENTS = ["miami-grand-prix", "chinese-grand-prix", "japanese-grand-prix"]
MODE = "batch"
```

### Process Multiple Years
```python
YEARS = [2024, 2025, 2026]
EVENTS = ["all"]
MODE = "batch"
```

### Interactive Mode (Confirm Each File)
```python
YEARS = [2026]
EVENTS = ["miami-grand-prix"]
MODE = "interactive"  # Will prompt for each PDF
```

## Output Structure

```
processed_documents/
├── 2026/
│   └── miami-grand-prix/
│       ├── championship-points/
│       │   ├── championship-points.html    # Styled HTML
│       │   ├── championship-points.md      # Clean markdown
│       │   ├── metadata.json               # Processing info
│       │   └── images/                     # Extracted images
│       │       ├── page1_img0.png
│       │       └── page2_img0.png
│       ├── index.json                      # Event index
│       └── ...
└── manifest.json                           # Global manifest
```

## What Gets Extracted

✅ **Text** - With formatting preserved (bold, headers)  
✅ **Images** - At original resolution  
✅ **Tables** - Converted to markdown format  
✅ **Metadata** - Pages, fonts, orientations  
✅ **Auto-rotation** - Landscape pages automatically adjusted

## Output Files

### Per Document
- **`.html`** - Styled with FIA branding (#E10600 red, Titillium Web font)
- **`.md`** - Clean markdown text
- **`metadata.json`** - Processing details
- **`images/`** - All extracted images

### Per Event
- **`index.json`** - List of all processed documents (same format as existing)

### Global
- **`manifest.json`** - Complete processing manifest

## Example: Process 2026 Miami Grand Prix

1. **Edit `process_pdfs.py`:**
   ```python
   YEARS = [2026]
   EVENTS = ["miami-grand-prix"]
   MODE = "batch"
   ```

2. **Run:**
   ```bash
   uv run python process_pdfs.py
   ```

3. **Result:**
   ```
   ✓ Successfully Processed: 93 PDFs
   ✓ Output: processed_documents/2026/miami-grand-prix/
   ```

## Viewing Results

### HTML Files
Open any `.html` file in a browser to see the styled document with FIA branding.

### Markdown Files
View `.md` files in any text editor or markdown viewer for clean text.

### Images
All images are in the `images/` subfolder of each document.

## Performance

- **Speed**: ~2-5 seconds per PDF
- **Memory**: ~100-500MB per PDF
- **Disk**: Original size × 2-3 (includes images and HTML)

## Troubleshooting

### "No PDFs found"
- Check that the year/event exists in `documents/` folder
- Verify event slug matches folder name (e.g., `miami-grand-prix`)

### Memory Issues
Process fewer events at once:
```python
EVENTS = ["miami-grand-prix"]  # One at a time
```

### Permission Errors
```bash
mkdir -p processed_documents
chmod 755 processed_documents
```

## Advanced: Filter by Document Type

Edit the script to process only specific document types:

```python
# In the main() function, replace:
pdf_files = list(event_dir.glob("*.pdf"))

# With:
pdf_files = list(event_dir.glob("decision-*.pdf"))  # Only decisions
# or
pdf_files = list(event_dir.glob("infringement-*.pdf"))  # Only infringements
```

## Next Steps

See [PDF_PROCESSOR_README.md](PDF_PROCESSOR_README.md) for complete documentation.
