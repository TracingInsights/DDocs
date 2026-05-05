import fitz
import json
from pathlib import Path

pdf_path = Path("documents/2026/miami-grand-prix/decision-car-14-alleged-yellow-flag-infringement.pdf")
doc = fitz.open(pdf_path)
page = doc[0]

# Get text with coordinates to understand layout
text_dict = page.get_text("dict")
with open("pdf_layout.json", "w") as f:
    json.dump(text_dict, f, indent=2)

print(f"Page size: {page.rect}")
doc.close()
