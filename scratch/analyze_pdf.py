import fitz # PyMuPDF
import sys

def analyze_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    page = doc[0] # Usually 1 page documents
    
    print(f"Page size: {page.rect}")
    
    # Text Analysis
    print("\n--- TEXT ELEMENTS ---")
    text_instances = page.get_text("dict")["blocks"]
    for block in text_instances:
        if "lines" in block:
            for line in block["lines"]:
                for span in line["spans"]:
                    print(f"'{span['text']}' at {span['bbox']} (font: {span['font']}, size: {span['size']})")
                    
    # Line/Drawing Analysis
    print("\n--- DRAWING ELEMENTS ---")
    drawings = page.get_drawings()
    for d in drawings:
        if d["type"] == "l": # line
            print(f"Line from {d['pt1']} to {d['pt2']} (width: {d['width']}, color: {d['color']})")
        elif d["type"] == "r": # rectangle
            print(f"Rect at {d['rect']} (width: {d['width']}, color: {d['color']})")
            
if __name__ == "__main__":
    analyze_pdf(sys.argv[1])
