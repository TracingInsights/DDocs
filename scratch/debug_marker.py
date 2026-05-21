import sys
from marker.convert import Converter
from marker.models import load_all_models
from marker.output import save_markdown
import os

def check_markdown(pdf_path):
    print(f"Loading models and converting {pdf_path}...")
    # Using a simpler version of the converter call to see what it produces
    # In the actual script it's: converter = Converter()
    from marker.convert import Converter
    converter = Converter()
    
    result = converter(pdf_path)
    # The result is typically a tuple or an object depending on the version
    # In fia_pdf_processor.py it's: full_markdown = converter(str(pdf_path))
    
    with open("debug_output.md", "w", encoding="utf-8") as f:
        f.write(str(result))
    
    print("Done. Saved to debug_output.md")

if __name__ == "__main__":
    pdf = "/Ubuntu/home/devcontainers/uGithub/DDocs/documents/2026/miami-grand-prix/car-presentation-submissions.pdf"
    if os.path.exists(pdf):
        check_markdown(pdf)
    else:
        print(f"File not found: {pdf}")
