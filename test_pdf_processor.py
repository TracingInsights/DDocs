#!/usr/bin/env python3
"""
Simple test for PDF processor
"""

from pathlib import Path


def test_documents_structure():
    """Test if we can read the documents structure"""
    documents_dir = Path("documents")
    
    if not documents_dir.exists():
        print("Documents directory not found")
        return
    
    print("Available years:")
    years = []
    for item in documents_dir.iterdir():
        if item.is_dir() and item.name.isdigit():
            years.append(int(item.name))
    
    years = sorted(years, reverse=True)
    for year in years:
        print(f"  {year}")
    
    # Test 2026 Miami Grand Prix
    miami_dir = documents_dir / "2026" / "miami-grand-prix"
    if miami_dir.exists():
        print("\nMiami Grand Prix PDFs:")
        pdfs = list(miami_dir.glob("*.pdf"))
        print(f"Found {len(pdfs)} PDFs")
        for pdf in pdfs[:5]:  # Show first 5
            size_mb = pdf.stat().st_size / (1024 * 1024)
            print(f"  {pdf.name} ({size_mb:.1f} MB)")
        if len(pdfs) > 5:
            print(f"  ... and {len(pdfs) - 5} more")
    
    assert years

if __name__ == "__main__":
    test_documents_structure()
