import re
from pathlib import Path

def parse_doc_path(manifest_key: str) -> dict:
    """
    Input:  'documents/2024/miami-grand-prix/doc_17_-_..._decision.pdf'
    Output: {'year': 2024, 'event': 'miami-grand-prix', 'pdf_name': 'doc_17...pdf'}
    """
    parts = Path(manifest_key).parts  # ('documents', '2024', 'miami-grand-prix', 'doc_17...pdf')
    return {
        "year": int(parts[1]),
        "event": parts[2],               # e.g. 'miami-grand-prix'
        "pdf_name": parts[3],
        "pdf_stem": Path(parts[3]).stem, # filename without .pdf
    }

def get_extracted_md_path(manifest_key: str, extracted_root: Path) -> Path:
    """Returns path to the .md file for a given manifest key."""
    info = parse_doc_path(manifest_key)
    return extracted_root / str(info["year"]) / info["event"] / (info["pdf_stem"] + ".md")

def get_extracted_json_path(manifest_key: str, extracted_root: Path) -> Path:
    """Returns path to the .json sidecar for a given manifest key."""
    info = parse_doc_path(manifest_key)
    return extracted_root / str(info["year"]) / info["event"] / (info["pdf_stem"] + ".json")
