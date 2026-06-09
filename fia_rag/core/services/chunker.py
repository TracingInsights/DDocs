import re

def chunk_markdown(doc_id: str, text: str, max_size=1200, overlap=200) -> list[dict]:
    """Simple header-aware chunker for FIA .md files."""
    sections = re.split(r'\n(?=#{1,4} )', text)
    chunks = []
    idx = 0
    for section in sections:
        header = ""
        m = re.match(r'^(#{1,4} .+)\n', section)
        if m:
            header = m.group(1).strip("# ").strip()
        if len(section) <= max_size:
            if section.strip():
                chunks.append({"doc_id": doc_id, "chunk_index": idx,
                               "header_path": header, "chunk_text": section.strip()})
                idx += 1
        else:
            # Sliding window on oversized sections
            start = 0
            while start < len(section):
                end = start + max_size
                chunks.append({"doc_id": doc_id, "chunk_index": idx,
                               "header_path": header,
                               "chunk_text": section[start:end].strip()})
                idx += 1
                start += max_size - overlap
    return chunks
