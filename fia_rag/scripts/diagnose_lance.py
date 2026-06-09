import lancedb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LANCEDB_PATH = BASE_DIR / "fia_rag" / "data" / "lancedb"

db = lancedb.connect(str(LANCEDB_PATH))
table = db.open_table("chunks")

# Count via lance dataset (reads all fragments, ignores to_arrow limit)
ds = table.to_lance()
lance_count = ds.count_rows()
print(f"count_rows via lance dataset : {lance_count}")

# Count fragments
fragments = list(ds.get_fragments())
print(f"Number of Lance fragments    : {len(fragments)}")

# Count via to_arrow (the buggy path)
arr = table.to_arrow()
print(f"count_rows via to_arrow()    : {len(arr)}")

print()
if lance_count != len(arr):
    print("*** MISMATCH: to_arrow() is NOT reading all fragments! ***")
    print(f"    Missing rows: {lance_count - len(arr)}")
else:
    print("OK: both methods agree.")
