import duckdb
import threading
from django.conf import settings

_local = threading.local()

def get_duckdb_conn(read_only=True):
    if not hasattr(_local, 'conn') or _local.conn is None:
        db_path = settings.BASE_DIR / 'data' / 'fia_analytics.duckdb'
        _local.conn = duckdb.connect(str(db_path), read_only=read_only)
    return _local.conn

def close_duckdb_conn():
    if hasattr(_local, 'conn') and _local.conn:
        _local.conn.close()
        _local.conn = None
