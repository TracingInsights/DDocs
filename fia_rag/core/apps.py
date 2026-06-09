from django.apps import AppConfig

class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'core'
    
    def ready(self):
        import atexit
        try:
            from core.services.duckdb import close_duckdb_conn
            atexit.register(close_duckdb_conn)
        except ImportError:
            pass  # If DuckDB service not built yet
