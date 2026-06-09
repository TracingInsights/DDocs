from django.core.management.base import BaseCommand
from core.services.duckdb import get_duckdb_conn

class Command(BaseCommand):
    help = 'Initializes the DuckDB schema for the FIA RAG system.'

    def handle(self, *args, **options):
        conn = get_duckdb_conn(read_only=False)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id VARCHAR PRIMARY KEY, manifest_key VARCHAR NOT NULL, md_path VARCHAR NOT NULL,
            year INTEGER, event VARCHAR, pdf_stem VARCHAR, doc_type VARCHAR, pages INTEGER,
            tables_extracted INTEGER, extracted_at TIMESTAMP, driver_name VARCHAR, car_number VARCHAR,
            team VARCHAR, charge_description TEXT, charge_normalized VARCHAR, regulation_articles VARCHAR[],
            heard BOOLEAN, decision VARCHAR, penalty_type VARCHAR, penalty_value VARCHAR,
            penalty_unit VARCHAR, stewards VARCHAR[], ingested_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS chunks (
            id VARCHAR PRIMARY KEY, doc_id VARCHAR NOT NULL REFERENCES documents(id),
            chunk_index INTEGER, header_path VARCHAR, chunk_text TEXT, bm25_id INTEGER
        );
        """)
        self.stdout.write(self.style.SUCCESS('Successfully initialized DuckDB schema.'))
