import duckdb
import yaml
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ENGINE")

class CloudBillHunter:
    # UPDATED: Accept db_path for testing
    def __init__(self, config_path='config.yaml', db_path=None):
        self.root_dir = os.path.dirname(os.path.abspath(__file__))
        config_full_path = os.path.join(self.root_dir, '..', config_path)
        
        with open(config_full_path, 'r') as f:
            self.config = yaml.safe_load(f)
            
        # If no path provided, use the default from config/hardcoded
        if db_path is None:
             db_path = os.path.join(self.root_dir, '..', 'data/warehouse.duckdb')
        
        self.db_path = db_path
        self.con = duckdb.connect(database=self.db_path) 

    def _read_sql(self, model_name):
        path = os.path.join(self.root_dir, 'sql/models', f"{model_name}.sql")
        with open(path, 'r') as f:
            return f.read()

    def close(self):
        """Closes the database connection to release the lock"""
        self.con.close()
        logger.info("🔒 Database connection closed.")

    def ingest_data(self, csv_path):
        """BRONZE LAYER: Raw Ingestion"""
        logger.info("🏗️  Building BRONZE layer...")
        # Normalize path for DuckDB
        csv_path = os.path.normpath(csv_path).replace('\\', '/')
        
        # FIX: The previous logic doubled the data on fresh runs.
        # "CREATE TABLE AS SELECT" inserts data. Then "INSERT" inserted it again.
        # We add "WHERE 1=0" to CREATE TABLE so it only creates the structure (empty).
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze_billing AS SELECT * FROM read_csv_auto('{csv_path}') WHERE 1=0;
            INSERT INTO bronze_billing SELECT * FROM read_csv_auto('{csv_path}');
        """)

    def run_pipeline(self):
        """Orchestrates the Silver and Gold transformations"""
        
        # --- SILVER LAYER ---
        logger.info("🥈 Building SILVER layer...")
        sql_fact = self._read_sql('silver_fact_usage')
        self.con.execute(f"CREATE OR REPLACE TABLE silver_fact_usage AS {sql_fact}")
        
        sql_dim = self._read_sql('silver_dim_resource')
        self.con.execute(f"CREATE OR REPLACE TABLE silver_dim_resource AS {sql_dim}")
        
        # --- GOLD LAYER ---
        logger.info("🥇 Building GOLD layer...")
        sql_gold = self._read_sql('gold_zombie_report')
        self.con.execute(f"CREATE OR REPLACE TABLE gold_zombie_report AS {sql_gold}")
        
        logger.info(f"✅ Data Refresh Complete.")