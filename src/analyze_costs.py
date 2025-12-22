import duckdb
import yaml
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ENGINE")

class CloudBillHunter:
    def __init__(self, config_path='config.yaml'):
        self.root_dir = os.path.dirname(os.path.abspath(__file__))
        config_full_path = os.path.join(self.root_dir, '..', config_path)
        
        with open(config_full_path, 'r') as f:
            self.config = yaml.safe_load(f)
            
        # PERSISTENT STORAGE
        db_path = os.path.join(self.root_dir, '..', 'data/warehouse.duckdb')
        self.con = duckdb.connect(database=db_path) 

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
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze_billing AS SELECT * FROM read_csv_auto('{csv_path}');
            INSERT INTO bronze_billing SELECT * FROM read_csv_auto('{csv_path}');
        """)

    def run_pipeline(self):
        """Orchestrates the Silver and Gold transformations"""
        # NOTE: We removed the try/finally block. 
        # The CALLER is now responsible for calling .close()
        
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