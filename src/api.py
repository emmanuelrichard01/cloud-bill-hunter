from fastapi import FastAPI, UploadFile, File, HTTPException
import shutil
import os
import duckdb
import logging
from src.analyze_costs import CloudBillHunter  # Import our Engine

# Initialize API and Logger
app = FastAPI(
    title="Cloud Bill Hunter API", 
    version="2.2.0",
    description="Enterprise FinOps Platform API for Ingestion and Querying"
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

# Setup temporary storage
UPLOAD_DIR = "data/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
WAREHOUSE_PATH = "data/warehouse.duckdb"

@app.get("/")
def health_check():
    """Simple ping to check if service is alive"""
    return {"status": "online", "service": "Cloud Bill Hunter", "version": "2.2.0"}

# --- 1. THE INGESTION ENDPOINT (Write) ---
@app.post("/analyze/upload")
async def analyze_upload(file: UploadFile = File(...)):
    """
    Ingest a billing CSV. 
    1. Saves file.
    2. Triggers Medallion Pipeline (Bronze -> Silver -> Gold).
    3. Updates Warehouse.
    4. Returns Analysis.
    """
    try:
        # 1. Save the file locally
        file_location = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        logger.info(f"📥 Received file via API: {file.filename}")

        # 2. Trigger the Data Engine
        engine = CloudBillHunter()
        try:
            # Run the full pipeline (Same logic as the Watcher!)
            engine.ingest_data(file_location)   # Bronze
            engine.run_pipeline()               # Silver -> Gold
            
            # 3. Query the result immediately to return to the user
            # We query the Gold table we just updated
            zombie_df = engine.con.execute("SELECT * FROM gold_zombie_report").df()
            report = zombie_df.to_dict(orient="records")
            total_waste = sum(item['total_wasted_cost'] for item in report)
            
            return {
                "status": "success",
                "message": "File ingested and Warehouse updated.",
                "zombies_found": len(report),
                "total_wasted_cost": total_waste,
                "details": report
            }
        finally:
            engine.close() # Always close DB connection!

    except Exception as e:
        logger.error(f"❌ Analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# --- 2. THE QUERY ENDPOINT (Read) ---
@app.get("/zombies")
def get_zombies():
    """
    Allow external tools (Slack, Jira, CI/CD) to fetch the 
    latest Zombie Report from the warehouse without uploading a file.
    """
    if not os.path.exists(WAREHOUSE_PATH):
        return {"status": "empty", "message": "No data in warehouse yet."}
        
    try:
        # Connect in READ_ONLY mode to avoid locking
        con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
        
        # Check if table exists
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        if 'gold_zombie_report' not in tables:
             con.close()
             return {"status": "empty", "message": "Pipeline has not run yet."}

        # Query Gold Layer
        df = con.execute("SELECT * FROM gold_zombie_report").df()
        con.close()
        
        return {
            "status": "success",
            "timestamp": "latest",
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))