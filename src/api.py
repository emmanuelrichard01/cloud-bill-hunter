from fastapi import FastAPI, UploadFile, File, HTTPException
import shutil
import os
import duckdb
import logging
from src.analyze_costs import CloudBillHunter

# Initialize API and Logger
app = FastAPI(title="Cloud Bill Hunter API", version="2.2.0")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

# Setup paths
UPLOAD_DIR = "data/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# UPDATED: Use Env Var for testing isolation
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "data/warehouse.duckdb")

@app.get("/")
def health_check():
    return {"status": "online", "service": "Cloud Bill Hunter", "version": "2.2.0"}

@app.post("/analyze/upload")
async def analyze_upload(file: UploadFile = File(...)):
    try:
        file_location = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        logger.info(f"📥 Received file: {file.filename}")

        # Inject the correct Warehouse Path
        engine = CloudBillHunter(db_path=WAREHOUSE_PATH)
        try:
            engine.ingest_data(file_location)
            engine.run_pipeline()
            
            zombie_df = engine.con.execute("SELECT * FROM gold_zombie_report").df()
            report = zombie_df.to_dict(orient="records")
            total_waste = sum(item['total_wasted_cost'] for item in report)
            
            return {
                "status": "success",
                "zombies_found": len(report),
                "total_wasted_cost": total_waste,
                "details": report
            }
        finally:
            engine.close()

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/zombies")
def get_zombies():
    if not os.path.exists(WAREHOUSE_PATH):
        return {"status": "empty", "message": "No data yet."}
        
    try:
        con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
        # Check tables safely
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        
        if 'gold_zombie_report' not in tables:
             con.close()
             return {"status": "empty", "message": "Pipeline has not run yet."}

        df = con.execute("SELECT * FROM gold_zombie_report").df()
        con.close()
        
        return {
            "status": "success",
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))