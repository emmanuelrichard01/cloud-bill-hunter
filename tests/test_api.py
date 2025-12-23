import pytest
from fastapi.testclient import TestClient
import os
import sys
import io

# 1. SETUP: Override the Warehouse Path BEFORE importing the app
# This ensures the API uses a test DB, not production
TEST_DB = "test_warehouse.duckdb"
os.environ["WAREHOUSE_PATH"] = TEST_DB

# Ensure we can import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.api import app

client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def cleanup():
    """Runs after all tests to delete the test database"""
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    # Cleanup uploads if any
    if os.path.exists("data/uploads/test_bill.csv"):
        os.remove("data/uploads/test_bill.csv")

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "online"

def test_upload_and_query_flow():
    """
    FULL END-TO-END TEST:
    1. Upload CSV -> API Ingests -> Writes to Test DB
    2. Query GET /zombies -> API Reads -> Returns JSON
    """
    
    # --- 1. UPLOAD ---
    csv_content = """LineItem/ResourceId,LineItem/UsageStartDate,LineItem/ProductCode,LineItem/UsageAmount,LineItem/UnblendedCost,ResourceTags/user:Owner
i-api-zombie,2023-01-01,AmazonEC2,0.0,99.99,ApiTeam
"""
    file_obj = io.BytesIO(csv_content.encode('utf-8'))
    
    response = client.post(
        "/analyze/upload",
        files={"file": ("test_bill.csv", file_obj, "text/csv")}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["zombies_found"] == 1
    assert data["total_wasted_cost"] == 99.99

    # --- 2. QUERY (Persistence Check) ---
    # Does the GET endpoint see the data we just uploaded?
    response_q = client.get("/zombies")
    assert response_q.status_code == 200
    data_q = response_q.json()
    
    assert data_q["status"] == "success"
    assert data_q["count"] == 1
    assert data_q["data"][0]["resource_id"] == "i-api-zombie"