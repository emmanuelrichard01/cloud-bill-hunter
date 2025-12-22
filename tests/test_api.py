import pytest
from fastapi.testclient import TestClient
import pandas as pd
import io
import sys
import os

# Ensure we can import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.api import app

client = TestClient(app)

def test_health_check():
    """Does the API wake up?"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "online", "service": "Cloud Bill Hunter"}

def test_upload_analysis_flow():
    """INTEGRATION TEST: Upload a CSV -> Get JSON Analysis"""
    
    # 1. Create a tiny fake CSV in memory
    csv_content = """LineItem/ResourceId,LineItem/UsageStartDate,LineItem/ProductCode,LineItem/UsageAmount,LineItem/UnblendedCost,ResourceTags/user:Owner
i-zombie,2023-01-01,AmazonEC2,0.0,50.0,LegacyTeam
i-good,2023-01-01,AmazonEC2,10.0,10.0,DevTeam
"""
    # Convert string to bytes
    file_obj = io.BytesIO(csv_content.encode('utf-8'))
    
    # 2. Send POST request to the API
    response = client.post(
        "/analyze/upload",
        files={"file": ("test_bill.csv", file_obj, "text/csv")}
    )
    
    # 3. Verify Response Code
    assert response.status_code == 200, f"API failed with: {response.text}"
    
    # 4. Verify JSON Structure
    data = response.json()
    assert data["status"] == "success"
    assert data["zombies_found"] == 1
    assert data["total_wasted_cost"] == 50.0
    
    # Check details
    zombie = data["details"][0]
    assert zombie["resource_id"] == "i-zombie"