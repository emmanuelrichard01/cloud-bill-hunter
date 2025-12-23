import pytest
import duckdb
import pandas as pd
import os
import sys
import tempfile

# Ensure we can import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.analyze_costs import CloudBillHunter

@pytest.fixture
def temp_csv():
    """Creates a temporary CSV file with test data"""
    csv_content = """LineItem/ResourceId,LineItem/UsageStartDate,LineItem/ProductCode,LineItem/UsageAmount,LineItem/UnblendedCost,ResourceTags/user:Owner
i-zombie,2023-01-01,AmazonEC2,0.0,50.0,LegacyTeam
i-good,2023-01-01,AmazonEC2,10.0,10.0,DevTeam
"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write(csv_content)
        path = f.name
    yield path
    os.remove(path)

def test_medallion_pipeline(temp_csv):
    """
    INTEGRATION TEST:
    Verifies that ingest_data() + run_pipeline() correctly 
    transforms raw CSV -> Gold Layer Table.
    """
    
    # 1. Initialize Engine with IN-MEMORY database (Fast, Isolated)
    engine = CloudBillHunter(db_path=':memory:')
    
    try:
        # 2. Run Bronze Ingestion
        engine.ingest_data(temp_csv)
        
        # Verify Bronze exists
        tables = engine.con.execute("SHOW TABLES").fetchall()
        assert ('bronze_billing',) in tables
        
        # 3. Run Silver/Gold Pipeline
        engine.run_pipeline()
        
        # 4. Assert Gold Layer Logic
        result = engine.con.execute("SELECT * FROM gold_zombie_report").df()
        
        # Should catch 'i-zombie' (Cost > 0, Usage = 0)
        assert len(result) == 1
        assert result.iloc[0]['resource_id'] == 'i-zombie'
        assert result.iloc[0]['total_wasted_cost'] == 50.0
        
        # Should NOT catch 'i-good' (Usage > 0)
        assert 'i-good' not in result['resource_id'].values
        
    finally:
        engine.close()