import requests
import time
import os
import json

# Configuration
API_URL = "http://localhost:8000"
SAMPLE_FILE = "data/raw/aws_billing_data.csv"

def print_header(title):
    print(f"\n{'='*60}")
    print(f"🤖 SIMULATION: {title}")
    print(f"{'='*60}")

def simulate_cicd_pipeline():
    """
    Scenario: A GitHub Action pipeline finishes a deployment.
    It uploads the latest bill to check if the new deployment created waste.
    """
    print_header("CI/CD PIPELINE TRIGGER")
    print(f"📡 Uploading {SAMPLE_FILE} to API...")
    
    if not os.path.exists(SAMPLE_FILE):
        print("❌ Error: Generate data first! (Run 'make data')")
        return

    try:
        with open(SAMPLE_FILE, 'rb') as f:
            start_time = time.time()
            
            # --- FIX IS HERE ---
            # We must send only the filename (aws_billing_data.csv), not the path.
            files = {'file': (os.path.basename(SAMPLE_FILE), f, 'text/csv')}
            
            response = requests.post(
                f"{API_URL}/analyze/upload", 
                files=files
            )
            duration = time.time() - start_time
            
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Pipeline Success ({duration:.2f}s)")
            print(f"📊 Warehouse Updated. Zombies Found: {data['zombies_found']}")
            print(f"💰 Total Waste: ${data['total_wasted_cost']:,.2f}")
        else:
            print(f"❌ Pipeline Failed: {response.text}")
    except Exception as e:
        print(f"❌ Connection Error: {e}")

def simulate_slack_bot():
    """
    Scenario: A Slack Bot polls the API every morning to post a status update.
    It uses the GET /zombies endpoint (Read Only).
    """
    print_header("SLACK BOT POLL")
    print("🤖 Bot asking API: 'Are there any zombies right now?'")
    
    try:
        response = requests.get(f"{API_URL}/zombies")
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success':
                count = data['count']
                print(f"✅ Bot Received Data: {count} zombies active.")
                
                if count > 0:
                    print("🚨 Bot Action: Posting alert to #engineering-cost channel...")
                    print(f"   '⚠️ Alert: {count} idle resources detected!'")
                else:
                    print("💚 Bot Action: Posting 'All Systems Green'.")
            else:
                print(f"ℹ️ Bot Info: {data['message']}")
        else:
            print(f"❌ Bot Error: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Connection Error: {e}")

if __name__ == "__main__":
    # Ensure API is running
    print("Checking system status...")
    try:
        requests.get(API_URL)
    except:
        print("❌ API is offline. Run 'make up' first!")
        exit(1)

    # 1. Run Ingestion (Write)
    simulate_cicd_pipeline()
    
    # Pause for realism
    time.sleep(2)
    
    # 2. Run Query (Read)
    simulate_slack_bot()