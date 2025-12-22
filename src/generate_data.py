import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Initialize Faker and seed for reproducibility
fake = Faker()
Faker.seed(42)
np.random.seed(42)

def generate_billing_data(num_rows=5000):
    print(f"🚀 Generating {num_rows} rows of synthetic AWS billing data...")

    data = []

    # 1. Define typical AWS Services and their pricing models
    services = {
        'AmazonEC2': {'unit': 'Hrs', 'cost_range': (0.5, 4.0)},
        'AmazonRDS': {'unit': 'Hrs', 'cost_range': (1.2, 8.0)},
        'AmazonS3': {'unit': 'GB-Mo', 'cost_range': (0.023, 0.05)},
        'AmazonLambda': {'unit': 'Requests', 'cost_range': (0.0000166667, 0.0002)}
    }

    # 2. Create a list of "Active" Resource IDs to track over time
    resource_ids = [fake.uuid4() for _ in range(50)]

    # --- THE TRAP: Create a "Zombie Resource" ---
    # This resource exists but does nothing useful.
    zombie_id = "i-000000-ZOMBIE-ASSET"
    resource_ids.append(zombie_id)
    
    # Generate dates for the last 90 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    for _ in range(num_rows):
        date = fake.date_between(start_date=start_date, end_date=end_date)
        service = random.choice(list(services.keys()))
        resource_id = random.choice(resource_ids)
        
        # Default Logic
        usage_amount = round(random.uniform(1.0, 24.0), 2)
        cost_per_unit = random.uniform(*services[service]['cost_range'])
        unblended_cost = round(usage_amount * cost_per_unit, 4)
        owner_tag = random.choice(['engineering', 'data-science', 'marketing', 'unknown'])
        
        # --- INJECT ZOMBIE LOGIC ---
        # If this is the Zombie ID, it has Cost (Reservation/Storage) but ZERO Usage (CPU/Requests)
        if resource_id == zombie_id:
            service = 'AmazonEC2' # It's an idle server
            usage_amount = 0.0      # Zero CPU utilization or active hours logged as "usage"
            unblended_cost = 45.0   # But it still costs money (e.g. unattached EBS volume or Reserved Instance)
            owner_tag = 'legacy-team' # Harder to find who owns it

        data.append({
            'LineItem/UsageStartDate': date,
            'LineItem/ResourceId': resource_id,
            'LineItem/ProductCode': service,
            'LineItem/UsageAmount': usage_amount,
            'LineItem/UnblendedCost': unblended_cost,
            'ResourceTags/user:Owner': owner_tag
        })

    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Sort by date for realism
    df['LineItem/UsageStartDate'] = pd.to_datetime(df['LineItem/UsageStartDate'])
    df = df.sort_values(by='LineItem/UsageStartDate')
    

    # --- FIX THE PATH LOGIC HERE ---
    # Get the directory where THIS script is located (e.g., /app/src)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to data/raw relative to this script
    # It resolves to: /app/src/../data/raw/aws_billing_data.csv -> /app/data/raw/...
    output_path = os.path.join(current_dir, '../data/raw/aws_billing_data.csv')
    
    # Normalize the path to remove the '..' (cleaner)
    output_path = os.path.normpath(output_path)
   # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"✅ Success! Saved to: {output_path}")
    print(f"👀 Hint: Look for resource '{zombie_id}' in the data.")

if __name__ == "__main__":
    generate_billing_data()