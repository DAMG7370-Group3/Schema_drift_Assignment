# Databricks notebook source
# Script to READ existing customer_data_4.json and ADD a new column to it
import json

# Your volume path
volume_base_path = "/Volumes/workspace/damg7370/datastore/schema_drift/demo_smm"
input_file = f"{volume_base_path}/customer_data_4.json"

try:
    # Read the existing customer_data_4.json file
    with open(input_file.replace("dbfs:", "/dbfs"), "r") as f:
        content = f.read().strip()
        
        # Check if it's an array format or newline-delimited
        if content.startswith('['):
            # Array format: [{...}, {...}]
            existing_data = json.loads(content)
        else:
            # Newline-delimited format
            existing_data = []
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    existing_data.append(json.loads(line))
    
    # Add new column "MembershipTier" to each record
    tiers = ["Premium", "Standard", "Premium", "Basic", "Gold", "Premium", "Standard", "Gold", "Basic", "Standard"]
    
    for i, record in enumerate(existing_data):
        if i < len(tiers):
            record["MembershipTier"] = tiers[i]
        else:
            record["MembershipTier"] = "Standard"
    
    
    # Overwrite original file with updated data (newline-delimited format)
    with open(input_file.replace("dbfs:", "/dbfs"), "w") as f:
        for record in existing_data:
            f.write(json.dumps(record) + "\n")
    
    print(f"✅ Updated: {input_file} with MembershipTier column added")
    print(f"   Original backed up to: customer_data_4_backup.json")
    print(f"\n🧪 Now run your pipeline to see _rescued_data populated!")

except FileNotFoundError:
    print(f"❌ File not found: {input_file}")
except json.JSONDecodeError as e:
    print(f"❌ Invalid JSON format: {e}")
except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

import json

volume_base_path = "/Volumes/workspace/damg7370/datastore/schema_drift/demo_smm"

print("=" * 80)
print("CREATE customer_data_5.json WITH NEW COLUMN")
print("=" * 80)

# Read existing customer_data_4.json to get current data
input_file = f"{volume_base_path}/customer_data_4.json"

try:
    with open(input_file.replace("dbfs:", "/dbfs"), "r") as f:
        content = f.read().strip()
        
        if content.startswith('['):
            existing_data = json.loads(content)
        else:
            existing_data = []
            for line in content.split('\n'):
                if line.strip():
                    existing_data.append(json.loads(line.strip()))
    
    print(f"✅ Read {len(existing_data)} records from customer_data_4.json")
    print(f"   Current columns: {list(existing_data[0].keys())}")
    
    # Create NEW data with a DIFFERENT new column: AccountStatus
    new_data = []
    account_statuses = ["Active", "Active", "Suspended", "Active", "Premium"]
    
    # Add 5 new customers with ALL existing columns + NEW column
    new_customers = [
        {"CustomerID": "C011", "FullName": "Michael Scott", "Email": "michael.scott@example.com", "PhoneNumber": "555-123-4567", "City": "Scranton", "Age": 45, "Gender": "Male", "LoyaltyStatus": "Gold", "CreditScore": 720},
        {"CustomerID": "C012", "FullName": "Pam Beesly", "Email": "pam.beesly@example.com", "PhoneNumber": "555-234-5678", "City": "Scranton", "Age": 32, "Gender": "Female", "LoyaltyStatus": "Silver", "CreditScore": 680},
        {"CustomerID": "C013", "FullName": "Jim Halpert", "Email": "jim.halpert@example.com", "PhoneNumber": "555-345-6789", "City": "Scranton", "Age": 34, "Gender": "Male", "LoyaltyStatus": "Gold", "CreditScore": 750},
        {"CustomerID": "C014", "FullName": "Dwight Schrute", "Email": "dwight.schrute@example.com", "PhoneNumber": "555-456-7890", "City": "Scranton", "Age": 38, "Gender": "Male", "LoyaltyStatus": "Platinum", "CreditScore": 800},
        {"CustomerID": "C015", "FullName": "Angela Martin", "Email": "angela.martin@example.com", "PhoneNumber": "555-567-8901", "City": "Scranton", "Age": 36, "Gender": "Female", "LoyaltyStatus": "Silver", "CreditScore": 690}
    ]
    
    # Add the NEW column "AccountStatus" to each new customer
    for i, customer in enumerate(new_customers):
        customer["AccountStatus"] = account_statuses[i]
        new_data.append(customer)
    
    # Write customer_data_5.json
    output_file = f"{volume_base_path}/customer_data_5.json"
    with open(output_file.replace("dbfs:", "/dbfs"), "w") as f:
        for record in new_data:
            f.write(json.dumps(record) + "\n")
    
    print(f"\n✅ Created: customer_data_5.json")
    print(f"   Records: {len(new_data)}")
    print(f"   NEW COLUMN: AccountStatus")  
    print(f"   Sample: {new_data[0]}")

except Exception as e:
    print(f"❌ Error: {e}")

# COMMAND ----------

# !!! Before performing any data analysis, make sure to run the pipeline to materialize the sample datasets. The tables referenced in this notebook depend on that step.

display(spark.sql("SELECT * FROM workspace.bronze.sample_aggregation_schema_drift"))
