import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def create_mock_data():
    print("Generating mock IBM AML transaction data...")
    num_records = 5000
    
    # Base accounts
    normal_accounts = [f"ACC_{i:04d}" for i in range(1, 1001)]
    suspicious_accounts = ["ACC_9999", "ACC_8888", "ACC_7777"]
    all_accounts = normal_accounts + suspicious_accounts
    
    banks = ["Bank_A", "Bank_B", "Bank_C", "Bank_D", "Offshore_Bank_X"]
    
    data = []
    start_date = datetime(2026, 1, 1)
    
    for i in range(num_records):
        timestamp = start_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        from_acc = random.choice(all_accounts)
        to_acc = random.choice(all_accounts)
        while from_acc == to_acc:
            to_acc = random.choice(all_accounts)
            
        from_bank = random.choice(banks)
        to_bank = random.choice(banks)
        
        # Normal transaction amounts
        amount = round(random.uniform(10.0, 5000.0), 2)
        currency = "USD"
        
        # Inject some laundering patterns (Structuring: amounts just under 10k)
        if random.random() < 0.02 or from_acc in suspicious_accounts:
            amount = round(random.uniform(9000.0, 9999.0), 2)
            
        # Large anomaly
        if random.random() < 0.01:
            amount = round(random.uniform(50000.0, 100000.0), 2)
            
        data.append({
            "Transaction_ID": f"TXN_{i:05d}",
            "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "From_Bank": from_bank,
            "From_Account": from_acc,
            "To_Bank": to_bank,
            "To_Account": to_acc,
            "Amount_Paid": amount,
            "Currency": currency
        })
        
    df = pd.DataFrame(data)
    
    # Save to CSV
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/ibm_aml_transactions.csv", index=False)
    print(f"Created data/ibm_aml_transactions.csv with {len(df)} records.")
    
    # Create Sanctions List
    sanctions_data = [
        {"Entity_Name": "Shady Corp", "Account_ID": "ACC_9999", "Sanction_Type": "OFAC"},
        {"Entity_Name": "Bad Actor Inc", "Account_ID": "ACC_8888", "Sanction_Type": "EU_Sanctions"}
    ]
    pd.DataFrame(sanctions_data).to_csv("data/sanctions_list.csv", index=False)
    print("Created data/sanctions_list.csv")

if __name__ == "__main__":
    create_mock_data()
