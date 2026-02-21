import duckdb
import pandas as pd
import json

class DatabaseEngine:
    def __init__(self):
        # We start an in-memory duckdb instance
        self.con = duckdb.connect(database=':memory:', read_only=False)
        self.setup_tables()
        
    def setup_tables(self):
        print("Setting up DuckDB tables from CSV...")
        try:
            # We assume data is already generated
            # self.con.execute("CREATE TABLE transactions AS SELECT * FROM read_csv_auto('data/911.csv')")
            self.con.execute("CREATE TABLE transactions AS SELECT * FROM read_csv_auto('data/ibm_aml_transactions.csv')")
            self.con.execute("CREATE TABLE sanctions AS SELECT * FROM read_csv_auto('data/sanctions_list.csv')")
            print("Tables 'transactions' and 'sanctions' are ready.")
        except Exception as e:
            print(f"Error loading CSVs. Did you run generate_mock_data.py? Error: {e}")

    def get_schema_map(self):
        """
        The Ground Truth Schema Map to prevent LLM hallucination.
        """
        schema = {
            "tables": {
                "transactions": {
                    "description": "Financial transactions between accounts.",
                    "columns": {
                        "Transaction_ID": "varchar",
                        "Timestamp": "timestamp",
                        "From_Bank": "varchar",
                        "From_Account": "varchar",
                        "To_Bank": "varchar",
                        "To_Account": "varchar",
                        "Amount_Paid": "double",
                        "Currency": "varchar"
                    }
                },
                "sanctions": {
                    "description": "Government blacklist of sanctioned accounts and entities.",
                    "columns": {
                        "Entity_Name": "varchar",
                        "Account_ID": "varchar",
                        "Sanction_Type": "varchar"
                    }
                }
            }
        }
        return json.dumps(schema, indent=2)

    def execute_query(self, sql_query):
        """
        Executes the AI-generated SQL query and returns the flagged records as a dict.
        """
        try:
            df = self.con.execute(sql_query).df()
            return {"success": True, "data": df.to_dict(orient='records'), "count": len(df)}
        except Exception as e:
            return {"success": False, "error": str(e)}

if __name__ == "__main__":
    # Test the engine
    engine = DatabaseEngine()
    print("Schema Map:")
    print(engine.get_schema_map())
    
    test_sql = "SELECT * FROM transactions WHERE Amount_Paid > 50000 LIMIT 5"
    result = engine.execute_query(test_sql)
    print(f"Test Query Result: {result['count']} rows found.")
