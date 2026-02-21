import pandas as pd
from executor import PandasExecutor
import json

df = pd.read_csv('data/ibm_aml_sample_1000.csv')
executor = PandasExecutor(df)

rules = [
  {
    "rule_id": "Rule 3.3",
    "title": "Ultra-High Transaction Alert",
    "severity": "CRITICAL",
    "pandas_query": "`Amount Paid` >= 1000000",
    "status": "READY"
  }
]

metrics = executor.run_all_rules_and_collect_metrics(rules)
with open('metrics.json', 'w') as f:
    json.dump(metrics, f, indent=2)
