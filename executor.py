import pandas as pd
import json

class PandasExecutor:
    """Safely executes dynamically mapped Pandas queries against a loaded DataFrame."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
        # Auto-fix common issues Agent 3 identified
        self._auto_fix_dtypes()

    def _auto_fix_dtypes(self):
        """Silently cast common columns to correct types for easier Pandas querying using sampling."""
        # Lowercase all actual columns to find standard ones
        cols_lower = {c.lower(): c for c in self.df.columns}
        
        # Auto-cast known timestamp columns based on 100-row sample
        for t_col in ['timestamp', 'date', 'time', 'trans_date', 'transaction_date']:
            if t_col in cols_lower:
                real_col = cols_lower[t_col]
                if self.df[real_col].dtype == 'object':
                    sample = self.df[real_col].dropna().head(100)
                    if len(sample) > 0 and sample.astype(str).str.match(r'^\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}').all():
                        try:
                            self.df[real_col] = pd.to_datetime(self.df[real_col], errors='coerce')
                        except (ValueError, TypeError) as e:
                            print(f"[Warning] Failed to auto-cast {real_col} to datetime: {e}")
        
        # Auto-cast known amount columns based on 100-row sample
        for a_col in ['amount', 'trans_amt', 'value', 'usd_amount', 'amount_paid']:
            if a_col in cols_lower:
                real_col = cols_lower[a_col]
                if self.df[real_col].dtype == 'object':
                    sample = self.df[real_col].dropna().head(100)
                    if len(sample) > 0 and sample.astype(str).str.match(r'^[\$,]?\s*\d+(?:,\d{3})*(?:\.\d+)?$').all():
                        try:
                            self.df[real_col] = self.df[real_col].replace(r'[\$,]', '', regex=True).astype(float)
                        except (ValueError, TypeError) as e:
                            print(f"[Warning] Failed to auto-cast {real_col} to float: {e}")

        # Pre-compute Global Schema Mappings for N+1 Metrics Calculation
        self.amount_col = None
        self.date_col = None
        self.account_col = None

        for c in self.df.columns:
            lower_c = c.lower()
            if 'amount' in lower_c or 'value' in lower_c or 'amt' in lower_c:
                self.amount_col = c
                break
                
        for c in self.df.columns:
            lower_c = c.lower()
            if 'date' in lower_c or 'time' in lower_c or 'timestamp' in lower_c:
                self.date_col = c
                break
                
        for c in self.df.columns:
            lower_c = c.lower()
            if 'account' in lower_c or 'acct' in lower_c or 'id' in lower_c:
                self.account_col = c
                break

    def get_schema_summary(self):
        """Returns standard headers and 5 sample values for Agent 2 to use in mapping."""
        return {
            "columns": list(self.df.columns),
            "sample_csv": json.dumps({
                col: self.df[col].dropna().unique()[:5].tolist()
                for col in self.df.columns
            }, default=str)
        }

    def execute_mapped_query(self, mapped_query: str):
        """
        Executes dynamically mapped Pandas queries safely using df.eval() rather than query() to get a boolean mask.
        Returns indices of violations rather than a full copied DataFrame for memory efficiency.
        """
        try:
            if not mapped_query or str(mapped_query).strip() == "":
                 return {"success": False, "error": "Empty query string."}
                 
            # df.eval returns a boolean mask, meaning we don't immediately copy rows.
            mask = self.df.eval(mapped_query)
            
            # Count True values in mask without allocating memory
            violation_count = mask.sum()
            
            # Save strictly what is needed: a small sample and the mask indices
            if violation_count > 0:
                sample_df = self.df[mask].head(5).copy()
                violation_indices = mask[mask].index
            else:
                 sample_df = pd.DataFrame()
                 violation_indices = pd.Index([])
            
            return {
                "success": True,
                "violation_count": int(violation_count),
                "violating_indices": violation_indices,
                "sample_df": sample_df
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def run_all_rules_and_collect_metrics(self, rules_from_agent2):
        """Runs Agent 3's execution loop and compiles the metric dictionary for reporting."""
        metrics = []
        
        for rule in rules_from_agent2:
            # If Agent 2 skipped it because of missing columns
            if rule['status'] != 'READY' or not rule['pandas_query']:
                metrics.append({
                    "rule_id": rule['rule_id'],
                    "title": rule['title'],
                    "severity": rule['severity'],
                    "status": "SKIPPED",
                    "violation_count": 0,
                    "total_amount_exposure": 0
                })
                continue
                
            print(f"Agent 3: Executing mapped query for '{rule['title']}'...")
            result = self.execute_mapped_query(rule['pandas_query'])
            
            if not result["success"]:
                 print(f"  [ERROR] {result['error']}")
                 metrics.append({
                    "rule_id": rule['rule_id'],
                    "title": rule['title'],
                    "severity": rule['severity'],
                    "status": "ERROR: " + result["error"],
                    "violation_count": 0,
                    "total_amount_exposure": 0
                })
            else:
                count = result["violation_count"]
                print(f"  [SUCCESS] Found {count} violations.")
                
                # Default generic metrics
                unique_accounts = 0
                total_exposure = 0
                avg_amount = 0
                date_range = "N/A"
                top_offenders = []
                
                # Dynamically extract Agent 2's exact mappings for this specific rule
                mapped_cols = rule.get('columns_remapped', [])
                rule_amount_col = None
                rule_date_col = None
                rule_account_col = None
                
                for mapping in mapped_cols:
                    if '->' in mapping:
                        generic, actual = mapping.split('->')
                        generic, actual = generic.strip().lower(), actual.strip()
                        if generic in ['amount', 'trans_amt', 'value']:
                            rule_amount_col = actual
                        elif generic in ['timestamp', 'date', 'time']:
                            rule_date_col = actual
                        elif generic in ['sender_account', 'account', 'from_acct']:
                            rule_account_col = actual
                
                if count > 0:
                    indices = result["violating_indices"]
                    sample_df = result["sample_df"]
                    
                    # Compute aggregations using the exact row indices on the actual mapped columns
                    target_amount_col = rule_amount_col or self.amount_col
                    target_date_col = rule_date_col or self.date_col
                    target_account_col = rule_account_col or self.account_col

                    if target_amount_col and target_amount_col in self.df.columns:
                        try:
                            amounts = pd.to_numeric(self.df.loc[indices, target_amount_col], errors='coerce').fillna(0)
                            total_exposure = amounts.sum()
                            avg_amount = amounts.mean()
                        except Exception as e:
                            print(f"[Warning] Failed to aggregate amount column {target_amount_col}: {e}")
                            
                    if target_date_col and target_date_col in self.df.columns:
                        try:
                            dates = pd.to_datetime(self.df.loc[indices, target_date_col], errors='coerce').dropna()
                            if not dates.empty:
                                date_range = f"{dates.min().strftime('%Y-%m-%d %H:%M')} to {dates.max().strftime('%Y-%m-%d %H:%M')}"
                        except Exception as e:
                            print(f"[Warning] Failed to find date range for {target_date_col}: {e}")
                            
                    if target_account_col and target_account_col in self.df.columns:
                        try:
                            accounts = self.df.loc[indices, target_account_col]
                            unique_accounts = accounts.nunique()
                            top_3 = accounts.value_counts().head(3)
                            top_offenders = [f"{acct} ({val} txns)" for acct, val in top_3.items()]
                        except Exception as e:
                            print(f"[Warning] Failed to extract top offenders for {target_account_col}: {e}")
                
                # Risk Score (1-10)
                base_scores = {"CRITICAL": 8, "HIGH": 5, "MEDIUM": 3, "LOW": 1}
                risk_score = base_scores.get(rule['severity'].upper(), 1)
                
                if count > (len(self.df) * 0.1): # > 10% of total rows
                    risk_score += 1
                if float(total_exposure) > 1000000:
                    risk_score += 1
                    
                risk_score = min(score for score in [risk_score, 10]) # Cap at 10

                metrics.append({
                    "rule_id": rule['rule_id'],
                    "title": rule['title'],
                    "severity": rule['severity'],
                    "status": "FLAGGED" if count > 0 else "CLEAN",
                    "risk_score": risk_score,
                    "violation_count": count,
                    "unique_accounts": int(unique_accounts),
                    "total_amount_exposure": float(total_exposure),
                    "avg_amount": float(avg_amount),
                    "date_range": date_range,
                    "top_offenders": top_offenders,
                    "sample_offending_row": sample_df.to_dict(orient="records") if count > 0 else []
                })
                
        return json.dumps(metrics, separators=(',', ':'), default=str)
