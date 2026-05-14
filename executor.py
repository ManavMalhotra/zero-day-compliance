import json
import re

import pandas as pd

from utils import canonicalize_name


class PandasExecutor:
    """Executes mapped Pandas rules against a loaded DataFrame with controlled fallbacks."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self._canonical_columns = {canonicalize_name(column): column for column in self.df.columns}
        self._auto_fix_dtypes()
        self.amount_col = self._find_best_column(["amount_paid", "amount_received", "amount", "value", "amt"])
        self.date_col = self._find_best_column(["timestamp", "date", "time", "transaction_date", "trans_date"])
        self.account_col = self._find_best_column(["from_account", "sender_account", "account", "acct", "account_id"])

    def _log(self, on_log, message: str):
        if on_log:
            on_log(message)

    def _find_best_column(self, candidates):
        for candidate in candidates:
            canonical = canonicalize_name(candidate)
            if canonical in self._canonical_columns:
                return self._canonical_columns[canonical]

        for canonical, actual in self._canonical_columns.items():
            if any(token in canonical for token in candidates):
                return actual
        return None

    def _series_looks_numeric(self, series: pd.Series) -> bool:
        sample = series.dropna().astype(str).head(100)
        if sample.empty:
            return False
        cleaned = sample.str.replace(r"[\$,]", "", regex=True).str.strip()
        return cleaned.str.match(r"^-?\d+(?:\.\d+)?$").all()

    def _series_looks_datetime(self, series: pd.Series) -> bool:
        sample = series.dropna().astype(str).head(100)
        if sample.empty:
            return False
        return sample.str.match(r"^\d{4}[-/]\d{2}[-/]\d{2}").all()

    def _auto_fix_dtypes(self):
        """Casts likely numeric and datetime columns even when names contain spaces/punctuation."""
        for column in self.df.columns:
            canonical = canonicalize_name(column)
            series = self.df[column]

            if series.dtype != "object":
                continue

            if any(token in canonical for token in ["timestamp", "date", "time"]) and self._series_looks_datetime(series):
                self.df[column] = pd.to_datetime(series, errors="coerce")
                continue

            if any(token in canonical for token in ["amount", "value", "amt"]) and self._series_looks_numeric(series):
                cleaned = series.astype(str).str.replace(r"[\$,]", "", regex=True).str.strip()
                self.df[column] = pd.to_numeric(cleaned, errors="coerce")

    def get_schema_summary(self):
        return {
            "columns": list(self.df.columns),
            "sample_csv": json.dumps(
                {column: self.df[column].dropna().unique()[:5].tolist() for column in self.df.columns},
                default=str,
            ),
        }

    def _normalize_query_text(self, query: str) -> str:
        cleaned = str(query or "").strip()
        cleaned = cleaned.strip("`") if cleaned.startswith("```") else cleaned
        cleaned = cleaned.replace("transactions", "df")
        # Free models often emit unicode comparison operators.
        cleaned = cleaned.replace("\u2265", ">=")  # ≥
        cleaned = cleaned.replace("\u2264", "<=")   # ≤
        cleaned = cleaned.replace("\u2260", "!=")   # ≠
        cleaned = cleaned.replace("\u2013", "-")    # en-dash
        cleaned = cleaned.replace("\u2014", "-")    # em-dash
        return cleaned

    def _is_unsafe_expression(self, expression: str) -> bool:
        lowered = expression.lower()
        blocked_markers = [
            "__",
            "import ",
            "open(",
            "exec(",
            "eval(",
            "compile(",
            "globals(",
            "locals(",
            "subprocess",
            "os.",
            "sys.",
        ]
        return any(marker in lowered for marker in blocked_markers)

    def _result_to_dataframe(self, result):
        if isinstance(result, pd.DataFrame):
            return result

        if isinstance(result, pd.Series):
            if result.dtype == bool:
                return self.df.loc[result.fillna(False)]
            return self.df.loc[result.index]

        if isinstance(result, pd.Index):
            return self.df.loc[result]

        if isinstance(result, (list, tuple, set)):
            return self.df.loc[list(result)]

        raise TypeError(f"Unsupported Pandas execution result: {type(result).__name__}")

    def _prepare_df_for_execution(self):
        """Returns a copy of df with DatetimeIndex if a datetime column exists, enabling rolling('24h')."""
        exec_df = self.df
        if self.date_col and self.date_col in exec_df.columns:
            if pd.api.types.is_datetime64_any_dtype(exec_df[self.date_col]):
                exec_df = exec_df.sort_values(self.date_col).set_index(self.date_col, drop=False)
        return exec_df

    def _execute_query_expression(self, expression: str):
        import numpy as np
        exec_df = self._prepare_df_for_execution()
        scope = {
            "df": exec_df,
            "transactions": exec_df,
            "pd": pd,
            "np": np,
        }
        allowed_builtins = {"len": len, "abs": abs, "min": min, "max": max, "sum": sum, "round": round}

        if expression.startswith(("df.", "df[", "pd.", "(")):
            result = eval(expression, {"__builtins__": allowed_builtins}, scope)
            return self._result_to_dataframe(result)

        return exec_df.query(expression, engine="python", local_dict={"pd": pd, "np": np})

    def execute_mapped_query(self, mapped_query: str, on_log=None):
        """
        Executes either:
        - a row filter expression for df.query(), or
        - a full pandas expression returning a DataFrame / mask / indices.
        """
        try:
            if not mapped_query or str(mapped_query).strip() == "":
                return {"success": False, "error": "Empty query string."}

            expression = self._normalize_query_text(mapped_query)
            if self._is_unsafe_expression(expression):
                raise ValueError("Blocked unsafe pandas expression.")

            filtered_df = self._execute_query_expression(expression)
            violation_count = len(filtered_df)

            if violation_count > 0:
                sample_df = filtered_df.head(5).copy()
                violation_indices = filtered_df.index
            else:
                sample_df = pd.DataFrame(columns=self.df.columns)
                violation_indices = pd.Index([])

            self._log(on_log, f"Query executed successfully. Matched {violation_count} rows.")
            return {
                "success": True,
                "violation_count": int(violation_count),
                "violating_indices": violation_indices,
                "violating_df": filtered_df,
                "sample_df": sample_df,
            }
        except Exception as error:
            self._log(on_log, f"Query execution failed: {error}")
            return {"success": False, "error": str(error)}

    def _extract_rule_targets(self, rule):
        mapped_cols = rule.get("columns_remapped", [])
        rule_amount_col = None
        rule_date_col = None
        rule_account_col = None

        for mapping in mapped_cols:
            if "->" not in mapping:
                continue
            generic, actual = mapping.split("->", 1)
            generic = canonicalize_name(generic)
            actual = actual.strip()
            if generic in {"amount", "trans_amt", "value"}:
                rule_amount_col = actual
            elif generic in {"timestamp", "date", "time"}:
                rule_date_col = actual
            elif generic in {"sender_account", "account", "from_acct"}:
                rule_account_col = actual

        return (
            rule_amount_col or self.amount_col,
            rule_date_col or self.date_col,
            rule_account_col or self.account_col,
        )

    def run_all_rules_and_collect_metrics(self, rules_from_agent2, on_log=None):
        metrics = []

        for rule in rules_from_agent2:
            if rule["status"] != "READY" or not rule.get("pandas_query"):
                skip_reason = rule.get("skip_reason", "Missing columns or unsupported mapping.")
                self._log(on_log, f"Skipping {rule['rule_id']} - {rule['title']} because Agent 2 marked it as {rule['status']}.")
                metrics.append(
                    {
                        "rule_id": rule["rule_id"],
                        "title": rule["title"],
                        "severity": rule["severity"],
                        "status": "SKIPPED",
                        "skip_reason": skip_reason,
                        "violation_count": 0,
                        "unique_accounts": 0,
                        "total_amount_exposure": 0.0,
                        "avg_amount": 0.0,
                        "date_range": "N/A",
                        "top_offenders": [],
                        "risk_score": {"CRITICAL": 8, "HIGH": 5, "MEDIUM": 3, "LOW": 1}.get(rule["severity"].upper(), 1),
                        "sql_query": rule.get("sql_query", ""),
                        "pandas_query": rule.get("pandas_query", ""),
                    }
                )
                continue

            self._log(on_log, f"Executing {rule['rule_id']} - {rule['title']}.")
            result = self.execute_mapped_query(rule["pandas_query"], on_log=on_log)

            if not result["success"]:
                error_message = result["error"]
                self._log(on_log, f"{rule['rule_id']} failed: {error_message}")
                metrics.append(
                    {
                        "rule_id": rule["rule_id"],
                        "title": rule["title"],
                        "severity": rule["severity"],
                        "status": "ERROR",
                        "error_message": error_message,
                        "violation_count": 0,
                        "unique_accounts": 0,
                        "total_amount_exposure": 0.0,
                        "avg_amount": 0.0,
                        "date_range": "N/A",
                        "top_offenders": [],
                        "risk_score": {"CRITICAL": 8, "HIGH": 5, "MEDIUM": 3, "LOW": 1}.get(rule["severity"].upper(), 1),
                        "sql_query": rule.get("sql_query", ""),
                        "pandas_query": rule.get("pandas_query", ""),
                    }
                )
                continue

            count = result["violation_count"]
            self._log(on_log, f"{rule['rule_id']} completed with {count} matched rows.")

            unique_accounts = 0
            total_exposure = 0.0
            avg_amount = 0.0
            date_range = "N/A"
            top_offenders = []
            sample_df = result["sample_df"]
            violating_df = result["violating_df"]
            indices = result["violating_indices"]
            target_amount_col, target_date_col, target_account_col = self._extract_rule_targets(rule)

            if count > 0:
                if target_amount_col and (target_amount_col in violating_df.columns or target_amount_col in self.df.columns):
                    try:
                        source_amounts = violating_df[target_amount_col] if target_amount_col in violating_df.columns else self.df.loc[indices, target_amount_col]
                        amounts = pd.to_numeric(source_amounts, errors="coerce").fillna(0)
                        total_exposure = float(amounts.sum())
                        avg_amount = float(amounts.mean())
                    except Exception as error:
                        self._log(on_log, f"Warning: failed amount aggregation for {target_amount_col}: {error}")

                if target_date_col and (target_date_col in violating_df.columns or target_date_col in self.df.columns):
                    try:
                        source_dates = violating_df[target_date_col] if target_date_col in violating_df.columns else self.df.loc[indices, target_date_col]
                        dates = pd.to_datetime(source_dates, errors="coerce").dropna()
                        if not dates.empty:
                            date_range = f"{dates.min().strftime('%Y-%m-%d %H:%M')} to {dates.max().strftime('%Y-%m-%d %H:%M')}"
                    except Exception as error:
                        self._log(on_log, f"Warning: failed date aggregation for {target_date_col}: {error}")

                if target_account_col and (target_account_col in violating_df.columns or target_account_col in self.df.columns):
                    try:
                        accounts = violating_df[target_account_col] if target_account_col in violating_df.columns else self.df.loc[indices, target_account_col]
                        unique_accounts = int(accounts.nunique())
                        top_3 = accounts.astype(str).value_counts().head(3)
                        top_offenders = [f"{acct} ({value} rows)" for acct, value in top_3.items()]
                    except Exception as error:
                        self._log(on_log, f"Warning: failed offender aggregation for {target_account_col}: {error}")

            base_scores = {"CRITICAL": 8, "HIGH": 5, "MEDIUM": 3, "LOW": 1}
            risk_score = base_scores.get(rule["severity"].upper(), 1)
            if count > (len(self.df) * 0.1):
                risk_score += 1
            if total_exposure > 1_000_000:
                risk_score += 1
            risk_score = min(risk_score, 10)

            metrics.append(
                {
                    "rule_id": rule["rule_id"],
                    "title": rule["title"],
                    "severity": rule["severity"],
                    "status": "FLAGGED" if count > 0 else "CLEAN",
                    "violation_count": count,
                    "unique_accounts": unique_accounts,
                    "total_amount_exposure": total_exposure,
                    "avg_amount": avg_amount,
                    "date_range": date_range,
                    "top_offenders": top_offenders,
                    "risk_score": risk_score,
                    "sql_query": rule.get("sql_query", ""),
                    "pandas_query": rule.get("pandas_query", ""),
                    "sample_offending_row": sample_df.to_dict(orient="records") if count > 0 else [],
                }
            )
            self._log(on_log, f"{rule['rule_id']} metrics ready. Risk score {risk_score}/10, exposure {total_exposure:,.2f}.")

        summary = {
            "total_rules": len(metrics),
            "executed_rules": sum(1 for metric in metrics if metric["status"] in {"FLAGGED", "CLEAN"}),
            "flagged_rules": sum(1 for metric in metrics if metric["status"] == "FLAGGED"),
            "clean_rules": sum(1 for metric in metrics if metric["status"] == "CLEAN"),
            "skipped_rules": sum(1 for metric in metrics if metric["status"] == "SKIPPED"),
            "error_rules": sum(1 for metric in metrics if metric["status"] == "ERROR"),
            "total_confirmed_exposure": float(sum(metric.get("total_amount_exposure", 0.0) for metric in metrics if metric["status"] == "FLAGGED")),
        }
        summary["coverage_pct"] = round(
            (summary["executed_rules"] / summary["total_rules"]) * 100, 2
        ) if summary["total_rules"] else 0.0

        lean_metrics = []
        for metric in metrics:
            lean_metrics.append({key: value for key, value in metric.items() if key != "sample_offending_row"})

        payload = {"summary": summary, "rules": lean_metrics}
        self._log(on_log, f"Prepared execution metrics for {len(lean_metrics)} rules. Coverage {summary['coverage_pct']}%.")
        return json.dumps(payload, separators=(",", ":"), default=str)
