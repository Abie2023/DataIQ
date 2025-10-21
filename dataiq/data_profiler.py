"""
Data profiling utilities for DataIQ.

Computes per-column nulls, duplicates, type consistency, and summary stats.
Saves profiling outputs to CSV under outputs/profiles/.
"""
from __future__ import annotations

import os
import logging
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd

from .oracle_connector import _setup_logger, _ensure_dirs, DEFAULT_CONFIG_PATH, LOCAL_OVERRIDE_PATH
import yaml


def _load_paths_from_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, str]:
    def _load_yaml(path: str):
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    base = _load_yaml(config_path)
    override = _load_yaml(LOCAL_OVERRIDE_PATH)
    paths = {**base.get("paths", {}), **override.get("paths", {})}
    # defaults
    logs_dir = paths.get("logs_dir", "logs")
    outputs_dir = paths.get("outputs_dir", "outputs")
    profiles_dir = os.path.join(outputs_dir, "profiles")
    _ensure_dirs([logs_dir, outputs_dir, profiles_dir])
    return {"logs_dir": logs_dir, "outputs_dir": outputs_dir, "profiles_dir": profiles_dir}


class DataProfiler:
    """Profile pandas DataFrames for DataIQ."""

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH) -> None:
        paths = _load_paths_from_config(config_path)
        self.logs_dir = paths["logs_dir"]
        self.outputs_dir = paths["outputs_dir"]
        self.profiles_dir = paths["profiles_dir"]
        self.logger = _setup_logger(self.logs_dir)

    def _infer_type_series(self, s: pd.Series) -> str:
        if pd.api.types.is_integer_dtype(s):
            return "int"
        if pd.api.types.is_float_dtype(s):
            return "float"
        if pd.api.types.is_bool_dtype(s):
            return "bool"
        if pd.api.types.is_datetime64_any_dtype(s):
            return "datetime"
        return "string"

    def profile(self, df: pd.DataFrame, name: Optional[str] = None) -> Dict[str, Any]:
        """Compute profiling metrics and save CSV under outputs/profiles.

        Returns a dictionary summary including per-column stats and overall counts.
        """
        self.logger.info("Starting profiling%s", f" for {name}" if name else "")
        summary_rows = []

        # Duplicate rows count
        duplicate_rows = int(df.duplicated().sum())

        for col in df.columns:
            s = df[col]
            nulls = int(s.isna().sum())
            total = int(len(s))
            uniques = int(s.nunique(dropna=True))
            dtype_inferred = self._infer_type_series(s)

            # Type consistency: fraction of values convertible to inferred type
            type_mismatch = 0
            if dtype_inferred == "int":
                mism = s.dropna().apply(lambda x: isinstance(x, (int, np.integer)))
                type_mismatch = int((~mism).sum())
            elif dtype_inferred == "float":
                mism = s.dropna().apply(lambda x: isinstance(x, (int, float, np.number)))
                type_mismatch = int((~mism).sum())
            elif dtype_inferred == "bool":
                mism = s.dropna().apply(lambda x: isinstance(x, (bool, np.bool_)))
                type_mismatch = int((~mism).sum())
            elif dtype_inferred == "datetime":
                def _is_dt(v):
                    try:
                        pd.to_datetime(v)
                        return True
                    except Exception:
                        return False
                type_mismatch = int((~s.dropna().apply(_is_dt)).sum())
            else:  # string
                # Strings are permissive; mark non-string scalars as mismatch if not NaN
                mism = s.dropna().apply(lambda x: isinstance(x, str))
                type_mismatch = int((~mism).sum())

            # Summary stats (numeric only when applicable)
            if pd.api.types.is_numeric_dtype(s):
                desc = s.describe()
                mean = float(desc.get("mean", np.nan)) if not pd.isna(desc.get("mean", np.nan)) else np.nan
                std = float(desc.get("std", np.nan)) if not pd.isna(desc.get("std", np.nan)) else np.nan
                min_ = float(desc.get("min", np.nan)) if not pd.isna(desc.get("min", np.nan)) else np.nan
                max_ = float(desc.get("max", np.nan)) if not pd.isna(desc.get("max", np.nan)) else np.nan
                median = float(s.median()) if s.notna().any() else np.nan
            else:
                mean = std = min_ = max_ = median = np.nan

            summary_rows.append({
                "column": col,
                "dtype": str(df[col].dtype),
                "inferred_type": dtype_inferred,
                "null_count": nulls,
                "duplicate_rows": duplicate_rows,  # overall duplicate rows for context
                "type_mismatch_count": type_mismatch,
                "unique_count": uniques,
                "mean": mean,
                "median": median,
                "std": std,
                "min": min_,
                "max": max_,
                "total_rows": total,
            })

        prof_df = pd.DataFrame(summary_rows)
        # Save CSV
        base = (name or "dataset").replace(os.sep, "_")
        out_path = os.path.join(self.profiles_dir, f"profile_{base}.csv")
        prof_df.to_csv(out_path, index=False)
        self.logger.info("Profile saved: %s", out_path)

        # Aggregate metrics for health score
        overall = {
            "rows": int(len(df)),
            "duplicate_rows": duplicate_rows,
            "total_nulls": int(df.isna().sum().sum()),
            "columns": int(df.shape[1]),
        }

        return {"per_column": prof_df, "overall": overall, "csv_path": out_path}

    def generate_data_health_score(self, profile: Dict[str, Any]) -> float:
        """Compute a 0–100 score based on nulls, duplicates, and type consistency.

        Heuristic:
        - Start at 100
        - Penalize null ratio up to -50
        - Penalize duplicate row ratio up to -25
        - Penalize type mismatch ratio up to -25
        """
        self.logger.info("Computing data health score")

        df_cols: pd.DataFrame = profile["per_column"]
        overall = profile["overall"]
        rows = max(1, overall.get("rows", 1))

        # Ratios
        null_ratio = min(1.0, overall.get("total_nulls", 0) / (rows * max(1, len(df_cols))))
        dup_ratio = min(1.0, overall.get("duplicate_rows", 0) / rows)
        mismatch_ratio = 0.0
        if rows > 0:
            mismatch_ratio = min(1.0, float(df_cols.get("type_mismatch_count", pd.Series([0]*len(df_cols))).sum()) / (rows * max(1, len(df_cols))))

        score = 100.0
        score -= 50.0 * null_ratio
        score -= 25.0 * dup_ratio
        score -= 25.0 * mismatch_ratio

        score = max(0.0, min(100.0, score))
        self.logger.info("Data health score: %.2f", score)
        return score


__all__ = ["DataProfiler"]
