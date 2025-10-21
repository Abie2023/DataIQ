"""
Data cleaning utilities for DataIQ.

Provides duplicate removal, null handling, and string normalization.
Saves cleaned data to outputs/cleaned_data/.
"""
from __future__ import annotations

import os
import logging
from typing import Optional

import pandas as pd
import numpy as np

from .oracle_connector import _setup_logger, _ensure_dirs, DEFAULT_CONFIG_PATH, LOCAL_OVERRIDE_PATH
import yaml


def _load_paths_from_config(config_path: str = DEFAULT_CONFIG_PATH):
    def _load_yaml(path: str):
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    base = _load_yaml(config_path)
    override = _load_yaml(LOCAL_OVERRIDE_PATH)
    paths = {**base.get("paths", {}), **override.get("paths", {})}
    logs_dir = paths.get("logs_dir", "logs")
    outputs_dir = paths.get("outputs_dir", "outputs")
    cleaned_dir = os.path.join(outputs_dir, "cleaned_data")
    _ensure_dirs([logs_dir, outputs_dir, cleaned_dir])
    return {"logs_dir": logs_dir, "outputs_dir": outputs_dir, "cleaned_dir": cleaned_dir}


class DataCleaner:
    """Clean and standardize pandas DataFrames for DataIQ."""

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH) -> None:
        paths = _load_paths_from_config(config_path)
        self.logs_dir = paths["logs_dir"]
        self.outputs_dir = paths["outputs_dir"]
        self.cleaned_dir = paths["cleaned_dir"]
        self.logger = _setup_logger(self.logs_dir)

    def clean_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        cleaned = df.drop_duplicates()
        after = len(cleaned)
        self.logger.info("Removed %d duplicate rows", before - after)
        return cleaned

    def handle_nulls(self, df: pd.DataFrame, strategy: str = "drop") -> pd.DataFrame:
        """Handle nulls by strategy.

        - drop: drop any rows with nulls
        - fill_mean: fill numeric columns with mean, non-numeric with mode
        """
        if strategy not in {"drop", "fill_mean"}:
            raise ValueError("strategy must be 'drop' or 'fill_mean'")

        if strategy == "drop":
            before = len(df)
            cleaned = df.dropna()
            self.logger.info("Dropped %d rows with nulls", before - len(cleaned))
            return cleaned

        # fill_mean
        cleaned = df.copy()
        num_cols = cleaned.select_dtypes(include=[np.number]).columns
        for c in num_cols:
            if cleaned[c].isna().any():
                mean_val = cleaned[c].mean()
                cleaned[c] = cleaned[c].fillna(mean_val)
                self.logger.info("Filled NaNs in numeric column '%s' with mean %.4f", c, mean_val)
        other_cols = [c for c in cleaned.columns if c not in num_cols]
        for c in other_cols:
            if cleaned[c].isna().any():
                try:
                    mode_val = cleaned[c].mode(dropna=True)
                    fill_val = mode_val.iloc[0] if not mode_val.empty else ""
                except Exception:
                    fill_val = ""
                cleaned[c] = cleaned[c].fillna(fill_val)
                self.logger.info("Filled NaNs in non-numeric column '%s' with mode/default", c)
        return cleaned

    def normalize_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        cleaned = df.copy()
        obj_cols = cleaned.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            cleaned[c] = cleaned[c].astype("string")
            cleaned[c] = cleaned[c].str.strip().str.normalize("NFKC")
        self.logger.info("Normalized string columns: %s", list(obj_cols))
        return cleaned

    def save_cleaned(self, df: pd.DataFrame, name: str = "dataset") -> str:
        safe = name.replace(os.sep, "_")
        out_path = os.path.join(self.cleaned_dir, f"cleaned_{safe}.csv")
        df.to_csv(out_path, index=False)
        self.logger.info("Saved cleaned dataset: %s", out_path)
        return out_path


__all__ = ["DataCleaner"]
