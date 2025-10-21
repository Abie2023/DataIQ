"""
Anomaly detection for numeric columns using IsolationForest.

Saves bar chart of outlier counts per column under dashboard/assets/charts/.
"""
from __future__ import annotations

import os
import logging
from typing import Dict, Any

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt

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
    charts_dir = os.path.join("dashboard", "assets", "charts")
    _ensure_dirs([logs_dir, outputs_dir, charts_dir])
    return {"logs_dir": logs_dir, "outputs_dir": outputs_dir, "charts_dir": charts_dir}


class AnomalyDetector:
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH) -> None:
        paths = _load_paths_from_config(config_path)
        self.logs_dir = paths["logs_dir"]
        self.outputs_dir = paths["outputs_dir"]
        self.charts_dir = paths["charts_dir"]
        self.logger = _setup_logger(self.logs_dir)

    def detect(self, df: pd.DataFrame, random_state: int = 42) -> Dict[str, Any]:
        self.logger.info("Starting anomaly detection")

        num_df = df.select_dtypes(include=[np.number])
        if num_df.empty:
            self.logger.warning("No numeric columns found; skipping anomaly detection")
            return {"per_column": {}, "chart_path": None}

        # IsolationForest works on all numeric features; we'll compute per-column flags by running separately per column
        outlier_counts = {}
        for col in num_df.columns:
            s = num_df[[col]].dropna()
            if s.empty:
                outlier_counts[col] = 0
                continue
            try:
                model = IsolationForest(contamination="auto", random_state=random_state)
                labels = model.fit_predict(s)
                outliers = int((labels == -1).sum())
                outlier_counts[col] = outliers
            except Exception as e:
                self.logger.exception("IsolationForest failed on column %s: %s", col, e)
                outlier_counts[col] = 0

        # Plot bar chart
        if outlier_counts:
            cols = list(outlier_counts.keys())
            vals = [outlier_counts[c] for c in cols]
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar(cols, vals, color="#d9534f")
            ax.set_title("Anomaly counts per numeric column")
            ax.set_ylabel("Outliers")
            ax.set_xticks(range(len(cols)))
            ax.set_xticklabels(cols, rotation=45, ha="right")
            fig.tight_layout()
            chart_path = os.path.join(self.charts_dir, "anomalies_per_column.png")
            fig.savefig(chart_path)
            plt.close(fig)
            self.logger.info("Anomaly chart saved: %s", chart_path)
        else:
            chart_path = None

        return {"per_column": outlier_counts, "chart_path": chart_path}


__all__ = ["AnomalyDetector"]
