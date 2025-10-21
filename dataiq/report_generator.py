"""
Report generation for DataIQ: PDF (fpdf) and HTML.
"""
from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Dict, Any

from fpdf import FPDF

from .oracle_connector import _setup_logger, _ensure_dirs, DEFAULT_CONFIG_PATH, LOCAL_OVERRIDE_PATH
import yaml
import pandas as pd


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
    reports_dir = os.path.join(outputs_dir, "reports")
    _ensure_dirs([logs_dir, outputs_dir, reports_dir])
    return {"logs_dir": logs_dir, "outputs_dir": outputs_dir, "reports_dir": reports_dir}


class ReportGenerator:
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH) -> None:
        paths = _load_paths_from_config(config_path)
        self.logs_dir = paths["logs_dir"]
        self.outputs_dir = paths["outputs_dir"]
        self.reports_dir = paths["reports_dir"]
        self.logger = _setup_logger(self.logs_dir)

    def _timestamp(self) -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _safe_path(self, prefix: str, ext: str) -> str:
        ts = self._timestamp()
        return os.path.join(self.reports_dir, f"{prefix}_{ts}.{ext}")

    def generate_reports(self, profiling_summary: Dict[str, Any], health_score: float,
                         anomaly_summary: Dict[str, Any]) -> Dict[str, str]:
        """Create PDF and HTML reports and return their paths."""
        try:
            pdf_path = self._create_pdf(profiling_summary, health_score, anomaly_summary)
            html_path = self._create_html(profiling_summary, health_score, anomaly_summary)
            self.logger.info("Reports generated: PDF=%s, HTML=%s", pdf_path, html_path)
            return {"pdf": pdf_path, "html": html_path}
        except Exception as e:
            self.logger.exception("Failed to generate reports: %s", e)
            raise

    def _create_pdf(self, profiling_summary: Dict[str, Any], health_score: float,
                    anomaly_summary: Dict[str, Any]) -> str:
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Arial", size=16)
        pdf.cell(0, 10, "DataIQ Data Quality Report", ln=True)

        pdf.set_font("Arial", size=12)
        pdf.cell(0, 8, f"Health Score: {health_score:.2f}", ln=True)

        overall = profiling_summary.get("overall", {})
        pdf.cell(0, 8, f"Rows: {overall.get('rows', 0)} | Columns: {overall.get('columns', 0)}", ln=True)
        pdf.cell(0, 8, f"Duplicates: {overall.get('duplicate_rows', 0)} | Total Nulls: {overall.get('total_nulls', 0)}", ln=True)

        # Per-column summary table
        df_cols: pd.DataFrame = profiling_summary.get("per_column")
        if isinstance(df_cols, pd.DataFrame) and not df_cols.empty:
            pdf.ln(4)
            pdf.set_font("Arial", style="B", size=11)
            pdf.cell(0, 8, "Column Summary", ln=True)
            pdf.set_font("Arial", size=9)

            # Simple table format
            for idx, row in df_cols.head(15).iterrows():
                col_name = str(row.get("column", ""))[:25]
                null_cnt = row.get("null_count", 0)
                unique = row.get("unique_count", 0)
                pdf.cell(0, 5, f"{col_name}: nulls={null_cnt}, unique={unique}", ln=True)

        # Anomalies summary
        per_col = anomaly_summary.get("per_column", {}) if anomaly_summary else {}
        if per_col:
            pdf.ln(4)
            pdf.set_font("Arial", style="B", size=11)
            pdf.cell(0, 8, "Anomalies", ln=True)
            pdf.set_font("Arial", size=9)
            for k, v in list(per_col.items())[:10]:  # Limit to 10
                pdf.cell(0, 5, f"{str(k)[:30]}: {v} outliers", ln=True)

        # Attach chart if exists
        chart_path = anomaly_summary.get("chart_path") if anomaly_summary else None
        if chart_path and os.path.exists(chart_path):
            try:
                pdf.ln(4)
                pdf.image(chart_path, w=180)
            except Exception:
                # ignore image issues
                pass

        pdf_path = self._safe_path("dataiq_report", "pdf")
        pdf.output(pdf_path)
        return pdf_path

    def _create_html(self, profiling_summary: Dict[str, Any], health_score: float,
                     anomaly_summary: Dict[str, Any]) -> str:
        css = """
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #222; }
            .score { font-size: 1.2em; margin-bottom: 10px; }
            table { border-collapse: collapse; width: 100%; margin-top: 10px; }
            th, td { border: 1px solid #ccc; padding: 6px 8px; font-size: 12px; }
            th { background: #f5f5f5; }
        """

        overall = profiling_summary.get("overall", {})
        df_cols: pd.DataFrame = profiling_summary.get("per_column")
        html_table = df_cols.head(50).to_html(index=False) if isinstance(df_cols, pd.DataFrame) else ""

        per_col = anomaly_summary.get("per_column", {}) if anomaly_summary else {}
        chart_path = anomaly_summary.get("chart_path") if anomaly_summary else None

        html = f"""
        <html>
          <head><style>{css}</style></head>
          <body>
            <h1>DataIQ Data Quality Report</h1>
            <div class="score">Health Score: {health_score:.2f}</div>
            <div>Rows: {overall.get('rows', 0)} | Columns: {overall.get('columns', 0)} | Duplicates: {overall.get('duplicate_rows', 0)} | Total Nulls: {overall.get('total_nulls', 0)}</div>
            <h2>Per-column summary (first 50 rows)</h2>
            {html_table}
            <h2>Anomalies</h2>
            <ul>
            {''.join(f'<li>{k}: {v}</li>' for k, v in per_col.items())}
            </ul>
            {f'<img src="{chart_path}" style="max-width: 100%;" />' if chart_path else ''}
          </body>
        </html>
        """

        html_path = self._safe_path("dataiq_report", "html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html_path


__all__ = ["ReportGenerator"]
