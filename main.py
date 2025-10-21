"""
DataIQ Orchestrator Script

Usage:
  python main.py --mode [profile|clean|detect|report|all] --table <TABLE_NAME> --limit 1000
"""
from __future__ import annotations

import argparse
import sys
import os
import traceback

import pandas as pd

from dataiq.oracle_connector import OracleConnector, _setup_logger, DEFAULT_CONFIG_PATH
from dataiq.data_profiler import DataProfiler
from dataiq.data_cleaner import DataCleaner
from dataiq.anomaly_detector import AnomalyDetector
from dataiq.report_generator import ReportGenerator


logger = _setup_logger("logs")


def run_profile(conn: OracleConnector, table: str, limit: int) -> dict:
    print(f"[DataIQ] Profiling table: {table} (limit={limit})")
    df = conn.fetch_table_data(table_name=table, limit=limit)
    profiler = DataProfiler()
    profile = profiler.profile(df, name=table)
    score = profiler.generate_data_health_score(profile)
    print(f"[DataIQ] Profiling complete. Health Score: {score:.2f}")
    return {"df": df, "profile": profile, "score": score}


def run_clean(df: pd.DataFrame, name: str) -> dict:
    print(f"[DataIQ] Cleaning dataset: {name}")
    cleaner = DataCleaner()
    df1 = cleaner.clean_duplicates(df)
    df2 = cleaner.handle_nulls(df1, strategy="fill_mean")
    df3 = cleaner.normalize_strings(df2)
    out_path = cleaner.save_cleaned(df3, name=name)
    print(f"[DataIQ] Cleaning complete. Saved: {out_path}")
    return {"df": df3, "path": out_path}


def run_detect(df: pd.DataFrame) -> dict:
    print("[DataIQ] Running anomaly detection")
    detector = AnomalyDetector()
    result = detector.detect(df)
    print("[DataIQ] Anomaly detection complete")
    return result


def run_report(profile: dict, score: float, anomalies: dict) -> dict:
    print("[DataIQ] Generating reports")
    generator = ReportGenerator()
    paths = generator.generate_reports(profile, score, anomalies)
    print(f"[DataIQ] Reports generated: {paths}")
    return paths


def orchestrate(mode: str, table: str | None, limit: int) -> int:
    conn = OracleConnector(DEFAULT_CONFIG_PATH)

    try:
        if not conn.test_connection():
            print("[DataIQ] ERROR: Database connectivity failed. See logs/app.log for details.")
            return 2

        # Choose a default table if not provided
        if not table:
            tables = conn.get_table_names()
            if not tables:
                print("[DataIQ] No tables found in schema.")
                return 3
            table = tables[0]
            print(f"[DataIQ] No --table provided. Using: {table}")

        if mode == "profile":
            result = run_profile(conn, table, limit)
        elif mode == "clean":
            df = conn.fetch_table_data(table_name=table, limit=limit)
            result = run_clean(df, name=table)
        elif mode == "detect":
            df = conn.fetch_table_data(table_name=table, limit=limit)
            result = run_detect(df)
        elif mode == "report":
            # Re-profile to compute a report directly
            prof = run_profile(conn, table, limit)
            result = run_report(prof["profile"], prof["score"], run_detect(prof["df"]))
        elif mode == "all":
            prof = run_profile(conn, table, limit)
            cleaned = run_clean(prof["df"], name=table)
            anomalies = run_detect(cleaned["df"])
            result = run_report(prof["profile"], prof["score"], anomalies)
        else:
            print(f"[DataIQ] Unknown mode: {mode}")
            return 4

        return 0
    except Exception as e:
        logger.exception("Main orchestration failed: %s\n%s", e, traceback.format_exc())
        print(f"[DataIQ] ERROR: {e}")
        return 1
    finally:
        conn.dispose()


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DataIQ Orchestrator")
    p.add_argument("--mode", choices=["profile", "clean", "detect", "report", "all"], required=True,
                   help="Which pipeline to run")
    p.add_argument("--table", help="Table name to process", default=None)
    p.add_argument("--limit", type=int, default=1000, help="Row limit for sampling")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    exit_code = orchestrate(args.mode, args.table, args.limit)
    sys.exit(exit_code)
