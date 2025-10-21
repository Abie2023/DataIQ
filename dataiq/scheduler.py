"""
Simple scheduler for DataIQ using `schedule`.
"""
from __future__ import annotations

import time
import os
from datetime import datetime

import schedule

from .oracle_connector import _setup_logger, DEFAULT_CONFIG_PATH


logger = _setup_logger("logs")


def run_daily_profile():
    start = datetime.now()
    logger.info("Daily profiling job started at %s", start.isoformat())
    try:
        # Intentionally minimal; integration with main flow to be added later
        # This function would orchestrate: fetch data -> profile -> save outputs
        logger.info("Daily profiling job completed")
    except Exception as e:
        logger.exception("Daily profiling job failed: %s", e)


def run_weekly_clean():
    start = datetime.now()
    logger.info("Weekly cleaning job started at %s", start.isoformat())
    try:
        # Intentionally minimal; integration with main flow to be added later
        # This function would orchestrate: fetch data -> clean -> save outputs
        logger.info("Weekly cleaning job completed")
    except Exception as e:
        logger.exception("Weekly cleaning job failed: %s", e)


def start_scheduler():
    # Example: profile daily at 01:00, clean weekly on Sunday at 02:00
    schedule.every().day.at("01:00").do(run_daily_profile)
    schedule.every().sunday.at("02:00").do(run_weekly_clean)

    logger.info("Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)


__all__ = ["run_daily_profile", "run_weekly_clean", "start_scheduler"]
