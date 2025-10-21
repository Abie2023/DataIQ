"""
Automated test runner for DataIQ.

Runs:
1. Unit tests via pytest
2. Oracle connection verification
3. main.py in profile, clean, and detect modes sequentially

Reports pass/fail for each stage and exits with code 1 if any fail.

Usage:
    python scripts/auto_test.py
"""
from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dataiq.oracle_connector import _setup_logger

logger = _setup_logger("logs")


def print_header(title: str) -> None:
    """Print a formatted section header."""
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def run_pytest() -> bool:
    """Run pytest test suite."""
    print_header("Running Unit Tests (pytest)")
    logger.info("Starting pytest test suite")
    
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
        if result.returncode == 0:
            print("✓ Passed: Unit Tests")
            logger.info("pytest tests passed")
            return True
        else:
            print("✗ Failed: Unit Tests")
            logger.error("pytest tests failed")
            return False
            
    except Exception as e:
        print(f"✗ Failed: Unit Tests - {e}")
        logger.exception(f"pytest execution error: {e}")
        return False


def check_oracle_connection() -> bool:
    """Verify Oracle database connectivity."""
    print_header("Checking Oracle Connection")
    logger.info("Verifying Oracle connection")
    
    try:
        from dataiq.oracle_connector import OracleConnector
        
        conn = OracleConnector()
        if conn.test_connection():
            print("✓ Passed: Oracle Connection")
            logger.info("Oracle connection verified")
            
            # Get a sample table for testing
            with conn.connect() as c:
                tables = conn.get_table_names()
                if tables:
                    print(f"  Found {len(tables)} table(s) in schema")
                    logger.info(f"Found {len(tables)} tables")
                else:
                    print("  ⚠ Warning: No tables found in schema")
                    logger.warning("No tables found in schema")
            
            conn.dispose()
            return True
        else:
            print("✗ Failed: Oracle Connection")
            logger.error("Oracle connection failed")
            return False
            
    except Exception as e:
        print(f"✗ Failed: Oracle Connection - {e}")
        logger.exception(f"Oracle connection error: {e}")
        return False


def run_main_mode(mode: str, table: str | None = None) -> bool:
    """Run main.py in a specific mode."""
    print_header(f"Running main.py --mode {mode}")
    logger.info(f"Executing main.py --mode {mode}")
    
    cmd = [sys.executable, "main.py", "--mode", mode, "--limit", "100"]
    if table:
        cmd.extend(["--table", table])
    
    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"✓ Passed: main.py --mode {mode}")
            logger.info(f"main.py --mode {mode} completed successfully")
            return True
        else:
            print(f"✗ Failed: main.py --mode {mode}")
            logger.error(f"main.py --mode {mode} failed with exit code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"✗ Failed: main.py --mode {mode} (timeout)")
        logger.error(f"main.py --mode {mode} timed out")
        return False
    except Exception as e:
        print(f"✗ Failed: main.py --mode {mode} - {e}")
        logger.exception(f"main.py --mode {mode} error: {e}")
        return False


def get_test_table() -> str | None:
    """Get a table name to use for testing."""
    try:
        from dataiq.oracle_connector import OracleConnector
        conn = OracleConnector()
        with conn.connect() as c:
            tables = conn.get_table_names()
            conn.dispose()
            return tables[0] if tables else None
    except Exception:
        return None


def main() -> int:
    print("=" * 60)
    print("DataIQ Automated Test Runner")
    print("=" * 60)
    
    logger.info("Starting automated test suite")
    
    # Track results
    results = {}
    
    # Stage 1: Unit tests
    results["Unit Tests"] = run_pytest()
    
    # Stage 2: Oracle connection
    results["Oracle Connection"] = check_oracle_connection()
    
    # Get a test table if available
    test_table = None
    if results["Oracle Connection"]:
        test_table = get_test_table()
        if test_table:
            print(f"\nUsing table '{test_table}' for integration tests")
            logger.info(f"Using table '{test_table}' for testing")
        else:
            print("\n⚠ Warning: No tables available for integration tests")
            logger.warning("No tables available for integration tests")
    
    # Stage 3-5: main.py modes (only if Oracle connection succeeded)
    if results["Oracle Connection"] and test_table:
        results["Profile Mode"] = run_main_mode("profile", test_table)
        results["Clean Mode"] = run_main_mode("clean", test_table)
        results["Detect Mode"] = run_main_mode("detect", test_table)
    else:
        print("\n⚠ Skipping integration tests (no Oracle connection or no tables)")
        logger.warning("Skipping integration tests")
        results["Profile Mode"] = None
        results["Clean Mode"] = None
        results["Detect Mode"] = None
    
    # Summary
    print()
    print("=" * 60)
    print("Test Summary:")
    print("=" * 60)
    
    all_passed = True
    skipped = False
    
    for test_name, passed in results.items():
        if passed is None:
            print(f"  ⊘ SKIPPED: {test_name}")
            skipped = True
        elif passed:
            print(f"  ✓ PASSED: {test_name}")
        else:
            print(f"  ✗ FAILED: {test_name}")
            all_passed = False
    
    print()
    
    if all_passed and not skipped:
        print("🎉 All tests passed!")
        logger.info("All tests passed")
        return 0
    elif all_passed and skipped:
        print("⚠ Tests passed but some were skipped")
        logger.warning("Tests passed with skipped stages")
        return 0
    else:
        print("❌ Some tests failed. See logs/app.log for details.")
        logger.error("Test suite completed with failures")
        return 1


if __name__ == "__main__":
    sys.exit(main())
