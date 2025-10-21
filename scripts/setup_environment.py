"""
Environment setup and validation script for DataIQ.

Checks:
- Oracle connectivity using cx_Oracle
- Folder structure exists (logs, outputs, dashboard assets)
- Missing dependencies from requirements.txt
- Logs all results to logs/app.log

Usage:
    python scripts/setup_environment.py
"""
from __future__ import annotations

import os
import sys
import subprocess
import importlib.util
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dataiq.oracle_connector import _setup_logger, _ensure_dirs

logger = _setup_logger("logs")

REQUIRED_FOLDERS = [
    "logs",
    "outputs/reports",
    "outputs/profiles",
    "outputs/cleaned_data",
    "dashboard/assets/charts",
]


def check_oracle_connectivity() -> bool:
    """Test Oracle connection using python-oracledb."""
    logger.info("Checking Oracle connectivity...")
    print("[1/4] Checking Oracle connectivity...")
    
    try:
        import oracledb
        print("  ✓ python-oracledb module imported")
        logger.info("python-oracledb module imported successfully")
        
        # Try to connect using config
        try:
            from dataiq.oracle_connector import OracleConnector
            conn = OracleConnector()
            if conn.test_connection():
                print("  ✓ Oracle connection successful")
                logger.info("Oracle connection test passed")
                conn.dispose()
                return True
            else:
                print("  ✗ Oracle connection failed (check credentials in config/db_config.yaml)")
                logger.warning("Oracle connection test failed")
                return False
        except Exception as e:
            print(f"  ✗ Oracle connection error: {e}")
            logger.error(f"Oracle connection error: {e}")
            return False
            
    except ImportError:
        print("  ✗ python-oracledb not installed")
        logger.error("python-oracledb not installed")
        return False


def verify_folder_structure() -> bool:
    """Ensure all required folders exist."""
    logger.info("Verifying folder structure...")
    print("[2/4] Verifying folder structure...")
    
    all_exist = True
    for folder in REQUIRED_FOLDERS:
        folder_path = PROJECT_ROOT / folder
        if not folder_path.exists():
            print(f"  → Creating missing folder: {folder}")
            logger.info(f"Creating folder: {folder}")
            folder_path.mkdir(parents=True, exist_ok=True)
        else:
            print(f"  ✓ {folder}")
    
    print("  ✓ All required folders exist")
    logger.info("Folder structure verified")
    return True


def check_dependencies() -> bool:
    """Check and install missing dependencies from requirements.txt."""
    logger.info("Checking dependencies...")
    print("[3/4] Checking dependencies...")
    
    requirements_file = PROJECT_ROOT / "requirements.txt"
    if not requirements_file.exists():
        print("  ✗ requirements.txt not found")
        logger.error("requirements.txt not found")
        return False
    
    # Read requirements
    with open(requirements_file, "r", encoding="utf-8") as f:
        requirements = [
            line.strip().split(">=")[0].split("==")[0]
            for line in f
            if line.strip() and not line.startswith("#")
        ]
    
    missing = []
    for pkg in requirements:
        # Handle package name variations
        import_name = pkg.replace("-", "_").lower()
        # Special cases
        if pkg == "fpdf2":
            import_name = "fpdf"
        elif pkg == "scikit-learn":
            import_name = "sklearn"
        elif pkg == "cx_Oracle":
            import_name = "cx_Oracle"
        elif pkg == "PyYAML":
            import_name = "yaml"
        
        if importlib.util.find_spec(import_name) is None:
            missing.append(pkg)
            print(f"  ✗ Missing: {pkg}")
        else:
            print(f"  ✓ {pkg}")
    
    if missing:
        print(f"\n  → Installing {len(missing)} missing package(s)...")
        logger.info(f"Installing missing packages: {missing}")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--quiet"] + missing
            )
            print("  ✓ Dependencies installed")
            logger.info("Missing dependencies installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Failed to install dependencies: {e}")
            logger.error(f"Failed to install dependencies: {e}")
            return False
    else:
        print("  ✓ All dependencies installed")
        logger.info("All dependencies present")
        return True


def check_oracle_instant_client() -> bool:
    """Check python-oracledb mode (thin vs thick)."""
    logger.info("Checking python-oracledb mode...")
    print("[4/4] Checking python-oracledb mode...")
    
    try:
        import oracledb
        print(f"  ✓ python-oracledb version: {oracledb.__version__}")
        logger.info(f"python-oracledb version: {oracledb.__version__}")
        
        # Check if running in thin mode (default, no Instant Client needed)
        try:
            print(f"  ✓ Running in thin mode (no Oracle Instant Client required)")
            logger.info("python-oracledb running in thin mode")
        except Exception as e:
            print(f"  ⚠ Could not determine mode: {e}")
            logger.warning(f"Could not determine oracledb mode: {e}")
        
        return True
    except ImportError:
        print("  ✗ python-oracledb not installed")
        logger.error("python-oracledb not installed")
        return False


def main() -> int:
    print("=" * 60)
    print("DataIQ Environment Setup & Validation")
    print("=" * 60)
    print()
    
    logger.info("Starting environment setup")
    
    results = {
        "Oracle Connectivity": check_oracle_connectivity(),
        "Folder Structure": verify_folder_structure(),
        "Dependencies": check_dependencies(),
        "Oracle Client": check_oracle_instant_client(),
    }
    
    print()
    print("=" * 60)
    print("Summary:")
    print("=" * 60)
    
    all_passed = True
    for check, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {check}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 Environment ready!")
        logger.info("Environment setup completed successfully")
        return 0
    else:
        print("⚠ Some checks failed. Review logs/app.log for details.")
        logger.warning("Environment setup completed with failures")
        return 1


if __name__ == "__main__":
    sys.exit(main())
