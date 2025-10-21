"""Quick test for OracleConnector.

Run from repo root:
    python scripts/test_connection.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dataiq.oracle_connector import OracleConnector


def main():
    oc = OracleConnector()
    ok = oc.test_connection()
    print(f"Connection OK: {ok}")
    if not ok:
        return

    with oc.connect() as conn:
        user_tables = oc.list_tables(conn=conn)
        print(f"Tables ({len(user_tables)}):", user_tables[:10])
        if user_tables:
            t = user_tables[0]
            cols = oc.get_columns(t, conn=conn)
            print(f"Columns for {t}:")
            print(cols.head())
            sample = oc.sample_table(t, rows=5, conn=conn)
            print(f"Sample from {t}:")
            print(sample.head())

    oc.dispose()


if __name__ == "__main__":
    main()
