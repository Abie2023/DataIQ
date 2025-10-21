import os
import pandas as pd

from dataiq.data_profiler import DataProfiler


def test_profile_and_health_score(tmp_path, monkeypatch):
    # Redirect outputs to temp directory
    profiles_dir = tmp_path / "outputs" / "profiles"
    profiles_dir.mkdir(parents=True)

    dp = DataProfiler()
    dp.profiles_dir = str(profiles_dir)

    df = pd.DataFrame({
        "a": [1, 2, None, 4],
        "b": ["x", "y", "y", None],
        "c": [1.0, 2.5, 2.5, 4.0],
    })
    prof = dp.profile(df, name="test")
    assert os.path.exists(prof["csv_path"])  # CSV created

    score = dp.generate_data_health_score(prof)
    assert 0.0 <= score <= 100.0
