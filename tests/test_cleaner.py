import os
import pandas as pd

from dataiq.data_cleaner import DataCleaner


def test_clean_duplicates_and_nulls(tmp_path):
    cleaned_dir = tmp_path / "outputs" / "cleaned_data"
    cleaned_dir.mkdir(parents=True)

    dc = DataCleaner()
    dc.cleaned_dir = str(cleaned_dir)

    df = pd.DataFrame({
        "a": [1, 1, 2, None],
        "b": [" x ", " x ", "y", None],
    })
    d1 = dc.clean_duplicates(df)
    assert len(d1) < len(df)

    d2 = dc.handle_nulls(d1, strategy="fill_mean")
    assert d2.isna().sum().sum() == 0

    d3 = dc.normalize_strings(d2)
    assert d3["b"].iloc[0] == "x"

    out = dc.save_cleaned(d3, name="test")
    assert os.path.exists(out)
