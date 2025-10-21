# DataIQ CSV Upload Mode - User Guide

## Overview

DataIQ now supports **CSV Upload Mode**, allowing you to run complete data quality analysis **without Oracle XE** or any database connection. This is perfect for:

- Quick data quality checks on any CSV file
- Offline analysis when database is unavailable
- Testing DataIQ features before setting up Oracle
- Analyzing data from non-Oracle sources

## Features

### 1. Data Source Selection

- Toggle between **Oracle Database** and **CSV Upload** modes
- Seamless switching without restart
- Independent session management

### 2. CSV Upload Capabilities

- ✅ **Upload any CSV file** from your computer
- ✅ **Automatic sampling** for large files (>10,000 rows)
- ✅ **Data profiling** with health score calculation
- ✅ **Data cleaning** (duplicates, nulls, string normalization)
- ✅ **AI-powered anomaly detection** with visual charts
- ✅ **Download cleaned data** as CSV
- ✅ **Export profiling results** as CSV

### 3. Metrics Displayed

- **Dataset Overview:**
  - Total rows and columns
  - Missing values count
  - Duplicate rows count
- **Profiling Results:**
  - Data health score (0-100)
  - Per-column statistics
  - Null percentages
  - Data type analysis
- **Anomaly Detection:**
  - Anomalies per column
  - Visual bar charts
  - Anomaly counts and distributions

## Quick Start

### Step 1: Launch Dashboard

```powershell
streamlit run dashboard/app.py
```

### Step 2: Select CSV Upload Mode

1. In the sidebar, find "Select Data Source"
2. Choose **"CSV Upload"**
3. The interface will switch to CSV upload mode

### Step 3: Upload Your CSV

1. Click **"Browse files"** in the sidebar
2. Select a CSV file from your computer
3. Wait for the file to load (large files will be sampled)
4. View the dataset summary

### Step 4: Run Analysis

Click any of these buttons in the sidebar:

- **🔍 Run Profiling** - Analyze data quality and generate health score
- **🧹 Run Cleaning** - Remove duplicates, handle nulls, normalize strings
- **🎯 Detect Anomalies** - Find outliers using machine learning

### Step 5: View Results

- **Profile results** show in the main area with health score
- **Cleaned data preview** displays first 10 rows
- **Anomaly charts** visualize outliers per column
- **Download buttons** export cleaned data and profiles

## Sample Data

DataIQ includes sample data for testing CSV upload mode:

**File:** `sample_data/test_data.csv`

**Contents:** 30 rows of employee data with:

- Employee ID, name, department, salary
- Hire date, email, age, performance score
- **Intentional quality issues:**
  - Missing values in salary, age, and email columns
  - Potential outliers in performance scores
  - String inconsistencies

**Test with sample data:**

```powershell
# Run automated test
python scripts/test_csv_upload.py
```

## Workflow Example

### Scenario: Analyzing a customer dataset

1. **Upload File**

   - Select `customers.csv` from your downloads folder
   - File: 5,000 rows × 12 columns
   - System message: "✅ File loaded: customers.csv"

2. **Profile Data**

   - Click "🔍 Run Profiling"
   - Health Score: 78.5 / 100
   - Issues found: 15% null values, 2% duplicates

3. **Clean Data**

   - Click "🧹 Run Cleaning"
   - Duplicates removed: 100 rows
   - Nulls filled with mean/mode
   - Strings normalized

4. **Detect Anomalies**

   - Click "🎯 Detect Anomalies"
   - Found 127 anomalies across 5 numeric columns
   - Visual chart shows distribution

5. **Export Results**
   - Click "💾 Download Cleaned Data as CSV"
   - File saved: `cleaned_customers.csv`
   - Ready for import into your system!

## Technical Details

### File Size Limits

- **Files ≤ 10,000 rows**: Processed completely
- **Files > 10,000 rows**: Automatically sampled to 10,000 rows
- **Sampling method**: Random sampling (seed=42) for reproducibility

### Supported Operations

| Operation         | Oracle Mode | CSV Upload Mode   |
| ----------------- | ----------- | ----------------- |
| Data Profiling    | ✅          | ✅                |
| Data Cleaning     | ✅          | ✅                |
| Anomaly Detection | ✅          | ✅                |
| Report Generation | ✅          | ⚠️ Manual export  |
| Scheduled Jobs    | ✅          | ❌ Not applicable |

### Data Processing Pipeline

```
CSV Upload → Load into pandas DataFrame
           → Store in session_state
           → Run profiling (optional)
           → Run cleaning (optional)
           → Run anomaly detection (optional)
           → Download results
```

### Session Management

- Uploaded data stored in `st.session_state["uploaded_data"]`
- Switching data sources clears session cache
- Multiple uploads replace previous data (no stacking)

## Troubleshooting

### Issue: File upload fails

**Solution:**

- Check file format (must be `.csv`)
- Verify CSV structure (comma-separated, valid headers)
- Try opening in Excel/Notepad to validate format

### Issue: Profiling shows errors

**Solution:**

- Check for non-standard characters in data
- Verify column data types
- Look for completely empty columns

### Issue: Anomaly detection finds no anomalies

**Solution:**

- Dataset may be too clean (no outliers)
- Verify numeric columns exist
- Check if data has sufficient variance

### Issue: Large file takes too long

**Solution:**

- System automatically samples large files
- Files >10,000 rows are reduced to 10,000
- Warning message displays sampling info

## Comparison: Oracle vs CSV Mode

| Feature            | Oracle Database Mode      | CSV Upload Mode           |
| ------------------ | ------------------------- | ------------------------- |
| **Setup Required** | Oracle XE installation    | None                      |
| **Data Source**    | Oracle tables             | Local CSV files           |
| **Connection**     | SQLAlchemy + oracledb     | Direct pandas             |
| **Max Records**    | Limited by `--limit` flag | 10,000 (auto-sampled)     |
| **Persistence**    | Data stays in database    | Temporary (session-based) |
| **Automation**     | Scheduler support         | Manual runs only          |
| **Best For**       | Production ETL pipelines  | Quick analysis, testing   |

## Advanced Tips

### 1. Working with Large Files

If your CSV has millions of rows:

1. Pre-sample before upload using pandas:
   ```python
   df = pd.read_csv('huge_file.csv')
   df.sample(n=10000).to_csv('sampled.csv', index=False)
   ```
2. Upload `sampled.csv` to DataIQ

### 2. Cleaning Strategy Selection

- **Default:** Nulls filled with mean (numeric) or mode (categorical)
- **Alternative:** Edit `data_cleaner.py` to change strategy
- Options: `drop`, `fill_mean`, `fill_median`, `fill_mode`

### 3. Customizing Anomaly Detection

- Default: IsolationForest with contamination=0.1
- Edit `anomaly_detector.py` to adjust sensitivity:
  ```python
  clf = IsolationForest(contamination=0.05)  # Stricter
  clf = IsolationForest(contamination=0.2)   # More lenient
  ```

### 4. Exporting Multiple Formats

Currently supports CSV. For Excel export:

```python
# Add to your code
cleaned_df.to_excel('output.xlsx', index=False)
```

## API Reference

### Session State Variables

```python
st.session_state["data_source"]        # "Oracle Database" or "CSV Upload"
st.session_state["uploaded_data"]      # pandas DataFrame or None
st.session_state["uploaded_filename"]  # str or None
st.session_state["profiling_result"]   # dict with 'profile' and 'score'
st.session_state["cleaned_data"]       # pandas DataFrame or None
st.session_state["anomaly_result"]     # dict with anomaly info
```

### Key Functions

```python
# Profiling
profiler = DataProfiler()
profile = profiler.profile(df, name="my_data")
score = profiler.generate_data_health_score(profile)

# Cleaning
cleaner = DataCleaner()
df1 = cleaner.clean_duplicates(df)
df2 = cleaner.handle_nulls(df1, strategy="fill_mean")
df3 = cleaner.normalize_strings(df2)

# Anomaly Detection
detector = AnomalyDetector()
result = detector.detect(df)
```

## Roadmap

Future enhancements for CSV Upload mode:

- [ ] Support for Excel files (.xlsx, .xls)
- [ ] Multiple file upload and comparison
- [ ] Custom cleaning strategies in UI
- [ ] Advanced anomaly detection settings
- [ ] CSV template generator
- [ ] Data validation rules
- [ ] Column type inference and conversion

## Support

For issues or feature requests:

1. Check existing logs in `logs/app.log`
2. Run validation: `python scripts/test_csv_upload.py`
3. Open issue on GitHub: https://github.com/AbieWorks/DataIQ

---

**DataIQ v1.1.0** – Now with CSV Upload Support! 🚀
