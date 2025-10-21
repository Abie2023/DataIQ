**Test Scenarios**:

1. **CSV Upload**

   - Switch to "CSV Upload" mode
   - Upload `sample_data/test_data.csv`
   - Click "Run Profiling" → Health score displayed
   - Click "Run Cleaning" → Cleaned data preview
   - Click "Detect Anomalies" → Chart rendered
   - Download cleaned data → CSV exported

2. **Oracle Mode - Offline**

   - Select "Oracle Database" mode
   - Oracle disconnected → Error message displayed
   - Buttons disabled → Cannot run profiling
   - Graceful degradation → No crashes

3. **Mode Switching**
   - Toggle between modes → Cache cleared
   - No stale data → Clean slate each time
   - Fast switching → No lag

### Example 1: Quick CSV Analysis

```powershell
# 1. Launch dashboard
streamlit run dashboard/app.py

# 2. In browser:
# - Select "CSV Upload" mode
# - Upload your CSV file
# - Click "Run Profiling"
# - View health score and metrics
# - Click "Download Profile as CSV"
```

### Example 2: Data Cleaning Workflow

```powershell
# 1. Upload CSV with quality issues
# 2. Run profiling to see issues
# 3. Click "Run Cleaning"
# 4. Compare before/after metrics
# 5. Download cleaned data
# 6. Import into your system
```

### Example 3: Anomaly Detection

```powershell
# 1. Upload CSV with numeric columns
# 2. Optional: Run cleaning first
# 3. Click "Detect Anomalies"
# 4. View chart showing outliers per column
# 5. Export chart for reporting
```

## 🔍 Use Cases

### 1. Quick Data Quality Check

- Upload customer data extract
- Get instant health score
- Identify quality issues
- No database setup needed

### 2. Offline Analysis

- Work without VPN/network
- Analyze local CSV files
- Perfect for travel/remote work
- No dependency on infrastructure

### 3. Pre-Production Testing

- Test DataIQ features before Oracle setup
- Validate cleaning logic
- Prototype anomaly detection rules
- Train users without database access

### 4. Ad-Hoc Analysis

- Analyze data from non-Oracle sources
- Quick profiling for Excel exports
- One-time cleaning tasks
- Exploratory data analysis

### Profiling Metrics Provided

- **Overall Metrics**:

  - Total rows
  - Total columns
  - Missing values count
  - Duplicate rows count
  - Data health score (0-100)

- **Per-Column Metrics**:
  - Column name and data type
  - Null count and percentage
  - Unique value count
  - Type mismatch count
  - Numeric statistics (mean, median, std, min, max)

### Cleaning Operations

- **Duplicate Removal**: Drops exact duplicate rows
- **Null Handling**: Fills nulls with mean (numeric) or mode (categorical)
- **String Normalization**: Strips whitespace + NFKC normalization

### Anomaly Detection

- **Algorithm**: IsolationForest (unsupervised ML)
- **Contamination**: 0.1 (10% expected outliers)
- **Output**: Bar chart + anomaly counts per column


**Getting Started**:

1. Install DataIQ: `pip install -r requirements.txt`
2. Launch dashboard: `streamlit run dashboard/app.py`
3. Select "CSV Upload" mode
4. Upload `sample_data/test_data.csv`
5. Click action buttons to explore features
6. (Optional) Oracle XE Database for local instance required

### Technologies

- **Streamlit**: Web UI framework
- **pandas**: Data manipulation
- **scikit-learn**: Machine learning (IsolationForest)
- **python-oracledb**: Oracle connectivity

## 📄 License

DataIQ is released under the MIT License. See LICENSE file for details.