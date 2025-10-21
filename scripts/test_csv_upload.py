"""
Test script for DataIQ CSV Upload mode
Tests all CSV upload functionality without requiring Oracle connection
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from dataiq.data_profiler import DataProfiler
from dataiq.data_cleaner import DataCleaner
from dataiq.anomaly_detector import AnomalyDetector

def test_csv_upload_workflow():
    """Test complete CSV upload workflow"""
    print("\n" + "="*60)
    print("DataIQ CSV Upload Mode - Integration Test")
    print("="*60 + "\n")
    
    # Load sample CSV
    csv_path = PROJECT_ROOT / "sample_data" / "test_data.csv"
    if not csv_path.exists():
        print(f"❌ Sample CSV not found: {csv_path}")
        return False
    
    print(f"✅ Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Missing values: {df.isnull().sum().sum()}")
    print(f"   Duplicate rows: {df.duplicated().sum()}")
    
    # Test 1: Data Profiling
    print("\n" + "-"*60)
    print("Test 1: Data Profiling")
    print("-"*60)
    try:
        profiler = DataProfiler()
        profile = profiler.profile(df, name="test_data")
        score = profiler.generate_data_health_score(profile)
        print(f"✅ Profiling complete!")
        print(f"   Health Score: {score:.2f} / 100")
        
        # Profile is a dict with metrics
        print("\n   Sample Metrics:")
        metric_count = 0
        for key, value in profile.items():
            if metric_count < 5:
                print(f"   - {key}: {value}")
                metric_count += 1
        
        print(f"   Total metrics: {len(profile)}")
    except Exception as e:
        print(f"❌ Profiling failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 2: Data Cleaning
    print("\n" + "-"*60)
    print("Test 2: Data Cleaning")
    print("-"*60)
    try:
        cleaner = DataCleaner()
        
        # Step 1: Remove duplicates
        df1 = cleaner.clean_duplicates(df)
        print(f"✅ Duplicates removed: {len(df) - len(df1)}")
        
        # Step 2: Handle nulls
        df2 = cleaner.handle_nulls(df1, strategy="fill_mean")
        nulls_before = df1.isnull().sum().sum()
        nulls_after = df2.isnull().sum().sum()
        print(f"✅ Nulls handled: {nulls_before} → {nulls_after}")
        
        # Step 3: Normalize strings
        df3 = cleaner.normalize_strings(df2)
        print(f"✅ Strings normalized")
        print(f"   Final cleaned rows: {len(df3):,}")
        
        cleaned_df = df3
    except Exception as e:
        print(f"❌ Cleaning failed: {e}")
        return False
    
    # Test 3: Anomaly Detection
    print("\n" + "-"*60)
    print("Test 3: Anomaly Detection")
    print("-"*60)
    try:
        detector = AnomalyDetector()
        result = detector.detect(cleaned_df)
        
        print(f"✅ Anomaly detection complete!")
        if "anomalies_per_column" in result:
            total_anomalies = sum(result["anomalies_per_column"].values())
            print(f"   Total anomalies detected: {total_anomalies}")
            print(f"   Columns analyzed: {len(result['anomalies_per_column'])}")
            
            # Show top 5 columns with most anomalies
            sorted_anomalies = sorted(result["anomalies_per_column"].items(), 
                                     key=lambda x: x[1], reverse=True)
            print("\n   Top 5 columns with anomalies:")
            for col, count in sorted_anomalies[:5]:
                print(f"   - {col}: {count} anomalies")
        
        if "chart_path" in result:
            print(f"   Chart saved: {result['chart_path']}")
    except Exception as e:
        print(f"❌ Anomaly detection failed: {e}")
        return False
    
    # Test 4: Data Export
    print("\n" + "-"*60)
    print("Test 4: Data Export")
    print("-"*60)
    try:
        # Export cleaned data
        output_path = PROJECT_ROOT / "outputs" / "cleaned_data" / "test_cleaned_csv_upload.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned_df.to_csv(output_path, index=False)
        print(f"✅ Cleaned data exported: {output_path}")
        
        # Export profile as DataFrame
        profile_path = PROJECT_ROOT / "outputs" / "profiles" / "test_profile_csv_upload.csv"
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert profile dict to DataFrame
        profile_df = pd.DataFrame(list(profile.items()), columns=['metric', 'value'])
        profile_df.to_csv(profile_path, index=False)
        print(f"✅ Profile exported: {profile_path}")
    except Exception as e:
        print(f"❌ Export failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Summary
    print("\n" + "="*60)
    print("✅ ALL TESTS PASSED - CSV Upload Mode Working!")
    print("="*60)
    print("\nNext Steps:")
    print("1. Run dashboard: streamlit run dashboard/app.py")
    print("2. Select 'CSV Upload' mode in the sidebar")
    print("3. Upload sample_data/test_data.csv")
    print("4. Click the action buttons to test profiling, cleaning, and anomaly detection")
    print("\n")
    
    return True

if __name__ == "__main__":
    success = test_csv_upload_workflow()
    sys.exit(0 if success else 1)
