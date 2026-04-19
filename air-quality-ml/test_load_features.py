#!/usr/bin/env python
"""
Script test nhanh để kiểm tra load features
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from air_quality_ml.processing.load_features import (
    add_alert_target_from_pm25,
    load_and_prepare_features,
)
from air_quality_ml.settings import load_base_settings, resolve_path
from air_quality_ml.utils.spark import create_spark_session


def main():
    import os
    
    # Ensure HADOOP_HOME is set before creating Spark session
    if os.name == 'nt':
        if 'HADOOP_HOME' not in os.environ:
            if os.path.exists('C:\\hadoop'):
                os.environ['HADOOP_HOME'] = 'C:\\hadoop'
                print("[INFO] Set HADOOP_HOME to C:\\hadoop")
        
        print(f"[INFO] HADOOP_HOME = {os.environ.get('HADOOP_HOME', 'NOT SET')}")
    
    print("=" * 80)
    print("TEST LOAD FEATURES")
    print("=" * 80)
    
    # Load config
    base_config_path = Path("configs/base.yaml").resolve()
    settings = load_base_settings(base_config_path)
    
    features_path = str(resolve_path(base_config_path.parent, settings.data.features_path))
    
    # Debug info
    print(f"\n[DEBUG] Config file: {base_config_path}")
    print(f"[DEBUG] Config parent: {base_config_path.parent}")
    print(f"[DEBUG] Features path (config): {settings.data.features_path}")
    print(f"[DEBUG] Features path (resolved): {features_path}")
    print(f"[DEBUG] Path exists: {Path(features_path).exists()}")
    
    print(f"\n📂 Features path: {features_path}")
    
    # Create Spark session
    spark = create_spark_session(settings)
    
    try:
        # Load features
        print("\n⏳ Loading features...")
        df = load_and_prepare_features(spark, features_path)
        
        # Stats
        total_rows = df.count()
        total_cols = len(df.columns)
        
        print(f"\n✅ Loaded successfully!")
        print(f"   - Total rows: {total_rows:,}")
        print(f"   - Total columns: {total_cols}")
        
        # Schema
        print("\n" + "=" * 80)
        print("SCHEMA:")
        print("=" * 80)
        df.printSchema()
        
        # Sample data
        print("\n" + "=" * 80)
        print("SAMPLE DATA (5 rows):")
        print("=" * 80)
        df.show(5, truncate=False, vertical=True)
        
        # Check targets
        print("\n" + "=" * 80)
        print("AVAILABLE TARGETS:")
        print("=" * 80)
        target_cols = [c for c in df.columns if c.startswith("target_")]
        for col in sorted(target_cols):
            count = df.filter(df[col].isNotNull()).count()
            print(f"   - {col}: {count:,} non-null values")
        
        # Test add alert target
        print("\n" + "=" * 80)
        print("TEST ADD ALERT TARGET:")
        print("=" * 80)
        
        if "target_pm25_1h" in df.columns:
            df_with_alert = add_alert_target_from_pm25(
                df,
                pm25_target_col="target_pm25_1h",
                alert_target_col="target_alert_1h",
                threshold=35.0
            )
            
            alert_count = df_with_alert.filter(df_with_alert["target_alert_1h"] == 1).count()
            print(f"   ✅ Created target_alert_1h")
            print(f"   - Alert cases (PM2.5 >= 35): {alert_count:,}")
            print(f"   - Alert rate: {alert_count/total_rows*100:.2f}%")
        
        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
