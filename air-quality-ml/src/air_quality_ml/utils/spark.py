from __future__ import annotations

from pyspark.sql import SparkSession

from air_quality_ml.settings import BaseSettings


def create_spark_session(settings: BaseSettings) -> SparkSession:
    import os
    import sys
    
    # Windows-specific Hadoop configuration
    if os.name == 'nt':
        # Set HADOOP_HOME if not already set
        if 'HADOOP_HOME' not in os.environ:
            if os.path.exists('C:\\hadoop'):
                os.environ['HADOOP_HOME'] = 'C:\\hadoop'
                print(f"[INFO] Set HADOOP_HOME to C:\\hadoop")
        
        # Disable Hadoop native library to avoid UnsatisfiedLinkError
        os.environ['HADOOP_OPTS'] = '-Djava.library.path='
        
        # Set PYSPARK_PYTHON to current Python executable
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        print(f"[INFO] Set PYSPARK_PYTHON to {sys.executable}")
    
    builder = (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.sql.session.timeZone", settings.spark.session_timezone)
        .config("spark.sql.shuffle.partitions", str(settings.spark.shuffle_partitions))
    )
    
    # Additional Windows-specific configs to bypass native library issues
    if os.name == 'nt':
        builder = (
            builder
            .config("spark.hadoop.io.native.lib.available", "false")
            # Use RawLocalFileSystem to bypass native Windows file access
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        )
    
    spark = builder.getOrCreate()
    return spark
