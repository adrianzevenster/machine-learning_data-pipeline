#!/usr/bin/env python
"""
Test script to debug MySQL connection and data issues
Run this inside the Docker container to test connectivity
"""

from pyspark.sql import SparkSession
import sys
import os

def test_mysql_connection():
    print("="*60)
    print("MYSQL CONNECTION TEST")
    print("="*60)

    spark = SparkSession.builder \
        .appName("TestConnection") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Connection parameters
    mysql_host = os.getenv("MYSQL_HOST", "local-mysql")
    mysql_db = os.getenv("MYSQL_DATABASE", "RawData")
    jdbc_url = f"jdbc:mysql://{mysql_host}:3306/{mysql_db}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    props = {
        "user": os.getenv("MYSQL_USER", "spark"),
        "password": os.getenv("MYSQL_PASSWORD", "sparkpw"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    tests_passed = 0
    tests_failed = 0

    # Test 1: Check raw data table
    print("\n1. Testing RAW DATA table (DP_CDR_Data)...")
    try:
        raw_count_query = "(SELECT COUNT(*) as cnt FROM DP_CDR_Data) t"
        df = spark.read.jdbc(url=jdbc_url, table=raw_count_query, properties=props)
        count = df.collect()[0]['cnt']
        print(f"   ✅ SUCCESS: Found {count:,} rows in DP_CDR_Data")
        tests_passed += 1

        # Get sample
        if count > 0:
            sample_query = "(SELECT * FROM DP_CDR_Data LIMIT 5) t"
            sample_df = spark.read.jdbc(url=jdbc_url, table=sample_query, properties=props)
            print("\n   Sample data:")
            sample_df.show(truncate=False)

            # Check columns
            print(f"\n   Columns: {sample_df.columns}")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        tests_failed += 1

    # Test 2: Check processed data table
    print("\n2. Testing PROCESSED DATA table (Processed_Data)...")
    try:
        proc_count_query = "(SELECT COUNT(*) as cnt FROM Processed_Data) t"
        df = spark.read.jdbc(url=jdbc_url, table=proc_count_query, properties=props)
        count = df.collect()[0]['cnt']
        print(f"   ✅ SUCCESS: Found {count:,} rows in Processed_Data")
        tests_passed += 1

        if count > 0:
            # Get sample
            sample_query = "(SELECT * FROM Processed_Data LIMIT 5) t"
            sample_df = spark.read.jdbc(url=jdbc_url, table=sample_query, properties=props)
            print("\n   Sample data:")
            sample_df.show(truncate=False)

            # Check label distribution
            label_query = "(SELECT M_TENURE_CHURN, COUNT(*) as cnt FROM Processed_Data GROUP BY M_TENURE_CHURN) t"
            label_df = spark.read.jdbc(url=jdbc_url, table=label_query, properties=props)
            print("\n   Label distribution:")
            label_df.show()
        else:
            print("   ⚠️  Table is empty - run PySparkAnalysis.py first")

    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        tests_failed += 1

    # Test 3: Check date formats in raw data
    print("\n3. Testing DATE formats in raw data...")
    try:
        date_query = """(SELECT 
            MIN(DP_DATE) as min_date,
            MAX(DP_DATE) as max_date,
            COUNT(DISTINCT DATE(DP_DATE)) as unique_dates
            FROM DP_CDR_Data) t"""
        df = spark.read.jdbc(url=jdbc_url, table=date_query, properties=props)
        result = df.collect()[0]
        print(f"   Date range: {result['min_date']} to {result['max_date']}")
        print(f"   Unique dates: {result['unique_dates']}")
        print(f"   ✅ SUCCESS")
        tests_passed += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        tests_failed += 1

    # Summary
    print("\n" + "="*60)
    print(f"RESULTS: {tests_passed} passed, {tests_failed} failed")
    if tests_failed == 0:
        print("✅ All tests passed! Connection is working.")
    else:
        print("❌ Some tests failed. Check the errors above.")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    test_mysql_connection()
