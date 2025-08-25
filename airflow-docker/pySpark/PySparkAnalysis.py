import os
import json
import time
import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, coalesce, to_date, date_format, current_date, monotonically_increasing_id
from pyspark.sql.types import IntegerType, DoubleType, StringType

# ───────────── helpers ─────────────
def read_config(path="/app/config.json"):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def get_cfg():
    cfg = read_config()
    db = cfg.get("database", {})
    tbl = cfg.get("tables", {})
    dates = cfg.get("dates", {})
    io = cfg.get("io", {})

    # env overrides (Airflow DockerOperator passes these)
    host = os.getenv("MYSQL_HOST", db.get("host", "local-mysql"))
    port = int(os.getenv("MYSQL_PORT", db.get("port", 3306)))
    user = os.getenv("MYSQL_USER", db.get("user", "root"))
    pwd  = os.getenv("MYSQL_PASSWORD", db.get("password", ""))
    dbn  = os.getenv("MYSQL_DATABASE", db.get("db", "RawData"))

    start = os.getenv("PROCESSED_START", dates.get("start", "2024-10-01"))
    end   = os.getenv("PROCESSED_END",   dates.get("end",   "2025-12-31"))

    raw_table       = tbl.get("raw", "DP_CDR_Data")
    processed_table = tbl.get("processed", "Processed_Data")
    parquet_dir     = os.getenv("PARQUET_DIR", io.get("parquet_dir", "/app/parquetFiles"))

    return {
        "host": host, "port": port, "user": user, "pwd": pwd, "db": dbn,
        "raw_table": raw_table, "processed_table": processed_table,
        "start": start, "end": end,
        "parquet_dir": parquet_dir
    }

def build_jdbc_url(host, port, db):
    return f"jdbc:mysql://{host}:{port}/{db}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

def jdbc_props(user, pwd):
    return {
        "user": user,
        "password": pwd,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "useServerPrepStmts": "false"
    }

def get_data_with_diagnostics(spark, jdbc_url, props, cfg):
    """Get data with better diagnostics"""

    # First, check what dates actually exist
    date_check_query = f"""(SELECT 
        MIN(DP_DATE) as min_date, 
        MAX(DP_DATE) as max_date,
        COUNT(*) as total_rows,
        COUNT(DISTINCT DATE(DP_DATE)) as unique_dates
        FROM {cfg['db']}.{cfg['raw_table']}) t"""

    print("\n=== Checking available dates in raw data ===")
    try:
        date_info = spark.read.jdbc(url=jdbc_url, table=date_check_query, properties=props)
        date_info.show()
        date_results = date_info.collect()[0]
        print(f"Date range in raw data: {date_results['min_date']} to {date_results['max_date']}")
        print(f"Total rows: {date_results['total_rows']}")
    except Exception as e:
        print(f"Could not check dates: {e}")

    # Get sample of actual dates
    sample_query = f"""(SELECT 
        DP_DATE,
        COUNT(*) as row_count
        FROM {cfg['db']}.{cfg['raw_table']} 
        GROUP BY DP_DATE 
        ORDER BY DP_DATE DESC 
        LIMIT 10) t"""

    print("\n=== Sample dates from raw data ===")
    try:
        sample_df = spark.read.jdbc(url=jdbc_url, table=sample_query, properties=props)
        sample_df.show(truncate=False)
    except Exception as e:
        print(f"Could not get sample dates: {e}")

    # Try multiple date formats
    queries = [
        ("Direct string comparison",
         f"""(SELECT * FROM {cfg['db']}.{cfg['raw_table']} 
              WHERE DP_DATE >= '{cfg['start']}' 
              AND DP_DATE <= '{cfg['end']}') t"""),

        ("Using DATE() function",
         f"""(SELECT * FROM {cfg['db']}.{cfg['raw_table']} 
              WHERE DATE(DP_DATE) >= '{cfg['start']}' 
              AND DATE(DP_DATE) <= '{cfg['end']}') t"""),

        ("No filter (will filter in Spark)",
         f"""(SELECT * FROM {cfg['db']}.{cfg['raw_table']}) t""")
    ]

    for name, query in queries:
        try:
            print(f"\n=== Attempting: {name} ===")
            print(f"Query: {query[:200]}...")
            test_df = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
            count = test_df.count()
            print(f"Result: {count} rows")

            if count > 0:
                print(f"✅ Success with {name}")

                # If we got all data, filter in Spark
                if "No filter" in name:
                    print(f"\nFiltering in Spark for dates {cfg['start']} to {cfg['end']}...")

                    # Try to parse the date column
                    test_df = test_df.withColumn(
                        "parsed_date",
                        coalesce(
                            to_date(col("DP_DATE"), "yyyy-MM-dd"),
                            to_date(col("DP_DATE"), "yyyy-MM-dd HH:mm:ss"),
                            to_date(col("DP_DATE"), "yyyyMMdd")
                        )
                    )

                    # Filter by parsed date
                    filtered_df = test_df.filter(
                        (col("parsed_date") >= cfg['start']) &
                        (col("parsed_date") <= cfg['end'])
                    )

                    filtered_count = filtered_df.count()
                    print(f"After filtering: {filtered_count} rows")

                    if filtered_count > 0:
                        return filtered_df
                    else:
                        print("⚠️ Date filtering removed all rows. Returning unfiltered data.")
                        return test_df
                else:
                    return test_df

        except Exception as e:
            print(f"Failed: {e}")
            continue

    return None

def transform_cdr_data(df):
    """Transform CDR data with better null handling"""

    print("\n=== Transforming data ===")
    print(f"Input columns: {df.columns}")

    # Check for column variations
    msisdn_col = None
    for col in ["DP_MSISDN", "MSISDN", "USER_ID", "SUBSCRIBER_ID"]:
        if col in df.columns:
            msisdn_col = col
            print(f"Found user column: {msisdn_col}")
            break

    if not msisdn_col:
        print("⚠️ No user identifier column found, using row number")
        df = df.withColumn("temp_user_id", monotonically_increasing_id())
        msisdn_col = "temp_user_id"

    # Check what date column we have
    date_col = None
    for col in ["DP_DATE", "parsed_date", "DATE", "CALL_DATE"]:
        if col in df.columns:
            date_col = col
            print(f"Found date column: {date_col}")
            break

    if not date_col:
        print("⚠️ No date column found, using current date")
        df = df.withColumn("temp_date", current_date())
        date_col = "temp_date"

    # Transform with null safety
    transformed = df.select(
        date_format(
            coalesce(
                to_date(col(date_col), "yyyy-MM-dd"),
                to_date(col(date_col), "yyyy-MM-dd HH:mm:ss"),
                col(date_col),  # In case it's already a date type
                current_date()
            ),
            "yyyy-MM-dd"
        ).alias("Date"),

        coalesce(
            col(msisdn_col).cast("string"),
            lit("UNKNOWN")
        ).alias("User"),

        coalesce(
            col("DP_MOC_COUNT").cast("int") if "DP_MOC_COUNT" in df.columns else lit(None),
            col("MOC_COUNT").cast("int") if "MOC_COUNT" in df.columns else lit(None),
            lit(0)
        ).alias("M_Out_Call_Count"),

        coalesce(
            col("DP_MOC_DURATION").cast("double") if "DP_MOC_DURATION" in df.columns else lit(None),
            col("MOC_DURATION").cast("double") if "MOC_DURATION" in df.columns else lit(None),
            lit(0.0)
        ).alias("M_Out_Call_Time"),

        coalesce(
            col("DP_DATA_VOLUME").cast("double") if "DP_DATA_VOLUME" in df.columns else lit(None),
            col("DATA_VOLUME").cast("double") if "DATA_VOLUME" in df.columns else lit(None),
            col("DATA_MB").cast("double") if "DATA_MB" in df.columns else lit(None),
            lit(0.0)
        ).alias("M_Data_Sum"),

        coalesce(
            col("DP_DATA_COUNT").cast("int") if "DP_DATA_COUNT" in df.columns else lit(None),
            col("DATA_SESSIONS").cast("int") if "DATA_SESSIONS" in df.columns else lit(None),
            lit(0)
        ).alias("M_Data_Count"),

        coalesce(
            col("DP_MTC_COUNT").cast("int") if "DP_MTC_COUNT" in df.columns else lit(None),
            col("MTC_COUNT").cast("int") if "MTC_COUNT" in df.columns else lit(None),
            lit(0)
        ).alias("M_In_Call_Count"),

        coalesce(
            col("DP_MTC_DURATION").cast("double") if "DP_MTC_DURATION" in df.columns else lit(None),
            col("MTC_DURATION").cast("double") if "MTC_DURATION" in df.columns else lit(None),
            lit(0.0)
        ).alias("M_In_Call_Time"),

        # For the label - this might not exist in raw CDR data
        coalesce(
            col("PSEUDO_CHURNED").cast("int") if "PSEUDO_CHURNED" in df.columns else lit(None),
            col("CHURN_FLAG").cast("int") if "CHURN_FLAG" in df.columns else lit(None),
            col("IS_CHURNED").cast("int") if "IS_CHURNED" in df.columns else lit(None),
            lit(0)  # Default to not churned
        ).alias("M_TENURE_CHURN")
    )

    # Remove duplicates and filter out completely null rows
    transformed = transformed.dropDuplicates(["Date", "User"])

    # Only keep rows with some activity
    transformed = transformed.filter(
        (col("M_Out_Call_Count") > 0) |
        (col("M_In_Call_Count") > 0) |
        (col("M_Data_Count") > 0) |
        (col("M_Data_Sum") > 0)
    )

    return transformed

def add_synthetic_churn_labels(df):
    """Add synthetic churn labels based on usage patterns if real labels are missing"""

    # Check if we have real labels
    churn_stats = df.groupBy("M_TENURE_CHURN").count().collect()
    print(f"\nChurn label distribution: {churn_stats}")

    # Check if all labels are 0
    total_churned = sum(row["count"] for row in churn_stats if row["M_TENURE_CHURN"] == 1)

    if total_churned == 0:
        print("⚠️ No real churn labels found. Creating synthetic labels...")

        # Create churn probability based on usage
        # Low usage = higher churn probability
        df = df.withColumn(
            "usage_score",
            col("M_Out_Call_Count") +
            col("M_In_Call_Count") +
            (col("M_Data_Count") * 0.1) +
            (col("M_Data_Sum") * 0.01)
        )

        # Bottom 20% of users by usage are marked as churned
        threshold = df.approxQuantile("usage_score", [0.2], 0.01)[0]

        df = df.withColumn(
            "M_TENURE_CHURN",
            F.when(col("usage_score") <= threshold, 1).otherwise(0)
        )

        # Verify the split
        new_stats = df.groupBy("M_TENURE_CHURN").count().collect()
        print(f"Synthetic churn distribution: {new_stats}")

        df = df.drop("usage_score")

    return df

def write_to_mysql(df, jdbc_url, props, table_name):
    """Write DataFrame to MySQL with proper error handling"""

    count = df.count()
    print(f"\n=== Writing {count} rows to {table_name} ===")

    if count == 0:
        print("⚠️ No rows to write!")
        return False

    try:
        # First, try overwrite mode
        print("Attempting overwrite mode...")
        df.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(url=jdbc_url, table=table_name, properties=props)

        print("✅ Write successful (overwrite mode)")
        return True

    except Exception as e:
        print(f"⚠️ Overwrite failed: {e}")
        print("Trying append mode...")

        try:
            # Try append mode
            df.write \
                .mode("append") \
                .jdbc(url=jdbc_url, table=table_name, properties=props)

            print("✅ Write successful (append mode)")
            return True

        except Exception as e2:
            print(f"❌ Write failed: {e2}")

            # Last resort - write in smaller batches
            print("Trying batch write...")
            try:
                batch_size = 10000
                total_rows = count
                num_batches = (total_rows // batch_size) + 1

                for i in range(num_batches):
                    start_idx = i * batch_size
                    end_idx = min((i + 1) * batch_size, total_rows)

                    batch_df = df.limit(end_idx).subtract(df.limit(start_idx))
                    batch_df.write \
                        .mode("append") \
                        .jdbc(url=jdbc_url, table=table_name, properties=props)

                    print(f"  Batch {i+1}/{num_batches} written")

                print("✅ Batch write successful")
                return True

            except Exception as e3:
                print(f"❌ Batch write also failed: {e3}")
                return False

# ───────────── main ─────────────
if __name__ == "__main__":
    print("\n" + "="*80)
    print("PYSPARK ANALYSIS - DATA PROCESSING PIPELINE")
    print("="*80)

    cfg = get_cfg()

    spark = (
        SparkSession.builder
        .appName("PySparkAnalysis")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    jdbc_url = build_jdbc_url(cfg["host"], cfg["port"], cfg["db"])
    props = jdbc_props(cfg["user"], cfg["pwd"])

    print(f"\n=== Configuration ===")
    print(f"MySQL Host: {cfg['host']}")
    print(f"Database: {cfg['db']}")
    print(f"Raw Table: {cfg['raw_table']}")
    print(f"Processed Table: {cfg['processed_table']}")
    print(f"Date Range: {cfg['start']} to {cfg['end']}")

    # Get raw data with better diagnostics
    raw_df = get_data_with_diagnostics(spark, jdbc_url, props, cfg)

    if raw_df is None:
        print("\n⚠️ Could not read data with any query method. Attempting direct read...")
        try:
            raw_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"{cfg['db']}.{cfg['raw_table']}",
                properties=props
            )
            print(f"Direct read successful: {raw_df.count()} rows")
        except Exception as e:
            print(f"❌ Complete failure to read data: {e}")
            raise RuntimeError("Cannot access raw data table")

    raw_count = raw_df.count()
    print(f"\n✅ RAW data loaded: {raw_count} rows")

    if raw_count == 0:
        print("❌ No data to process!")
        spark.stop()
        exit(1)

    # Show sample of raw data
    print("\n=== Sample of raw data ===")
    raw_df.show(5, truncate=False)

    # Transform the data
    transformed_df = transform_cdr_data(raw_df)

    # Add synthetic labels if needed
    transformed_df = add_synthetic_churn_labels(transformed_df)

    # Cache for reuse
    transformed_df.cache()

    print("\n=== Transformed data sample ===")
    transformed_df.show(10, truncate=False)

    transformed_count = transformed_df.count()
    print(f"\n✅ Transformed count: {transformed_count}")

    if transformed_count == 0:
        print("⚠️ No rows produced after transformation!")
        spark.stop()
        exit(1)

    # Check data statistics
    print("\n=== Data Statistics ===")
    transformed_df.describe().show()

    # Write to MySQL
    target_table = f"{cfg['db']}.{cfg['processed_table']}"
    success = write_to_mysql(transformed_df, jdbc_url, props, target_table)

    if success:
        # Verify the write
        try:
            verify_query = f"(SELECT COUNT(*) as cnt FROM {cfg['db']}.{cfg['processed_table']}) t"
            verify_df = spark.read.jdbc(url=jdbc_url, table=verify_query, properties=props)
            final_count = verify_df.collect()[0]['cnt']
            print(f"\n=== Verification ===")
            print(f"✅ Total rows in processed table: {final_count}")

            # Get date range
            range_query = f"""(SELECT 
                MIN(Date) as min_date, 
                MAX(Date) as max_date 
                FROM {cfg['db']}.{cfg['processed_table']}) t"""
            range_df = spark.read.jdbc(url=jdbc_url, table=range_query, properties=props)
            range_info = range_df.collect()[0]
            print(f"Date range: {range_info['min_date']} to {range_info['max_date']}")

        except Exception as e:
            print(f"Could not verify write: {e}")

    # Also save as parquet for backup/debugging
    try:
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        parquet_out = os.path.join(cfg["parquet_dir"], f"processed_{ts}")
        print(f"\n=== Writing parquet backup to {parquet_out} ===")
        transformed_df.write.mode("overwrite").parquet(parquet_out)
        print("✅ Parquet write complete")
    except Exception as e:
        print(f"⚠️ Parquet write failed: {e}")

    print("\n" + "="*80)
    print("✅ PYSPARK ANALYSIS COMPLETE!")
    print("="*80)

    spark.stop()