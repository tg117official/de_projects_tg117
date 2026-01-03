import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# ----------------------------
# CONFIG (EDIT THESE)
# ----------------------------
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DB   = "your_db"
MYSQL_USER = "root"
MYSQL_PASS = "root_password"

# Source table + CDC column
SOURCE_TABLE = "orders"              # change this
CDC_COLUMN   = "last_modified"       # must exist in SOURCE_TABLE (TIMESTAMP/DATETIME recommended)

# Local output path
OUTPUT_PATH = r"file:///C:/data/pyspark_output/orders"   # Windows example
# OUTPUT_PATH = "file:///home/ubuntu/pyspark_output/orders"  # Linux example

# Tracking table name
TRACKING_TABLE = "cdc_tracking"

# Output format: "parquet" or "csv"
OUTPUT_FORMAT = "parquet"
# If csv:
CSV_HEADER = True


# ----------------------------
# Spark + JDBC helpers
# ----------------------------
def mysql_url():
    # You can add extra options if needed:
    # useSSL=false, serverTimezone=UTC, etc.
    return f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false"

def jdbc_props():
    return {
        "user": MYSQL_USER,
        "password": MYSQL_PASS,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

def read_mysql_table(spark, table_name: str):
    return (spark.read
            .format("jdbc")
            .option("url", mysql_url())
            .option("dbtable", table_name)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASS)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load())

def read_mysql_query(spark, query: str):
    # Spark JDBC requires query to be wrapped + aliased
    wrapped = f"({query}) AS t"
    return (spark.read
            .format("jdbc")
            .option("url", mysql_url())
            .option("dbtable", wrapped)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASS)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load())

def overwrite_mysql_table(df, table_name: str):
    (df.write
      .format("jdbc")
      .option("url", mysql_url())
      .option("dbtable", table_name)
      .option("user", MYSQL_USER)
      .option("password", MYSQL_PASS)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .mode("overwrite")
      .save())


# ----------------------------
# CDC Tracking logic (OVERWRITE table each run)
# ----------------------------
TRACKING_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("last_value", StringType(), True),     # store timestamp as string (e.g., "2026-01-03 10:15:00")
    StructField("updated_at", TimestampType(), True)
])

def load_tracking_df(spark):
    """
    Try reading tracking table.
    If it doesn't exist (or read fails), return empty DF with schema.
    """
    try:
        df = read_mysql_table(spark, TRACKING_TABLE) \
                .select("table_name", "last_value", "updated_at")
        return df
    except Exception:
        return spark.createDataFrame([], TRACKING_SCHEMA)

def get_last_value_for_table(tracking_df, table_name: str):
    row = (tracking_df
           .filter(F.col("table_name") == table_name)
           .select("last_value")
           .limit(1)
           .collect())
    if not row:
        return None
    return row[0]["last_value"]

def upsert_tracking_overwrite(spark, tracking_df, table_name: str, new_last_value: str):
    """
    Build a new tracking DF in Spark, then overwrite the entire tracking table in MySQL.
    (No UPDATE statements.)
    """
    now_ts = datetime.now()

    # Remove existing entry for the table_name
    remaining = tracking_df.filter(F.col("table_name") != table_name)

    # Add new entry
    new_row = spark.createDataFrame(
        [(table_name, new_last_value, now_ts)],
        schema=TRACKING_SCHEMA
    )

    final_df = remaining.unionByName(new_row)

    # Overwrite MySQL tracking table entirely
    overwrite_mysql_table(final_df, TRACKING_TABLE)

    return final_df


# ----------------------------
# Main incremental extraction
# ----------------------------
def main():
    spark = (SparkSession.builder
             .appName("MySQL_to_Local_Incremental_CDC")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # 1) Load tracking
    tracking_df = load_tracking_df(spark)

    # 2) Get last processed value
    last_val = get_last_value_for_table(tracking_df, SOURCE_TABLE)

    # 3) Build incremental query
    if last_val is None:
        print(f"[INFO] No tracking found for '{SOURCE_TABLE}'. Doing FULL load.")
        query = f"SELECT * FROM {SOURCE_TABLE}"
    else:
        print(f"[INFO] Found last_value='{last_val}' for '{SOURCE_TABLE}'. Doing INCREMENTAL load.")
        # IMPORTANT: assumes CDC_COLUMN is comparable with this string (TIMESTAMP/DATETIME recommended)
        query = f"""
            SELECT *
            FROM {SOURCE_TABLE}
            WHERE {CDC_COLUMN} > '{last_val}'
        """

    # 4) Read data
    df = read_mysql_query(spark, query)

    # Add ingestion metadata (useful for local writes)
    df = df.withColumn("ingested_at", F.current_timestamp()) \
           .withColumn("ingest_date", F.to_date(F.col("ingested_at")))

    count = df.count()
    print(f"[INFO] Rows fetched = {count}")

    # 5) Write to local filesystem
    if count > 0:
        if OUTPUT_FORMAT.lower() == "parquet":
            (df.write
              .mode("append")
              .partitionBy("ingest_date")
              .parquet(OUTPUT_PATH))
        elif OUTPUT_FORMAT.lower() == "csv":
            (df.write
              .mode("append")
              .option("header", str(CSV_HEADER).lower())
              .partitionBy("ingest_date")
              .csv(OUTPUT_PATH))
        else:
            raise ValueError("OUTPUT_FORMAT must be 'parquet' or 'csv'")

        print(f"[INFO] Data written to: {OUTPUT_PATH}")
    else:
        print("[INFO] No new rows. Skipping local write.")

    # 6) Compute new max CDC value and overwrite tracking table
    if count > 0:
        max_row = df.agg(F.max(F.col(CDC_COLUMN)).alias("mx")).collect()[0]
        new_last_value = max_row["mx"]

        # Convert timestamp to string for tracking table
        # If mx is already a python datetime, str(mx) is ok (YYYY-MM-DD HH:MM:SS)
        new_last_value_str = str(new_last_value)

        print(f"[INFO] New last_value to store = {new_last_value_str}")

        tracking_df = upsert_tracking_overwrite(
            spark=spark,
            tracking_df=tracking_df,
            table_name=SOURCE_TABLE,
            new_last_value=new_last_value_str
        )
        print("[INFO] Tracking table overwritten successfully.")
    else:
        print("[INFO] Tracking not updated because no rows were processed.")

    spark.stop()


if __name__ == "__main__":
    main()
