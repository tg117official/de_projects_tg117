# Start PySpark Shell with :
# pyspark --packages mysql:mysql-connector-java:8.0.28


# -----------------------------
# 1) Connection configs
# -----------------------------
mysql_url = "jdbc:mysql://localhost:3306/inventory"
mysql_props = {
  "user": "root",
  "password": "root",
  "driver": "com.mysql.cj.jdbc.Driver"
}

# Output folder (local)
out_path = r"file:///C:/tmp/bronze/customers_delta"   # Windows example
# out_path = "file:///tmp/bronze/customers_delta"    # Linux/Mac example

# -----------------------------
# 2) Read last bookmark from MySQL cdc_tracking
# -----------------------------
cdc_df = (spark.read.jdbc(url=mysql_url, table="cdc_tracking", properties=mysql_props)
          .filter("table_name = 'customers'")
          .select("lastmodified"))

last_ts = cdc_df.withColumn('lastmodified', cdc_df.lastmodified.cast('string')).rdd.collect()[0][0]   # Python datetime object
last_ts_str = str(last_ts)         # "YYYY-MM-DD HH:MM:SS" usually

print("Last bookmark:", last_ts_str)

# -----------------------------
# 3) Read only changed rows from customers
#    (IMPORTANT: use a query via 'dbtable' with an alias)
# -----------------------------
customers_query = f"(SELECT * FROM customers WHERE lastmodified > '{last_ts_str}') AS t"

inc_df = spark.read.jdbc(url=mysql_url, table=customers_query, properties=mysql_props)

print("New/changed rows:", inc_df.count())
inc_df.show(20, truncate=False)

# -----------------------------
# 4) Write incremental data to local filesystem
# -----------------------------
if inc_df.count() > 0:
    (inc_df
     .coalesce(1)  # single file for demo; remove in real production
     .write.mode("append")
     .option("header", "true")
     .csv(out_path))

    # -----------------------------
    # 5) Update bookmark (max lastmodified of extracted rows)
    # -----------------------------
    max_ts_df = inc_df.agg({"lastmodified": "max"})
    max_ts_df = max_ts_df.withColumnRenamed("max(lastmodified)", "max_lm")
    max_ts_df = max_ts_df.withColumn("max_lm", max_ts_df.max_lm.cast('string'))
    max_ts = max_ts_df.rdd.collect()[0][0]
    print("New max lastmodified:", max_ts)

    # Update using a direct JDBC statement via Java (no extra python libs)
    jvm = spark._sc._gateway.jvm
    DriverManager = jvm.java.sql.DriverManager

    conn = DriverManager.getConnection(mysql_url, mysql_props["user"], mysql_props["password"])
    stmt = conn.createStatement()

    update_sql = f"""
      UPDATE cdc_tracking
      SET lastmodified = '{max_ts}'
      WHERE table_name = 'customers'
    """
    stmt.executeUpdate(update_sql)

    stmt.close()
    conn.close()

    print("Bookmark updated in cdc_tracking.")
else:
    print("No new changes. Bookmark not updated.")
