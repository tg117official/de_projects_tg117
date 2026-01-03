

# ============================================================
# PySpark Shell Script: SCD Type 1, Type 2, Type 3 (run_1 -> run_2)
# Copy-paste into pyspark shell (pyspark)
# ============================================================

from pyspark.sql import functions as F, Window, SparkSession

spark = SparkSession.builder.appName("DemoSCD").getOrCreate()
# ------------------------------------------------------------
# 0) CREATE SAMPLE "PIPELINE RUNS" (run_1 and run_2)
#    - run_1: initial load
#    - run_2: incremental load (updates + new records)
# ------------------------------------------------------------

run1 = spark.createDataFrame([
    ("C001","Amit","Shah","amit@gmail.com","Pune","1111",True,"2026-01-03 09:01:31"),
    ("C002","Neha","Patil","neha@gmail.com","Mumbai","2222",False,"2026-01-03 09:01:31"),
    ("C003","Ravi","Kale","ravi@gmail.com","Nagpur","3333",True,"2026-01-03 09:01:31"),
], ["customer_id","first_name","last_name","email","city","phone","is_active","last_modified_ts"]) \
.withColumn("last_modified_ts", F.to_timestamp("last_modified_ts"))

run2 = spark.createDataFrame([
    ("C001","Amit","Shah","amit@gmail.com","Pune - Hinjewadi","9999",True,"2026-01-03 10:15:00"), # changed city+phone
    ("C002","Neha","Patil","neha@gmail.com","Mumbai","2222",True,"2026-01-03 10:15:00"),          # changed is_active
    ("C004","Sita","More","sita@gmail.com","Nashik","4444",True,"2026-01-03 10:15:00"),           # new
    ("C005","Vikas","Jadhav","vikas@gmail.com","Pune","5555",True,"2026-01-03 10:15:00"),         # new
], ["customer_id","first_name","last_name","email","city","phone","is_active","last_modified_ts"]) \
.withColumn("last_modified_ts", F.to_timestamp("last_modified_ts"))

print("\n=== RUN 1 (Initial Load) ===")
run1.orderBy("customer_id").show(truncate=False)

print("\n=== RUN 2 (Incremental Changes + New Records) ===")
run2.orderBy("customer_id").show(truncate=False)


# ============================================================
# 1) SCD TYPE 1
# ============================================================
# What it means:
# - Keep ONLY ONE row per customer_id
# - Always keep LATEST attributes (overwrite old values)
#
# Shell-friendly approach:
# - Union old dim + new run
# - For each customer_id, pick the latest row using last_modified_ts
# ============================================================

print("\n==================== SCD TYPE 1 ====================")

# Step 1A: Initial dimension from run1
dim1 = run1
print("\nType 1 after run1:")
dim1.orderBy("customer_id").show(truncate=False)

# Step 1B: Apply run2 updates
unioned = dim1.unionByName(run2)

w = Window.partitionBy("customer_id").orderBy(F.col("last_modified_ts").desc())
dim1 = (unioned
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn"))

print("\nType 1 after run2 (latest per customer_id):")
dim1.orderBy("customer_id").show(truncate=False)


# ============================================================
# 2) SCD TYPE 2
# ============================================================
# What it means:
# - Keep HISTORY of changes
# - When a tracked attribute changes, do:
#   1) Expire old current row (set effective_to, is_current=false)
#   2) Insert a new row as current (is_current=true, version+1)
#
# We'll track changes for: city, phone, is_active
# (You can add email/first_name/last_name too if you want)
# ============================================================

print("\n==================== SCD TYPE 2 ====================")

tracked_cols = ["city", "phone", "is_active"]
far_future = F.to_timestamp(F.lit("9999-12-31 00:00:00"))

# Step 2A: Initial Type2 dimension from run1
dim2 = (run1
        .withColumn("effective_from", F.col("last_modified_ts"))
        .withColumn("effective_to", far_future)
        .withColumn("is_current", F.lit(True))
        .withColumn("version", F.lit(1)))

print("\nType 2 after run1:")
dim2.orderBy("customer_id","version").show(truncate=False)

# Step 2B: Identify which existing customers changed in run2
current = dim2.filter("is_current = true").alias("c")
s = run2.alias("s")

j = s.join(current, on="customer_id", how="left")

# Build change condition: (existing row) AND (any tracked column differs)
change_cond = None
for col in tracked_cols:
    cond = (F.col(f"s.{col}") != F.col(f"c.{col}"))
    change_cond = cond if change_cond is None else (change_cond | cond)

changed_keys = (j
    .filter(F.col("c.customer_id").isNotNull() & change_cond)
    .select("customer_id").distinct())

print("\nChanged customer_ids (in run2):")
changed_keys.show()

# Step 2C: Expire old current rows for changed customers
expired = (current
    .join(changed_keys, "customer_id", "inner")
    # For demo: expire at processing time. (Optionally: use run2.last_modified_ts)
    .withColumn("effective_to", F.current_timestamp())
    .withColumn("is_current", F.lit(False)))

print("\nExpired old rows:")
expired.orderBy("customer_id").show(truncate=False)

# Step 2D: Keep unchanged current rows
unchanged_current = current.join(changed_keys, "customer_id", "left_anti")

# Step 2E: Insert NEW versions for changed customers
changed_rows = (run2
    .join(changed_keys, "customer_id", "inner")
    .join(current.select("customer_id","version"), "customer_id", "left")
    .withColumn("version", F.col("version") + F.lit(1))
    .withColumn("effective_from", F.col("last_modified_ts"))
    .withColumn("effective_to", far_future)
    .withColumn("is_current", F.lit(True)))

# Step 2F: Insert brand NEW customers (not present in current dim2)
new_keys = run2.join(current.select("customer_id"), "customer_id", "left_anti")
new_inserts = (new_keys
    .withColumn("version", F.lit(1))
    .withColumn("effective_from", F.col("last_modified_ts"))
    .withColumn("effective_to", far_future)
    .withColumn("is_current", F.lit(True)))

# Step 2G: Rebuild the dimension:
# history (already expired old versions) + unchanged current + newly expired + new current versions + new inserts
history = dim2.filter("is_current = false")

dim2 = (history
        .unionByName(unchanged_current.select(dim2.columns))
        .unionByName(expired.select(dim2.columns))
        .unionByName(changed_rows.select(dim2.columns))
        .unionByName(new_inserts.select(dim2.columns)))

print("\nType 2 after run2 (history preserved):")
dim2.orderBy("customer_id","version").show(truncate=False)


# ============================================================
# 3) SCD TYPE 3
# ============================================================
# What it means:
# - Keep ONE row per key (like Type 1)
# - But store "previous value" for selected attributes
#
# We'll track Type3 for city only:
#   city_current, city_previous
#
# Rule:
# - If city changes in new run, move old city_current -> city_previous
# ============================================================

print("\n==================== SCD TYPE 3 ====================")

# Step 3A: Initial Type3 dimension from run1
dim3 = (run1
    .select(
        "customer_id","first_name","last_name","email","phone","is_active","last_modified_ts",
        F.col("city").alias("city_current")
    )
    .withColumn("city_previous", F.lit(None).cast("string"))
)

print("\nType 3 after run1:")
dim3.orderBy("customer_id").show(truncate=False)

# Step 3B: Merge run2 into dim3
s3 = (run2
    .select(
        "customer_id","first_name","last_name","email","phone","is_active","last_modified_ts",
        F.col("city").alias("new_city")
    ).alias("s")
)

d3 = dim3.alias("d")

joined = s3.join(d3, "customer_id", "full")

dim3 = (joined
    .select(
        F.coalesce(F.col("s.customer_id"), F.col("d.customer_id")).alias("customer_id"),

        # Overwrite-like for normal columns (take staging if present else keep old)
        F.coalesce(F.col("s.first_name"), F.col("d.first_name")).alias("first_name"),
        F.coalesce(F.col("s.last_name"), F.col("d.last_name")).alias("last_name"),
        F.coalesce(F.col("s.email"), F.col("d.email")).alias("email"),
        F.coalesce(F.col("s.phone"), F.col("d.phone")).alias("phone"),
        F.coalesce(F.col("s.is_active"), F.col("d.is_active")).alias("is_active"),
        F.coalesce(F.col("s.last_modified_ts"), F.col("d.last_modified_ts")).alias("last_modified_ts"),

        # city_current: take new_city if present else old city_current
        F.coalesce(F.col("s.new_city"), F.col("d.city_current")).alias("city_current"),

        # city_previous logic:
        # - New key => null
        # - No new data => keep existing city_previous
        # - If city changed => old city_current becomes city_previous
        # - else keep existing city_previous
        F.when(F.col("d.customer_id").isNull(), F.lit(None).cast("string")) \
         .when(F.col("s.customer_id").isNull(), F.col("d.city_previous")) \
         .when(F.col("s.new_city") != F.col("d.city_current"), F.col("d.city_current")) \
         .otherwise(F.col("d.city_previous")) \
         .alias("city_previous")
    )
)

print("\nType 3 after run2 (current + previous city):")
dim3.orderBy("customer_id").show(truncate=False)


print("\n==================== DONE ====================")
print("SCD Type 1 rows:", dim1.count())
print("SCD Type 2 rows:", dim2.count())
print("SCD Type 3 rows:", dim3.count())
