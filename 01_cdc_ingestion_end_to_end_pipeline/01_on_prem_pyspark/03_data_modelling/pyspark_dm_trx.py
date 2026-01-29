# =========================================================
# PYSPARK ETL: OLTP (normalized) -> OLAP (star schema)
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("oltp_to_olap_star_schema").getOrCreate()

# -----------------------------
# Config
# -----------------------------
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_USER = "root"
MYSQL_PASSWORD = "your_password"

OLTP_DB = "demo_oltp"
OLAP_DB = "demo_olap"

OLTP_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{OLTP_DB}?useSSL=false&allowPublicKeyRetrieval=true"
OLAP_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{OLAP_DB}?useSSL=false&allowPublicKeyRetrieval=true"

JDBC_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
}

def read_table(url: str, table: str):
    return spark.read.jdbc(url=url, table=table, properties=JDBC_PROPS)

def write_table(df, url: str, table: str, mode: str = "overwrite"):
    (
        df.write
        .mode(mode)
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", JDBC_PROPS["driver"])
        .option("batchsize", "2000")
        .save()
    )

# Deterministic surrogate key approach
# Using a stable hash of the natural key so keys do not change across reruns
def sk_bigint(col_expr):
    # Take first 16 hex chars of sha2 and convert from hex to bigint-like decimal string
    # This keeps the key stable and reproducible for training
    return F.conv(F.substring(F.sha2(col_expr, 256), 1, 16), 16, 10).cast("bigint")

# -----------------------------
# 1) Read OLTP tables
# -----------------------------
customers = read_table(OLTP_URL, "customers")
categories = read_table(OLTP_URL, "categories")
products = read_table(OLTP_URL, "products")
orders = read_table(OLTP_URL, "orders")
order_items = read_table(OLTP_URL, "order_items")

# -----------------------------
# 2) Build Dimensions
# -----------------------------

# dim_customer
dim_customer = (
    customers
    .select(
        F.col("customer_id").cast("int"),
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "created_at"
    )
    .withColumn("customer_key", sk_bigint(F.col("customer_id").cast("string")))
    .select(
        "customer_key",
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "created_at"
    )
)

# dim_product with category denormalized
dim_product = (
    products.alias("p")
    .join(categories.alias("c"), F.col("p.category_id") == F.col("c.category_id"), "left")
    .select(
        F.col("p.product_id").cast("int").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.category_id").cast("int").alias("category_id"),
        F.col("c.category_name").alias("category_name"),
        F.col("p.list_price").cast("decimal(10,2)").alias("list_price"),
        F.col("p.is_active").cast("int").alias("is_active"),
        F.col("p.created_at").alias("created_at"),
    )
    .withColumn("product_key", sk_bigint(F.col("product_id").cast("string")))
    .select(
        "product_key",
        "product_id",
        "product_name",
        "category_id",
        "category_name",
        "list_price",
        "is_active",
        "created_at"
    )
)

# dim_date derived from order_ts range
min_max_dates = orders.select(
    F.min(F.to_date("order_ts")).alias("min_date"),
    F.max(F.to_date("order_ts")).alias("max_date")
).collect()[0]

min_date = min_max_dates["min_date"]
max_date = min_max_dates["max_date"]

# Generate date sequence
dim_date = (
    spark.sql(f"SELECT sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day) AS dts")
    .select(F.explode("dts").alias("full_date"))
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year_num", F.year("full_date").cast("int"))
    .withColumn("month_num", F.month("full_date").cast("int"))
    .withColumn("day_num", F.dayofmonth("full_date").cast("int"))
    .withColumn("day_of_week", F.date_format("full_date", "u").cast("int"))  # 1=Mon .. 7=Sun
    .withColumn("day_name", F.date_format("full_date", "EEE"))
    .select("date_key", "full_date", "year_num", "month_num", "day_num", "day_of_week", "day_name")
)

# -----------------------------
# 3) Build Fact at order line grain
# -----------------------------
# Fact grain: one row per (order_id, line_number)
# Measures: quantity, unit_price, discount_amount, gross_amount, net_amount

fact_base = (
    order_items.alias("oi")
    .join(orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "inner")
    .select(
        F.col("oi.order_id").cast("int").alias("order_id"),
        F.col("oi.line_number").cast("int").alias("line_number"),
        F.col("o.customer_id").cast("int").alias("customer_id"),
        F.col("oi.product_id").cast("int").alias("product_id"),
        F.to_date("o.order_ts").alias("order_date"),
        F.col("o.order_status").alias("order_status"),
        F.col("oi.quantity").cast("int").alias("quantity"),
        F.col("oi.unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("oi.discount_amount").cast("decimal(10,2)").alias("discount_amount")
    )
)

fact_sales = (
    fact_base
    .withColumn("customer_key", sk_bigint(F.col("customer_id").cast("string")))
    .withColumn("product_key", sk_bigint(F.col("product_id").cast("string")))
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("gross_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(10,2)"))
    .withColumn("net_amount", (F.col("quantity") * F.col("unit_price") - F.col("discount_amount")).cast("decimal(10,2)"))
    .select(
        "order_id",
        "line_number",
        "customer_key",
        "product_key",
        "date_key",
        "order_status",
        "quantity",
        "unit_price",
        "discount_amount",
        "gross_amount",
        "net_amount"
    )
)

# Optional teaching rule
# You can choose to exclude cancelled orders from analytics fact
# For teaching, keep them and show how filters work
# fact_sales = fact_sales.filter(F.col("order_status") != F.lit("CANCELLED"))

# -----------------------------
# 4) Write to OLAP schema
# -----------------------------
# For training simplicity: overwrite tables each run
# In production: use upsert and SCD patterns

write_table(dim_customer, OLAP_URL, "dim_customer", mode="overwrite")
write_table(dim_product, OLAP_URL, "dim_product", mode="overwrite")
write_table(dim_date, OLAP_URL, "dim_date", mode="overwrite")
write_table(fact_sales, OLAP_URL, "fact_sales", mode="overwrite")

# -----------------------------
# 5) Validation queries in Spark
# -----------------------------
# Basic row counts
print("dim_customer count:", dim_customer.count())
print("dim_product count:", dim_product.count())
print("dim_date count:", dim_date.count())
print("fact_sales count:", fact_sales.count())

# Revenue by city example (star schema style)
rev_by_city = (
    fact_sales.alias("f")
    .join(dim_customer.alias("c"), F.col("f.customer_key") == F.col("c.customer_key"), "inner")
    .groupBy("c.city")
    .agg(F.sum("f.net_amount").alias("total_net_revenue"))
    .orderBy(F.desc("total_net_revenue"))
)

rev_by_city.show(truncate=False)
