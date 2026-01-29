# =========================================================
# PYSPARK SHELL RAW STEP BY STEP (No user-defined functions)
# OLTP (MySQL normalized) -> OLAP (MySQL star schema)
#
# Run in pyspark shell:
#   pyspark --jars /path/to/mysql-connector-j-8.x.x.jar
#
# Assumptions:
#   MySQL has demo_oltp and demo_olap databases created
#   demo_oltp tables: customers, categories, products, orders, order_items
#   demo_olap tables: dim_customer, dim_product, dim_date, fact_sales
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# STEP 1: Set MySQL connection info
# -----------------------------
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"

OLTP_DB = "demo_oltp"
OLAP_DB = "demo_olap"

OLTP_URL = "jdbc:mysql://{}:{}/{}?useSSL=false&allowPublicKeyRetrieval=true".format(MYSQL_HOST, MYSQL_PORT, OLTP_DB)
OLAP_URL = "jdbc:mysql://{}:{}/{}?useSSL=false&allowPublicKeyRetrieval=true".format(MYSQL_HOST, MYSQL_PORT, OLAP_DB)

# JDBC read options reused by copy-paste
# In pyspark shell, we use .format("jdbc").options(...).load()
DRIVER = "com.mysql.cj.jdbc.Driver"

# -----------------------------
# STEP 2: Read OLTP tables (raw)
# -----------------------------
customers = (
    spark.read.format("jdbc")
    .option("url", OLTP_URL)
    .option("dbtable", "customers")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .load()
)

categories = (
    spark.read.format("jdbc")
    .option("url", OLTP_URL)
    .option("dbtable", "categories")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .load()
)

products = (
    spark.read.format("jdbc")
    .option("url", OLTP_URL)
    .option("dbtable", "products")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .load()
)

orders = (
    spark.read.format("jdbc")
    .option("url", OLTP_URL)
    .option("dbtable", "orders")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .load()
)

order_items = (
    spark.read.format("jdbc")
    .option("url", OLTP_URL)
    .option("dbtable", "order_items")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .load()
)

# Optional: quick check
print("OLTP counts")
print("customers:", customers.count())
print("categories:", categories.count())
print("products:", products.count())
print("orders:", orders.count())
print("order_items:", order_items.count())

# -----------------------------
# STEP 3: Build dim_customer (raw)
# Surrogate key: deterministic from customer_id using sha2
# -----------------------------
dim_customer = (
    customers
    .select(
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("first_name").alias("first_name"),
        F.col("last_name").alias("last_name"),
        F.col("email").alias("email"),
        F.col("city").alias("city"),
        F.col("state").alias("state"),
        F.col("created_at").alias("created_at")
    )
    .withColumn(
        "customer_key",
        F.conv(F.substring(F.sha2(F.col("customer_id").cast("string"), 256), 1, 16), 16, 10).cast("bigint")
    )
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

dim_customer.show(5, truncate=False)

# -----------------------------
# STEP 4: Build dim_product (raw)
# Denormalize category_name into product dimension
# -----------------------------
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
        F.col("p.created_at").alias("created_at")
    )
    .withColumn(
        "product_key",
        F.conv(F.substring(F.sha2(F.col("product_id").cast("string"), 256), 1, 16), 16, 10).cast("bigint")
    )
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

dim_product.show(5, truncate=False)

# -----------------------------
# STEP 5: Build dim_date (raw)
# Create date rows from min(order_ts) to max(order_ts)
# -----------------------------
# dim_date (fixed for Spark 3.5.x)
min_max = (
    orders.select(
        F.min(F.to_date("order_ts")).alias("min_date"),
        F.max(F.to_date("order_ts")).alias("max_date")
    ).collect()[0]
)

min_date = min_max["min_date"]
max_date = min_max["max_date"]

dim_date = (
    spark.sql("SELECT sequence(to_date('{}'), to_date('{}'), interval 1 day) AS dts".format(min_date, max_date))
    .select(F.explode("dts").alias("full_date"))
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year_num", F.year("full_date").cast("int"))
    .withColumn("month_num", F.month("full_date").cast("int"))
    .withColumn("day_num", F.dayofmonth("full_date").cast("int"))
    # dayofweek(): 1=Sunday ... 7=Saturday (Spark standard)
    .withColumn("day_of_week", F.dayofweek("full_date").cast("int"))
    # day name using supported pattern
    .withColumn("day_name", F.date_format("full_date", "EEE"))
    .select("date_key", "full_date", "year_num", "month_num", "day_num", "day_of_week", "day_name")
)

dim_date.show(20, truncate=False)

# -----------------------------
# STEP 6: Build fact_sales (order line grain)
# Each row represents one product in one order
# Measures: quantity, unit_price, discount_amount, gross_amount, net_amount
# -----------------------------
fact_sales = (
    order_items.alias("oi")
    .join(orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "inner")
    .select(
        F.col("oi.order_id").cast("int").alias("order_id"),
        F.col("oi.line_number").cast("int").alias("line_number"),
        F.col("o.customer_id").cast("int").alias("customer_id"),
        F.col("oi.product_id").cast("int").alias("product_id"),
        F.to_date(F.col("o.order_ts")).alias("order_date"),
        F.col("o.order_status").alias("order_status"),
        F.col("oi.quantity").cast("int").alias("quantity"),
        F.col("oi.unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("oi.discount_amount").cast("decimal(10,2)").alias("discount_amount")
    )
    .withColumn(
        "customer_key",
        F.conv(F.substring(F.sha2(F.col("customer_id").cast("string"), 256), 1, 16), 16, 10).cast("bigint")
    )
    .withColumn(
        "product_key",
        F.conv(F.substring(F.sha2(F.col("product_id").cast("string"), 256), 1, 16), 16, 10).cast("bigint")
    )
    .withColumn("date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast("int"))
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

fact_sales.show(20, truncate=False)

# Optional: prove grain uniqueness (order_id, line_number should not duplicate)
dup_cnt = fact_sales.groupBy("order_id", "line_number").count().filter(F.col("count") > 1).count()
print("Duplicate grain rows:", dup_cnt)

# -----------------------------
# STEP 7: Write to OLAP tables (overwrite for training)
# -----------------------------
(
    dim_customer.write.format("jdbc")
    .mode("overwrite")
    .option("url", OLAP_URL)
    .option("dbtable", "dim_customer")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .option("batchsize", "5000")
    .save()
)

(
    dim_product.write.format("jdbc")
    .mode("overwrite")
    .option("url", OLAP_URL)
    .option("dbtable", "dim_product")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .option("batchsize", "5000")
    .save()
)

(
    dim_date.write.format("jdbc")
    .mode("overwrite")
    .option("url", OLAP_URL)
    .option("dbtable", "dim_date")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .option("batchsize", "5000")
    .save()
)

(
    fact_sales.write.format("jdbc")
    .mode("overwrite")
    .option("url", OLAP_URL)
    .option("dbtable", "fact_sales")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .option("driver", DRIVER)
    .option("batchsize", "5000")
    .save()
)

print("Write complete to demo_olap")

# -----------------------------
# STEP 8: Quick validation queries in Spark (optional)
# -----------------------------
print("OLAP counts in Spark objects")
print("dim_customer:", dim_customer.count())
print("dim_product:", dim_product.count())
print("dim_date:", dim_date.count())
print("fact_sales:", fact_sales.count())

# Proof query: revenue by city using star schema dataframes
rev_by_city = (
    fact_sales.alias("f")
    .join(dim_customer.alias("c"), F.col("f.customer_key") == F.col("c.customer_key"), "inner")
    .filter(F.col("f.order_status") == F.lit("PLACED"))
    .groupBy(F.col("c.city"))
    .agg(F.sum(F.col("f.net_amount")).alias("net_revenue"))
    .orderBy(F.desc("net_revenue"))
)

rev_by_city.show(truncate=False)
