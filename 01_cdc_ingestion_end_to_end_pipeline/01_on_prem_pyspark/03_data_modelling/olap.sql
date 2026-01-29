-- =========================================================
-- OLAP STAR SCHEMA MODEL (MySQL)
-- Database: demo_olap
-- =========================================================

DROP DATABASE IF EXISTS demo_olap;
CREATE DATABASE demo_olap;
USE demo_olap;

-- Dimension: Customer
DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
  customer_key    BIGINT PRIMARY KEY,
  customer_id     INT NOT NULL,
  first_name      VARCHAR(50) NOT NULL,
  last_name       VARCHAR(50) NOT NULL,
  email           VARCHAR(100) NOT NULL,
  city            VARCHAR(50) NOT NULL,
  state           VARCHAR(50) NOT NULL,
  created_at      DATETIME NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX ux_dim_customer_nk ON dim_customer(customer_id);

-- Dimension: Product (denormalized with category)
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
  product_key     BIGINT PRIMARY KEY,
  product_id      INT NOT NULL,
  product_name    VARCHAR(100) NOT NULL,
  category_id     INT NOT NULL,
  category_name   VARCHAR(50) NOT NULL,
  list_price      DECIMAL(10,2) NOT NULL,
  is_active       TINYINT NOT NULL,
  created_at      DATETIME NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX ux_dim_product_nk ON dim_product(product_id);

-- Dimension: Date
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
  date_key        INT PRIMARY KEY,
  full_date       DATE NOT NULL,
  year_num        INT NOT NULL,
  month_num       INT NOT NULL,
  day_num         INT NOT NULL,
  day_of_week     INT NOT NULL,
  day_name        VARCHAR(10) NOT NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX ux_dim_date_full ON dim_date(full_date);

-- Fact: Sales (order line grain)
DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
  sales_key       BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id        INT NOT NULL,
  line_number     INT NOT NULL,
  customer_key    BIGINT NOT NULL,
  product_key     BIGINT NOT NULL,
  date_key        INT NOT NULL,
  order_status    VARCHAR(20) NOT NULL,
  quantity        INT NOT NULL,
  unit_price      DECIMAL(10,2) NOT NULL,
  discount_amount DECIMAL(10,2) NOT NULL,
  gross_amount    DECIMAL(10,2) NOT NULL,
  net_amount      DECIMAL(10,2) NOT NULL,
  CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
  CONSTRAINT fk_fact_product  FOREIGN KEY (product_key)  REFERENCES dim_product(product_key),
  CONSTRAINT fk_fact_date     FOREIGN KEY (date_key)     REFERENCES dim_date(date_key)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX ux_fact_sales_nk ON fact_sales(order_id, line_number);
