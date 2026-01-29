DROP DATABASE IF EXISTS demo_olap;
CREATE DATABASE demo_olap;
USE demo_olap;

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;
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



INSERT INTO dim_customer VALUES
(10001, 1, 'Rahul',  'Sharma', 'rahul@example.com',  'Mumbai',    'MH', '2024-01-01 10:00:00'),
(10002, 2, 'Anjali', 'Patil',  'anjali@example.com', 'Pune',      'MH', '2024-01-03 11:00:00'),
(10003, 3, 'Amit',   'Verma',  'amit@example.com',   'Bengaluru', 'KA', '2024-01-05 09:30:00'),
(10004, 4, 'Neha',   'Singh',  'neha@example.com',   'Delhi',     'DL', '2024-01-06 15:10:00'),
(10005, 5, 'Kiran',  'Joshi',  'kiran@example.com',  'Mumbai',    'MH', '2024-01-07 08:45:00');

INSERT INTO dim_product VALUES
(20001, 101, 'Laptop Pro 14',            10, 'Electronics', 70000.00, 1, '2023-12-01 10:00:00'),
(20002, 102, 'Wireless Mouse',           20, 'Accessories',   800.00, 1, '2023-12-05 10:00:00'),
(20003, 103, 'Mechanical Keyboard',      20, 'Accessories',  3500.00, 1, '2023-12-07 10:00:00'),
(20004, 104, 'Coffee Maker',             30, 'Home',         4500.00, 1, '2023-12-10 10:00:00'),
(20005, 105, 'Noise Cancel Headphones',  10, 'Electronics',  9000.00, 1, '2023-12-12 10:00:00');

INSERT INTO dim_date VALUES
(20240201, '2024-02-01', 2024, 2, 1, 4, 'Thu'),
(20240202, '2024-02-02', 2024, 2, 2, 5, 'Fri'),
(20240203, '2024-02-03', 2024, 2, 3, 6, 'Sat'),
(20240204, '2024-02-04', 2024, 2, 4, 7, 'Sun');


INSERT INTO fact_sales
(order_id, line_number, customer_key, product_key, date_key, order_status,
 quantity, unit_price, discount_amount, gross_amount, net_amount)
VALUES
(1001, 1, 10001, 20001, 20240201, 'PLACED',    1, 70000.00,   0.00, 70000.00, 70000.00),
(1001, 2, 10001, 20002, 20240201, 'PLACED',    3,   800.00,   0.00,  2400.00,  2400.00),

(1002, 1, 10002, 20003, 20240201, 'PLACED',    1,  3500.00, 200.00,  3500.00,  3300.00),
(1002, 2, 10002, 20002, 20240201, 'PLACED',    1,   800.00,   0.00,   800.00,   800.00),

(1003, 1, 10001, 20002, 20240202, 'CANCELLED', 1,   800.00,   0.00,   800.00,   800.00),

(1004, 1, 10003, 20005, 20240203, 'PLACED',    1,  9000.00, 500.00,  9000.00,  8500.00),
(1004, 2, 10003, 20003, 20240203, 'PLACED',    1,  3500.00,   0.00,  3500.00,  3500.00),

(1005, 1, 10004, 20004, 20240203, 'PLACED',    1,  4500.00,   0.00,  4500.00,  4500.00),

(1006, 1, 10005, 20002, 20240204, 'PLACED',    2,   800.00,   0.00,  1600.00,  1600.00),
(1006, 2, 10005, 20003, 20240204, 'PLACED',    2,  3500.00,   0.00,  7000.00,  7000.00);

-- revenue by category

SELECT p.category_name,
       SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.order_status = 'PLACED'
GROUP BY p.category_name
ORDER BY net_revenue DESC;

-- revenue by city and category
SELECT c.city,
       p.category_name,
       SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product  p ON f.product_key  = p.product_key
WHERE f.order_status = 'PLACED'
GROUP BY c.city, p.category_name
ORDER BY net_revenue DESC;


-- daily revenue trend

SELECT d.full_date,
       SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'PLACED'
GROUP BY d.full_date
ORDER BY d.full_date;

