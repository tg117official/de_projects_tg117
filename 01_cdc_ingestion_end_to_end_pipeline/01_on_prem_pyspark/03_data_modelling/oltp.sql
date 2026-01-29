-- =========================================================
-- OLTP NORMALIZED MODEL (MySQL)
-- Database: demo_oltp
-- =========================================================

DROP DATABASE IF EXISTS demo_oltp;
CREATE DATABASE demo_oltp;
USE demo_oltp;

-- Customers
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  customer_id     INT PRIMARY KEY,
  first_name      VARCHAR(50) NOT NULL,
  last_name       VARCHAR(50) NOT NULL,
  email           VARCHAR(100) NOT NULL,
  city            VARCHAR(50) NOT NULL,
  state           VARCHAR(50) NOT NULL,
  created_at      DATETIME NOT NULL
) ENGINE=InnoDB;

-- Categories
DROP TABLE IF EXISTS categories;
CREATE TABLE categories (
  category_id     INT PRIMARY KEY,
  category_name   VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

-- Products
DROP TABLE IF EXISTS products;
CREATE TABLE products (
  product_id      INT PRIMARY KEY,
  product_name    VARCHAR(100) NOT NULL,
  category_id     INT NOT NULL,
  list_price      DECIMAL(10,2) NOT NULL,
  is_active       TINYINT NOT NULL DEFAULT 1,
  created_at      DATETIME NOT NULL,
  CONSTRAINT fk_products_category
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
) ENGINE=InnoDB;

-- Orders (order header)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  order_id        INT PRIMARY KEY,
  customer_id     INT NOT NULL,
  order_ts        DATETIME NOT NULL,
  order_status    VARCHAR(20) NOT NULL,
  order_total     DECIMAL(10,2) NOT NULL,
  CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

-- Order items (order lines)
DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (
  order_id        INT NOT NULL,
  line_number     INT NOT NULL,
  product_id      INT NOT NULL,
  quantity        INT NOT NULL,
  unit_price      DECIMAL(10,2) NOT NULL,
  discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  PRIMARY KEY (order_id, line_number),
  CONSTRAINT fk_items_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_items_product
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

-- ------------------------
-- Sample Data
-- ------------------------

INSERT INTO customers VALUES
(1,'Rahul','Sharma','rahul@example.com','Mumbai','MH','2024-01-01 10:00:00'),
(2,'Anjali','Patil','anjali@example.com','Pune','MH','2024-01-03 11:00:00'),
(3,'Amit','Verma','amit@example.com','Bengaluru','KA','2024-01-05 09:30:00'),
(4,'Neha','Singh','neha@example.com','Delhi','DL','2024-01-06 15:10:00'),
(5,'Kiran','Joshi','kiran@example.com','Mumbai','MH','2024-01-07 08:45:00');

INSERT INTO categories VALUES
(10,'Electronics'),
(20,'Accessories'),
(30,'Home');

INSERT INTO products VALUES
(101,'Laptop Pro 14',10,70000.00,1,'2023-12-01 10:00:00'),
(102,'Wireless Mouse',20,800.00,1,'2023-12-05 10:00:00'),
(103,'Mechanical Keyboard',20,3500.00,1,'2023-12-07 10:00:00'),
(104,'Coffee Maker',30,4500.00,1,'2023-12-10 10:00:00'),
(105,'Noise Cancel Headphones',10,9000.00,1,'2023-12-12 10:00:00');

-- Orders
INSERT INTO orders VALUES
(1001,1,'2024-02-01 09:10:00','PLACED',  72400.00),
(1002,2,'2024-02-01 12:40:00','PLACED',   4300.00),
(1003,1,'2024-02-02 18:05:00','CANCELLED', 800.00),
(1004,3,'2024-02-03 10:20:00','PLACED',  12500.00),
(1005,4,'2024-02-03 21:15:00','PLACED',   4500.00),
(1006,5,'2024-02-04 11:30:00','PLACED',   8600.00);

-- Order items
-- Order 1001: Laptop + Mouse
INSERT INTO order_items VALUES
(1001,1,101,1,70000.00,  0.00),
(1001,2,102,3,  800.00,  0.00);

-- Order 1002: Keyboard + Mouse
INSERT INTO order_items VALUES
(1002,1,103,1,3500.00,200.00),
(1002,2,102,1, 800.00,  0.00);

-- Order 1003: Cancelled, Mouse only
INSERT INTO order_items VALUES
(1003,1,102,1,800.00,0.00);

-- Order 1004: Headphones + Keyboard
INSERT INTO order_items VALUES
(1004,1,105,1,9000.00,500.00),
(1004,2,103,1,3500.00,0.00);

-- Order 1005: Coffee Maker
INSERT INTO order_items VALUES
(1005,1,104,1,4500.00,0.00);

-- Order 1006: Mouse + Keyboard
INSERT INTO order_items VALUES
(1006,1,102,2,800.00,0.00),
(1006,2,103,2,3500.00,0.00);

-- Quick checks
SELECT COUNT(*) AS customers_cnt FROM customers;
SELECT COUNT(*) AS orders_cnt FROM orders;
SELECT COUNT(*) AS order_items_cnt FROM order_items;

-- Revenue By Category

SELECT c.category_name,
       SUM(oi.quantity * oi.unit_price - oi.discount_amount) AS net_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE o.order_status = 'PLACED'
GROUP BY c.category_name
ORDER BY net_revenue DESC;


-- Revenue By City and Category

SELECT cu.city,
       c.category_name,
       SUM(oi.quantity * oi.unit_price - oi.discount_amount) AS net_revenue
FROM orders o
JOIN customers cu ON o.customer_id = cu.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE o.order_status = 'PLACED'
GROUP BY cu.city, c.category_name
ORDER BY net_revenue DESC;


-- Daily revenue Trent

SELECT DATE(o.order_ts) AS full_date,
       SUM(oi.quantity * oi.unit_price - oi.discount_amount) AS net_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'PLACED'
GROUP BY DATE(o.order_ts)
ORDER BY full_date;

