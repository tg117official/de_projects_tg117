/* ============================================================
   SINGLE SQL SCRIPT: SCD TYPE 1 vs TYPE 2 vs TYPE 3 (MySQL)
   Goal: show how SCD works with 3 simple scenarios
   Run in MySQL Workbench top-to-bottom
   ============================================================ */

DROP DATABASE IF EXISTS scd_demo;
CREATE DATABASE scd_demo;
USE scd_demo;

/* ------------------------------------------------------------
   STEP 1: Create staging and dimension tables
   ------------------------------------------------------------ */

DROP TABLE IF EXISTS stg_customer;
CREATE TABLE stg_customer (
  customer_id INT PRIMARY KEY,
  full_name   VARCHAR(100) NOT NULL,
  email       VARCHAR(100) NOT NULL,
  city        VARCHAR(50)  NOT NULL,
  updated_at  DATETIME     NOT NULL
);

DROP TABLE IF EXISTS dim_customer_type1;
CREATE TABLE dim_customer_type1 (
  customer_id INT PRIMARY KEY,
  full_name   VARCHAR(100) NOT NULL,
  email       VARCHAR(100) NOT NULL,
  city        VARCHAR(50)  NOT NULL,
  updated_at  DATETIME     NOT NULL
);

DROP TABLE IF EXISTS dim_customer_type2;
CREATE TABLE dim_customer_type2 (
  customer_key BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_id  INT NOT NULL,
  full_name    VARCHAR(100) NOT NULL,
  email        VARCHAR(100) NOT NULL,
  city         VARCHAR(50)  NOT NULL,
  start_date   DATETIME     NOT NULL,
  end_date     DATETIME     NULL,
  is_current   TINYINT      NOT NULL,
  updated_at   DATETIME     NOT NULL
);

CREATE INDEX ix_dim2_customer_current ON dim_customer_type2(customer_id, is_current);

DROP TABLE IF EXISTS dim_customer_type3;
CREATE TABLE dim_customer_type3 (
  customer_id     INT PRIMARY KEY,
  full_name       VARCHAR(100) NOT NULL,
  email           VARCHAR(100) NOT NULL,
  city_current    VARCHAR(50)  NOT NULL,
  city_previous   VARCHAR(50)  NULL,
  updated_at      DATETIME     NOT NULL
);

/* ------------------------------------------------------------
   STEP 2: Load initial staging data
   ------------------------------------------------------------ */

TRUNCATE TABLE stg_customer;

INSERT INTO stg_customer VALUES
(101, 'Rahul Sharma',  'rahul@example.com',  'Mumbai', '2024-01-10 10:00:00'),
(102, 'Anjali Patil',  'anjali@example.com', 'Pune',   '2024-01-10 10:00:00');

/* ------------------------------------------------------------
   STEP 3: Initial loads into each dimension table
   ------------------------------------------------------------ */

TRUNCATE TABLE dim_customer_type1;
INSERT INTO dim_customer_type1 (customer_id, full_name, email, city, updated_at)
SELECT customer_id, full_name, email, city, updated_at
FROM stg_customer;

TRUNCATE TABLE dim_customer_type2;
INSERT INTO dim_customer_type2
(customer_id, full_name, email, city, start_date, end_date, is_current, updated_at)
SELECT
  customer_id,
  full_name,
  email,
  city,
  updated_at AS start_date,
  NULL AS end_date,
  1 AS is_current,
  updated_at
FROM stg_customer;

TRUNCATE TABLE dim_customer_type3;
INSERT INTO dim_customer_type3 (customer_id, full_name, email, city_current, city_previous, updated_at)
SELECT customer_id, full_name, email, city, NULL, updated_at
FROM stg_customer;

/* ------------------------------------------------------------
   STEP 4: SCENARIO 1: SCD TYPE 1 (Overwrite)
   Story: Rahul email correction. History not needed.
   ------------------------------------------------------------ */

UPDATE stg_customer
SET email = 'rahul.new@example.com',
    updated_at = '2024-01-15 12:00:00'
WHERE customer_id = 101;

INSERT INTO dim_customer_type1 (customer_id, full_name, email, city, updated_at)
SELECT customer_id, full_name, email, city, updated_at
FROM stg_customer
ON DUPLICATE KEY UPDATE
  full_name  = VALUES(full_name),
  email      = VALUES(email),
  city       = VALUES(city),
  updated_at = VALUES(updated_at);

SELECT 'TYPE 1 RESULT AFTER EMAIL CORRECTION' AS step_title;
SELECT * FROM dim_customer_type1 ORDER BY customer_id;

/* ------------------------------------------------------------
   STEP 5: SCENARIO 2: SCD TYPE 2 (Full History)
   Story: Rahul city changes Mumbai -> Pune. Must preserve history.
   Logic:
   1) Expire current row if changed
   2) Insert new current row for changed records
   ------------------------------------------------------------ */

UPDATE stg_customer
SET city = 'Pune',
    updated_at = '2024-02-05 09:00:00'
WHERE customer_id = 101;

UPDATE dim_customer_type2 d
JOIN stg_customer s ON d.customer_id = s.customer_id
SET d.end_date = s.updated_at,
    d.is_current = 0
WHERE d.is_current = 1
  AND (
      d.full_name <> s.full_name
   OR d.email <> s.email
   OR d.city <> s.city
  );

INSERT INTO dim_customer_type2
(customer_id, full_name, email, city, start_date, end_date, is_current, updated_at)
SELECT
  s.customer_id,
  s.full_name,
  s.email,
  s.city,
  s.updated_at AS start_date,
  NULL AS end_date,
  1 AS is_current,
  s.updated_at
FROM stg_customer s
LEFT JOIN dim_customer_type2 d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE d.customer_id IS NULL;

SELECT 'TYPE 2 RESULT AFTER CITY CHANGE (HISTORY PRESERVED)' AS step_title;
SELECT customer_id, full_name, email, city, start_date, end_date, is_current
FROM dim_customer_type2
ORDER BY customer_id, start_date;

/* ------------------------------------------------------------
   STEP 6: SCENARIO 3: SCD TYPE 3 (Limited History)
   Story: Rahul city changes again Pune -> Delhi.
   Keep only current and previous city.
   ------------------------------------------------------------ */

UPDATE stg_customer
SET city = 'Delhi',
    updated_at = '2024-03-01 14:00:00'
WHERE customer_id = 101;

INSERT INTO dim_customer_type3 (customer_id, full_name, email, city_current, city_previous, updated_at)
SELECT customer_id, full_name, email, city, NULL, updated_at
FROM stg_customer
ON DUPLICATE KEY UPDATE
  full_name = VALUES(full_name),
  email = VALUES(email),
  city_previous = CASE
                    WHEN dim_customer_type3.city_current <> VALUES(city_current)
                    THEN dim_customer_type3.city_current
                    ELSE dim_customer_type3.city_previous
                  END,
  city_current = VALUES(city_current),
  updated_at = VALUES(updated_at);

SELECT 'TYPE 3 RESULT AFTER CITY CHANGE (ONLY PREVIOUS KEPT)' AS step_title;
SELECT * FROM dim_customer_type3 ORDER BY customer_id;

/* ------------------------------------------------------------
   STEP 7: Quick comparison summary query (optional)
   ------------------------------------------------------------ */

SELECT 'FINAL SUMMARY VIEW' AS step_title;

SELECT
  'TYPE1' AS scd_type,
  customer_id,
  full_name,
  email,
  city AS city_or_current_city,
  NULL AS previous_city,
  updated_at AS last_updated
FROM dim_customer_type1

UNION ALL

SELECT
  'TYPE2_CURRENT' AS scd_type,
  customer_id,
  full_name,
  email,
  city AS city_or_current_city,
  NULL AS previous_city,
  updated_at AS last_updated
FROM dim_customer_type2
WHERE is_current = 1

UNION ALL

SELECT
  'TYPE3' AS scd_type,
  customer_id,
  full_name,
  email,
  city_current AS city_or_current_city,
  city_previous AS previous_city,
  updated_at AS last_updated
FROM dim_customer_type3

ORDER BY scd_type, customer_id;
