/* =========================================================
   DATABASE SETUP
   ========================================================= */

CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;


/* =========================================================
   TABLE: customers
   Purpose:
   - Source table for Spark incremental ingestion
   - Uses lastmodified for CDC
   - Uses isDeleted for soft deletes
   ========================================================= */

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  customer_id   VARCHAR(50) PRIMARY KEY,
  first_name    VARCHAR(100) NOT NULL,
  last_name     VARCHAR(100) NOT NULL,
  email         VARCHAR(150) UNIQUE,
  address       VARCHAR(255),
  phone_number  VARCHAR(30),

  -- Soft delete flag (0 = active, 1 = deleted)
  isDeleted     TINYINT(1) NOT NULL DEFAULT 0,

  -- CDC column: auto-updated on INSERT and UPDATE
  lastmodified  TIMESTAMP NOT NULL
               DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP
);


/* =========================================================
   TABLE: cdc_tracking
   Purpose:
   - Stores last processed timestamp per table
   - Used as a bookmark for Spark incremental loads
   - DATETIME is used to avoid timezone & epoch issues
   ========================================================= */

DROP TABLE IF EXISTS cdc_tracking;

CREATE TABLE cdc_tracking (
  table_name    VARCHAR(100) PRIMARY KEY,
  lastmodified  DATETIME NOT NULL
);


/* =========================================================
   SEED CDC BOOKMARK
   Purpose:
   - Initial load marker for customers table
   ========================================================= */

INSERT INTO cdc_tracking (table_name, lastmodified)
VALUES ('customers', '1970-01-01 00:00:00')
ON DUPLICATE KEY UPDATE lastmodified = lastmodified;


/* =========================================================
   CDC DEMO OPERATIONS
   ========================================================= */


/* ---------------------------------------------------------
   INSERT: New customers
   - lastmodified auto-populated
   --------------------------------------------------------- */

INSERT INTO customers (customer_id, first_name, last_name, email, address, phone_number)
VALUES
('C001', 'Amit',  'Sharma', 'amit@example.com',  'Pune',   '9999990001'),
('C002', 'Neha',  'Patil',  'neha@example.com',  'Mumbai', '9999990002'),
('C003', 'Rohit', 'Joshi',  'rohit@example.com', 'Nagpur', '9999990003');


/* ---------------------------------------------------------
   UPDATE: Existing customer
   - Triggers lastmodified update
   --------------------------------------------------------- */

UPDATE customers
SET address = 'Pune - Hinjewadi',
    phone_number = '8888881111'
WHERE customer_id = 'C001';


/* ---------------------------------------------------------
   SOFT DELETE: Mark customer as deleted
   - No physical DELETE
   - lastmodified gets updated
   --------------------------------------------------------- */

UPDATE customers
SET isDeleted = 1
WHERE customer_id = 'C002';


/* ---------------------------------------------------------
   OPTIONAL: Restore soft-deleted customer
   --------------------------------------------------------- */

UPDATE customers
SET isDeleted = 0
WHERE customer_id = 'C002';


/* =========================================================
   VERIFICATION QUERIES (for demo)
   ========================================================= */

-- View source data
SELECT * FROM customers;

-- View CDC bookmark
SELECT * FROM cdc_tracking;
