/* ============================================================
   OLTP vs OLAP DEMO SCRIPT (MySQL)
   Assumes databases already exist:
   demo_oltp with customers, orders, order_items, products, categories
   demo_olap with dim_customer, dim_product, dim_date, fact_sales
   ============================================================ */


/* ------------------------------------------------------------
   PART A: OLTP (Normalized) - Show analytics friction
   ------------------------------------------------------------ */

USE demo_oltp;

/* A1. Business question: Revenue by category (correct OLTP) */
SELECT c.category_name,
       SUM(oi.quantity * oi.unit_price - oi.discount_amount) AS net_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE o.order_status = 'PLACED'
GROUP BY c.category_name
ORDER BY net_revenue DESC;

/* A2. Business question: Revenue by city and category (correct OLTP)
   Note: we now need one extra join to customers. */
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

/* A3. Business question: Daily revenue trend (correct OLTP)
   Note: we must repeatedly extract date from timestamp. */
SELECT DATE(o.order_ts) AS full_date,
       SUM(oi.quantity * oi.unit_price - oi.discount_amount) AS net_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'PLACED'
GROUP BY DATE(o.order_ts)
ORDER BY full_date;

/* ------------------------------------------------------------
   PART B: OLTP Intentional Mistake - Prove double counting
   ------------------------------------------------------------ */

/* B1. Intentionally wrong approach:
   Use order_total and join to order_items to get category-wise revenue.
   This repeats the same order_total once per order_item row and inflates totals.
*/
SELECT c.category_name,
       SUM(o.order_total) AS wrong_revenue_using_order_total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE o.order_status = 'PLACED'
GROUP BY c.category_name
ORDER BY wrong_revenue_using_order_total DESC;

/* B2. inflation by comparing totals side by side */
SELECT
  (SELECT SUM(oi.quantity * oi.unit_price - oi.discount_amount)
   FROM orders o
   JOIN order_items oi ON o.order_id = oi.order_id
   WHERE o.order_status = 'PLACED') AS correct_line_level_total,
  (SELECT SUM(o.order_total)
   FROM orders o
   JOIN order_items oi ON o.order_id = oi.order_id
   WHERE o.order_status = 'PLACED') AS wrong_repeated_order_total;

/* B3. proof: repetition count per order (why order_total repeats) */
SELECT o.order_id,
       o.order_total,
       COUNT(*) AS number_of_lines_in_order
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'PLACED'
GROUP BY o.order_id, o.order_total
ORDER BY o.order_id;

/* ------------------------------------------------------------
   PART C: OLAP (Star Schema) - OLAP reduces the pain
   ------------------------------------------------------------ */

USE demo_olap;

/* C1. Same business question: Revenue by category (OLAP)
   Fewer joins and the measure already exists as net_amount at the chosen grain.
*/
SELECT p.category_name,
       SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.order_status = 'PLACED'
GROUP BY p.category_name
ORDER BY net_revenue DESC;

/* C2. Same business question: Revenue by city and category (OLAP)
   Add one dimension join without changing how measures are calculated.
*/
SELECT c.city,
       p.category_name,
       SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product  p ON f.product_key  = p.product_key
WHERE f.order_status = 'PLACED'
GROUP BY c.city, p.category_name
ORDER BY net_revenue DESC;

/* C3. Same business question: Daily revenue trend (OLAP)
   No DATE() extraction from timestamps in every query. Date is standardized.
*/
SELECT d.full_date,
       SUM(f.net_amount) AS net_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'PLACED'
GROUP BY d.full_date
ORDER BY d.full_date;

/* ------------------------------------------------------------
   PART D: Summary proof queries for discussion
   ------------------------------------------------------------ */

/* D1. OLAP grain check: one row per order line */
SELECT order_id, COUNT(*) AS lines
FROM fact_sales
GROUP BY order_id
ORDER BY order_id;

/* D2. OLAP is safer: net_amount is line-grain measure, not order-grain */
SELECT SUM(net_amount) AS placed_net_total
FROM fact_sales
WHERE order_status = 'PLACED';
