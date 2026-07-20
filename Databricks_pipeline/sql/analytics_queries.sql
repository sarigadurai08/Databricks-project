-- =============================================================================
-- E-Commerce Lakehouse — SQL Analytics Scripts
-- Run against Gold / Silver Delta tables (Databricks SQL warehouse or Spark SQL)
--
-- PORTABLE: replace {{CATALOG}} with your runtime catalog, or run:
--   USE CATALOG <discovered_catalog>;
-- before executing. Schema names bronze/silver/gold are layer defaults.
-- Prefer notebook auto-registration (register_all_layers) over manual DDL.
-- =============================================================================

USE CATALOG {{CATALOG}};

-- -----------------------------------------------------------------------------
-- 1. Top Products — demand score, ratings, cart conversions
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_top_products AS
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active,
  units_demanded,
  cart_appearances,
  units_converted,
  review_count,
  avg_rating,
  demand_score
FROM gold.top_products
ORDER BY demand_score DESC
LIMIT 25;

SELECT * FROM gold.v_top_products;

-- -----------------------------------------------------------------------------
-- 2. Revenue Dashboard — daily gross revenue, discounts, payment capture
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_daily_revenue AS
SELECT
  revenue_date,
  gross_revenue,
  total_discounts,
  orders,
  captured_payments,
  failed_payments,
  refunds,
  ROUND(gross_revenue - total_discounts, 2) AS net_revenue,
  ROUND(captured_payments / NULLIF(gross_revenue, 0) * 100, 2) AS capture_rate_pct
FROM gold.revenue_dashboard
ORDER BY revenue_date;

SELECT * FROM gold.v_daily_revenue;

-- -----------------------------------------------------------------------------
-- 3. Orders — hourly order volume and AOV
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_hourly_orders AS
SELECT
  order_date,
  order_hour,
  order_count,
  revenue,
  avg_order_value,
  unique_customers
FROM gold.hourly_orders
ORDER BY order_date DESC, order_hour;

SELECT * FROM gold.v_hourly_orders;

-- -----------------------------------------------------------------------------
-- 4. Customer Journey — lifecycle metrics per user
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_customer_journey AS
SELECT
  user_id,
  email,
  region,
  loyalty_tier,
  status,
  order_count,
  first_order_time,
  last_order_time,
  lifetime_spend,
  click_events,
  sessions,
  cart_count,
  abandoned_carts,
  converted_carts,
  ROUND(abandoned_carts / NULLIF(cart_count, 0) * 100, 2) AS cart_abandonment_pct
FROM gold.customer_journey
ORDER BY lifetime_spend DESC;

SELECT * FROM gold.v_customer_journey LIMIT 50;

-- -----------------------------------------------------------------------------
-- 5. Session Analytics — engagement and duration
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_session_summary AS
SELECT
  session_id,
  user_id,
  device_type,
  referrer,
  session_start,
  session_end,
  duration_seconds,
  event_count,
  pages,
  products_viewed,
  CASE
    WHEN duration_seconds >= 300 THEN 'Long'
    WHEN duration_seconds >= 60 THEN 'Medium'
    ELSE 'Short'
  END AS session_depth
FROM gold.session_analytics
ORDER BY duration_seconds DESC;

SELECT * FROM gold.v_session_summary LIMIT 50;

-- -----------------------------------------------------------------------------
-- 6. Hourly Sales — peak hours with orders and revenue
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_peak_shopping_hours AS
SELECT
  hour_of_day,
  COALESCE(orders, 0) AS orders,
  COALESCE(revenue, 0) AS revenue,
  COALESCE(clicks, 0) AS clicks,
  COALESCE(sessions, 0) AS sessions,
  ROUND(COALESCE(revenue, 0) / NULLIF(COALESCE(orders, 0), 0), 2) AS avg_order_value
FROM gold.peak_shopping_hours
ORDER BY hour_of_day;

SELECT * FROM gold.v_peak_shopping_hours;

-- -----------------------------------------------------------------------------
-- 7. Cart Abandonment — abandoned value by user and cart
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_cart_abandonment AS
SELECT
  user_id,
  email,
  region,
  cart_id,
  products,
  units,
  abandoned_value,
  last_event_time
FROM gold.cart_abandonment
ORDER BY abandoned_value DESC;

SELECT
  COUNT(DISTINCT cart_id) AS abandoned_carts,
  COUNT(DISTINCT user_id) AS affected_users,
  ROUND(SUM(abandoned_value), 2) AS total_abandoned_value
FROM gold.cart_abandonment;

-- -----------------------------------------------------------------------------
-- 8. Repeat Customers — multi-order loyalty segment
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_repeat_customers AS
SELECT
  user_id,
  email,
  region,
  loyalty_tier,
  order_count,
  total_spend,
  first_order,
  last_order,
  days_between,
  ROUND(total_spend / NULLIF(order_count, 0), 2) AS avg_order_value
FROM gold.repeat_customers
ORDER BY order_count DESC, total_spend DESC;

SELECT
  COUNT(*) AS repeat_customer_count,
  ROUND(AVG(order_count), 2) AS avg_orders,
  ROUND(AVG(total_spend), 2) AS avg_lifetime_spend
FROM gold.repeat_customers;

-- -----------------------------------------------------------------------------
-- 9. Traffic Analytics — website traffic by date, device, referrer
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_website_traffic AS
SELECT
  traffic_date,
  event_type,
  device_type,
  referrer,
  events,
  sessions,
  users,
  ROUND(events / NULLIF(sessions, 0), 2) AS events_per_session
FROM gold.website_traffic
ORDER BY traffic_date DESC, events DESC;

SELECT
  referrer,
  SUM(sessions) AS total_sessions,
  SUM(events) AS total_events,
  SUM(users) AS total_users
FROM gold.website_traffic
GROUP BY referrer
ORDER BY total_sessions DESC;

-- -----------------------------------------------------------------------------
-- Bonus: Conversion Funnel
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_conversion_funnel AS
SELECT
  funnel_step,
  sessions,
  events,
  ROUND(sessions / NULLIF(FIRST_VALUE(sessions) OVER (ORDER BY
    CASE funnel_step
      WHEN 'browse' THEN 1
      WHEN 'cart' THEN 2
      WHEN 'checkout_start' THEN 3
      WHEN 'purchase' THEN 4
    END), 0) * 100, 2) AS session_pct_of_browse
FROM gold.conversion_funnel
ORDER BY
  CASE funnel_step
    WHEN 'browse' THEN 1
    WHEN 'cart' THEN 2
    WHEN 'checkout_start' THEN 3
    WHEN 'purchase' THEN 4
  END;

SELECT * FROM gold.v_conversion_funnel;

-- -----------------------------------------------------------------------------
-- Bonus: Revenue by Region
-- -----------------------------------------------------------------------------
SELECT
  region,
  orders,
  customers,
  revenue,
  avg_order_value,
  total_discounts
FROM gold.revenue_by_region
ORDER BY revenue DESC;
