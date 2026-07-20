# Databricks SQL Dashboard Definitions — E-Commerce Lakehouse

Import these queries into **Databricks SQL** dashboards or **Lakeview**. All queries use schema-qualified gold table names (`gold.<table>`) — **no hardcoded catalog**. Set the SQL warehouse catalog context with `USE CATALOG <your_catalog>` or bind the dashboard to your UC catalog.

---

## 1. Executive Dashboard

**Purpose:** C-suite KPIs — revenue, orders, customers, conversion, regional performance.

### Widgets

1. **KPI — Total Revenue (30 days)**
   ```sql
   SELECT ROUND(SUM(gross_revenue), 2) AS total_revenue
   FROM gold.revenue_dashboard
   WHERE revenue_date >= date_sub(current_date(), 30);
   ```

2. **KPI — Total Orders**
   ```sql
   SELECT SUM(orders) AS total_orders
   FROM gold.revenue_dashboard
   WHERE revenue_date >= date_sub(current_date(), 30);
   ```

3. **KPI — Active Customers**
   ```sql
   SELECT COUNT(DISTINCT user_id) AS active_customers
   FROM gold.customer_journey
   WHERE order_count > 0;
   ```

4. **KPI — Average Order Value**
   ```sql
   SELECT ROUND(AVG(avg_order_value), 2) AS network_aov
   FROM gold.average_order_value
   WHERE order_month = date_format(current_date(), 'yyyy-MM');
   ```

5. **Chart — Daily Revenue Trend**
   ```sql
   SELECT revenue_date, gross_revenue, captured_payments, total_discounts
   FROM gold.revenue_dashboard
   ORDER BY revenue_date;
   ```

6. **Chart — Revenue by Region**
   ```sql
   SELECT region, revenue, customers, orders
   FROM gold.revenue_by_region
   ORDER BY revenue DESC;
   ```

7. **Chart — Conversion Funnel**
   ```sql
   SELECT funnel_step, sessions, events
   FROM gold.conversion_funnel
   ORDER BY
     CASE funnel_step
       WHEN 'browse' THEN 1 WHEN 'cart' THEN 2
       WHEN 'checkout_start' THEN 3 WHEN 'purchase' THEN 4
     END;
   ```

8. **Table — Top 10 Products**
   ```sql
   SELECT product_name, category, demand_score, avg_rating, price
   FROM gold.top_products
   ORDER BY demand_score DESC
   LIMIT 10;
   ```

---

## 2. Sales Dashboard

**Purpose:** Daily sales performance, regional breakdown, order status mix.

### Widgets

1. **KPI — Today's Revenue**
   ```sql
   SELECT ROUND(SUM(revenue), 2) AS today_revenue
   FROM gold.sales_dashboard
   WHERE sales_date = current_date();
   ```

2. **Chart — Sales by Region**
   ```sql
   SELECT shipping_region, SUM(revenue) AS revenue, SUM(order_count) AS orders
   FROM gold.sales_dashboard
   GROUP BY shipping_region
   ORDER BY revenue DESC;
   ```

3. **Chart — Order Status Mix**
   ```sql
   SELECT status, SUM(order_count) AS orders, ROUND(SUM(revenue), 2) AS revenue
   FROM gold.sales_dashboard
   GROUP BY status
   ORDER BY orders DESC;
   ```

4. **Chart — Hourly Orders (latest day)**
   ```sql
   SELECT order_hour, order_count, revenue, avg_order_value
   FROM gold.hourly_orders
   WHERE order_date = (SELECT MAX(order_date) FROM gold.hourly_orders)
   ORDER BY order_hour;
   ```

5. **Table — Monthly AOV by Region**
   ```sql
   SELECT order_month, shipping_region, orders, avg_order_value, total_revenue
   FROM gold.average_order_value
   ORDER BY order_month DESC, total_revenue DESC;
   ```

6. **Chart — Peak Shopping Hours**
   ```sql
   SELECT hour_of_day, orders, revenue
   FROM gold.peak_shopping_hours
   ORDER BY hour_of_day;
   ```

---

## 3. Operations Dashboard

**Purpose:** Fulfillment, payments, delivery health, inventory alerts.

### Widgets

1. **KPI — Payment Capture Rate**
   ```sql
   SELECT ROUND(
     SUM(captured_payments) / NULLIF(SUM(gross_revenue), 0) * 100, 2
   ) AS capture_rate_pct
   FROM gold.revenue_dashboard;
   ```

2. **Chart — Payment Methods**
   ```sql
   SELECT method, SUM(payment_count) AS payments, ROUND(SUM(total_amount), 2) AS amount
   FROM gold.payment_dashboard
   GROUP BY method
   ORDER BY amount DESC;
   ```

3. **Chart — Payment Status**
   ```sql
   SELECT status, SUM(payment_count) AS count, ROUND(SUM(total_amount), 2) AS amount
   FROM gold.payment_dashboard
   GROUP BY status;
   ```

4. **KPI — SKUs Below Reorder Level**
   ```sql
   SELECT COUNT(*) AS skus_need_reorder
   FROM gold.inventory_dashboard
   WHERE stock_status = 'Reorder';
   ```

5. **KPI — Out of Stock SKUs**
   ```sql
   SELECT COUNT(*) AS out_of_stock
   FROM gold.inventory_dashboard
   WHERE stock_status = 'OutOfStock';
   ```

6. **Table — Inventory by Warehouse**
   ```sql
   SELECT warehouse_id, stock_status, COUNT(*) AS sku_count
   FROM gold.inventory_dashboard
   GROUP BY warehouse_id, stock_status
   ORDER BY warehouse_id, stock_status;
   ```

7. **Chart — Inventory Trends**
   ```sql
   SELECT snapshot_date, warehouse_id, SUM(on_hand) AS on_hand, SUM(below_reorder_skus) AS below_reorder
   FROM gold.inventory_trends
   GROUP BY snapshot_date, warehouse_id
   ORDER BY snapshot_date, warehouse_id;
   ```

---

## 4. Customer Dashboard

**Purpose:** Customer lifetime value, repeat buyers, journey analytics.

### Widgets

1. **KPI — Total Registered Users**
   ```sql
   SELECT COUNT(*) AS total_users FROM gold.customer_journey;
   ```

2. **KPI — Repeat Customer Rate**
   ```sql
   SELECT ROUND(
     (SELECT COUNT(*) FROM gold.repeat_customers) /
     NULLIF((SELECT COUNT(*) FROM gold.customer_journey WHERE order_count > 0), 0) * 100, 2
   ) AS repeat_rate_pct;
   ```

3. **Chart — CLV Distribution by Tier**
   ```sql
   SELECT loyalty_tier, COUNT(*) AS customers, ROUND(AVG(clv), 2) AS avg_clv
   FROM gold.customer_lifetime_value
   GROUP BY loyalty_tier
   ORDER BY avg_clv DESC;
   ```

4. **Table — Top Customers by CLV**
   ```sql
   SELECT email, region, loyalty_tier, orders, clv, avg_order_value
   FROM gold.customer_lifetime_value
   ORDER BY clv DESC
   LIMIT 25;
   ```

5. **Table — Repeat Customers**
   ```sql
   SELECT email, region, order_count, total_spend, days_between
   FROM gold.repeat_customers
   ORDER BY total_spend DESC
   LIMIT 25;
   ```

6. **Chart — Customer Journey Funnel Metrics**
   ```sql
   SELECT
     AVG(sessions) AS avg_sessions,
     AVG(click_events) AS avg_clicks,
     AVG(order_count) AS avg_orders,
     AVG(abandoned_carts) AS avg_abandoned_carts
   FROM gold.customer_journey;
   ```

---

## 5. Marketing Dashboard

**Purpose:** Traffic sources, coupon performance, category trends, conversion.

### Widgets

1. **Chart — Traffic by Referrer**
   ```sql
   SELECT referrer, SUM(sessions) AS sessions, SUM(events) AS events
   FROM gold.website_traffic
   GROUP BY referrer
   ORDER BY sessions DESC;
   ```

2. **Chart — Traffic by Device**
   ```sql
   SELECT device_type, SUM(sessions) AS sessions, SUM(users) AS users
   FROM gold.website_traffic
   GROUP BY device_type;
   ```

3. **Chart — Event Type Mix**
   ```sql
   SELECT event_type, SUM(events) AS events, SUM(sessions) AS sessions
   FROM gold.website_traffic
   GROUP BY event_type
   ORDER BY events DESC;
   ```

4. **Table — Coupon Analytics**
   ```sql
   SELECT coupon_code, redemptions, unique_users, total_discount, orders_with_coupon
   FROM gold.coupon_analytics
   ORDER BY redemptions DESC;
   ```

5. **Chart — Top Categories**
   ```sql
   SELECT category, product_count, active_products, avg_rating
   FROM gold.top_categories
   ORDER BY product_count DESC;
   ```

6. **Chart — Conversion Funnel Drop-off**
   ```sql
   SELECT funnel_step, sessions,
     ROUND(sessions - LAG(sessions) OVER (ORDER BY funnel_step), 0) AS drop_off
   FROM gold.conversion_funnel;
   ```

---

## 6. Inventory Dashboard

**Purpose:** Stock levels, reorder alerts, warehouse distribution, trends.

### Widgets

1. **KPI — Total On-Hand Units**
   ```sql
   SELECT SUM(quantity_on_hand) AS total_on_hand
   FROM gold.inventory_dashboard;
   ```

2. **KPI — Available Units (on hand − reserved)**
   ```sql
   SELECT SUM(available_qty) AS total_available
   FROM gold.inventory_dashboard;
   ```

3. **Chart — Stock Status Distribution**
   ```sql
   SELECT stock_status, COUNT(*) AS sku_count
   FROM gold.inventory_dashboard
   GROUP BY stock_status;
   ```

4. **Chart — Inventory by Category**
   ```sql
   SELECT category, SUM(quantity_on_hand) AS on_hand, SUM(available_qty) AS available
   FROM gold.inventory_dashboard
   GROUP BY category
   ORDER BY on_hand DESC;
   ```

5. **Table — Reorder Alerts**
   ```sql
   SELECT product_name, category, warehouse_id, quantity_on_hand, reorder_level, stock_status
   FROM gold.inventory_dashboard
   WHERE stock_status IN ('Reorder', 'OutOfStock')
   ORDER BY stock_status, quantity_on_hand;
   ```

6. **Chart — Daily Inventory Trend**
   ```sql
   SELECT snapshot_date, SUM(on_hand) AS on_hand, SUM(below_reorder_skus) AS below_reorder
   FROM gold.inventory_trends
   GROUP BY snapshot_date
   ORDER BY snapshot_date;
   ```

---

## 7. Streaming Dashboard

**Purpose:** Real-time ingestion health, event volume, streaming lag indicators.

### Widgets

1. **KPI — Landing Files (Bronze row counts proxy)**
   ```sql
   SELECT COUNT(*) AS order_events
   FROM silver.orders
   WHERE _ingestion_time >= current_timestamp() - INTERVAL 1 HOUR;
   ```

2. **Chart — Hourly Click Events (recent)**
   ```sql
   SELECT
     date_trunc('hour', event_time) AS event_hour,
     COUNT(*) AS click_events
   FROM silver.click_logs
   WHERE event_time >= current_timestamp() - INTERVAL 24 HOUR
   GROUP BY date_trunc('hour', event_time)
   ORDER BY event_hour;
   ```

3. **Chart — Orders per Hour (streaming window)**
   ```sql
   SELECT order_hour, order_count, revenue
   FROM gold.hourly_orders
   WHERE order_date >= date_sub(current_date(), 1)
   ORDER BY order_date, order_hour;
   ```

4. **KPI — Recent Payment Failures**
   ```sql
   SELECT SUM(payment_count) AS failed_payments
   FROM gold.payment_dashboard
   WHERE status = 'Failed'
     AND payment_date >= date_sub(current_date(), 1);
   ```

5. **Chart — Cart Events by Status**
   ```sql
   SELECT status, COUNT(*) AS events
   FROM silver.shopping_cart
   WHERE _ingestion_time >= current_timestamp() - INTERVAL 6 HOUR
   GROUP BY status;
   ```

6. **Table — DQ Failures (latest run)**
   ```sql
   SELECT Entity, RuleName, FailedCount, TotalCount, Status, ValidatedAt
   FROM data_quality.validation_results
   ORDER BY ValidatedAt DESC
   LIMIT 20;
   ```

7. **Table — Pipeline Audit (latest steps)**
   ```sql
   SELECT pipeline_name, step_name, status, rows_inserted, started_at, ended_at
   FROM audit.pipeline_audit
   ORDER BY started_at DESC
   LIMIT 15;
   ```

---

## Lakeview / SQL Dashboard Import Notes

1. Create a **Databricks SQL warehouse** (Serverless recommended).
2. Run notebooks through Gold to build and register tables (`register_all_layers`).
3. Optionally run `sql/analytics_queries.sql` after replacing `{{CATALOG}}`.
4. Create seven Lakeview dashboards matching the sections above.
5. Set dashboard **catalog** context to your discovered UC catalog.
6. Schedule refresh aligned with the Gold job (hourly for streaming, nightly for full rebuild).
