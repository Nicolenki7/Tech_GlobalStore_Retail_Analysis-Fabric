-- ============================================================
--  Retail Global Store · esquema estrella en DuckDB
--  Reimplementacion abierta del modelo que se construyo en
--  Microsoft Fabric (ver README). No requiere licencia ni nube.
--
--  Uso: lo ejecuta run.py, que sustituye {{CSV}} por la ruta real.
-- ============================================================

-- ---------- staging: tipos explicitos, nada de inferencia ----------
CREATE OR REPLACE TABLE stg_orders AS
SELECT
  try_cast("Row ID"       AS INTEGER) AS row_id,
  "Order ID"                          AS order_id,
  try_cast(
    regexp_replace("Order Date", '^(\d{2})/(\d{2})/(\d{4})$', '\3-\2-\1') AS DATE
  )                                   AS order_date,
  try_cast(
    regexp_replace("Ship Date", '^(\d{2})/(\d{2})/(\d{4})$', '\3-\2-\1') AS DATE
  )                                   AS ship_date,
  "Ship Mode"                         AS ship_mode,
  "Customer ID"                       AS customer_id,
  "Customer Name"                     AS customer_name,
  "Segment"                           AS segment,
  "Country"                           AS country,
  "City"                              AS city,
  "State"                             AS state,
  "Postal Code"                       AS postal_code,
  "Region"                            AS region,
  "Product ID"                        AS product_id,
  "Category"                          AS category,
  "Sub-Category"                      AS sub_category,
  "Product Name"                      AS product_name,
  try_cast("Sales" AS DOUBLE)         AS sales
FROM read_csv_auto('{{CSV}}', header = true, all_varchar = true);

-- ---------- dimensiones ----------
CREATE OR REPLACE TABLE dim_customer AS
SELECT
  customer_id,
  any_value(customer_name) AS customer_name,
  any_value(segment)       AS segment,
  any_value(city)          AS city,
  any_value(state)         AS state,
  any_value(region)        AS region,
  any_value(country)       AS country
FROM stg_orders
GROUP BY customer_id;

CREATE OR REPLACE TABLE dim_product AS
SELECT
  product_id,
  any_value(product_name) AS product_name,
  any_value(category)     AS category,
  any_value(sub_category) AS sub_category
FROM stg_orders
GROUP BY product_id;

CREATE OR REPLACE TABLE dim_ship_mode AS
SELECT row_number() OVER (ORDER BY ship_mode) AS ship_mode_key, ship_mode
FROM (SELECT DISTINCT ship_mode FROM stg_orders);

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
  order_date                          AS date_key,
  year(order_date)                    AS year,
  month(order_date)                   AS month,
  strftime(order_date, '%Y-%m')       AS year_month
FROM stg_orders
WHERE order_date IS NOT NULL;

-- ---------- hechos ----------
CREATE OR REPLACE TABLE fact_orders AS
SELECT
  o.row_id,
  o.order_id,
  o.order_date                                        AS date_key,
  o.customer_id,
  o.product_id,
  s.ship_mode_key,
  o.sales,
  date_diff('day', o.order_date, o.ship_date)         AS days_to_ship
FROM stg_orders o
LEFT JOIN dim_ship_mode s USING (ship_mode);

-- ---------- KPIs: los mismos cortes que el informe ----------
CREATE OR REPLACE VIEW kpi_ship_mode AS
SELECT
  s.ship_mode,
  count(*)                        AS lineas,
  count(DISTINCT f.order_id)      AS pedidos,
  round(sum(f.sales), 2)          AS ventas,
  round(avg(f.sales), 2)          AS ticket_medio,
  round(avg(f.days_to_ship), 2)   AS dias_envio
FROM fact_orders f
JOIN dim_ship_mode s USING (ship_mode_key)
GROUP BY s.ship_mode
ORDER BY ventas DESC;

CREATE OR REPLACE VIEW kpi_monthly AS
SELECT
  d.year_month,
  count(DISTINCT f.order_id) AS pedidos,
  round(sum(f.sales), 2)     AS ventas
FROM fact_orders f
JOIN dim_date d USING (date_key)
GROUP BY d.year_month
ORDER BY d.year_month;

CREATE OR REPLACE VIEW kpi_category AS
SELECT
  p.category,
  p.sub_category,
  round(sum(f.sales), 2) AS ventas,
  count(*)               AS lineas
FROM fact_orders f
JOIN dim_product p USING (product_id)
GROUP BY p.category, p.sub_category
ORDER BY ventas DESC;

CREATE OR REPLACE VIEW kpi_customer AS
SELECT
  c.segment,
  count(DISTINCT c.customer_id) AS clientes,
  round(sum(f.sales), 2)        AS ventas,
  round(sum(f.sales) / count(DISTINCT c.customer_id), 2) AS ventas_por_cliente
FROM fact_orders f
JOIN dim_customer c USING (customer_id)
GROUP BY c.segment
ORDER BY ventas DESC;

-- ---------- controles de calidad ----------
CREATE OR REPLACE VIEW dq_checks AS
SELECT
  (SELECT count(*) FROM stg_orders)                                                   AS filas,
  (SELECT count(DISTINCT order_id) FROM stg_orders)                                   AS pedidos_distintos,
  (SELECT count(*) FROM stg_orders WHERE order_date IS NULL)                          AS fechas_invalidas,
  (SELECT count(*) FROM stg_orders WHERE sales IS NULL)                               AS ventas_invalidas,
  (SELECT count(*) FROM stg_orders WHERE sales < 0)                                   AS ventas_negativas,
  (SELECT count(*) FROM stg_orders WHERE ship_date < order_date)                      AS envio_antes_de_pedido,
  (SELECT count(*) FROM fact_orders WHERE ship_mode_key IS NULL)                      AS hechos_sin_dimension;
