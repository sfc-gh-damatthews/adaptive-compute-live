-- =============================================================================
-- Setup: Adaptive vs Classic Warehouse Load Tester
-- Run this once before deploying the Streamlit app.
-- Uses SNOWFLAKE_SAMPLE_DATA.TPCH_SF10 (available in all Snowflake accounts).
-- =============================================================================

-- Database & schemas
CREATE DATABASE IF NOT EXISTS ZW_DB_ADAPTIVE;
USE DATABASE ZW_DB_ADAPTIVE;

CREATE SCHEMA IF NOT EXISTS ZW_SCH_ADMIN;
CREATE SCHEMA IF NOT EXISTS ZW_SCH_VIEWS;
CREATE SCHEMA IF NOT EXISTS ZW_SCH_TABLES;

-- =============================================================================
-- Warehouses
-- =============================================================================

-- Classic: Small multi-cluster, max 3 clusters (Standard scaling policy).
-- Under load it adds clusters — but there's always a spin-up delay before relief arrives.
CREATE OR REPLACE WAREHOUSE ZW_CLASSIC_WH
  WITH
    WAREHOUSE_SIZE        = 'SMALL'
    WAREHOUSE_TYPE        = 'STANDARD'
    GENERATION            = '2'
    MIN_CLUSTER_COUNT     = 1
    MAX_CLUSTER_COUNT     = 3
    SCALING_POLICY        = 'STANDARD'
    AUTO_SUSPEND          = 60
    AUTO_RESUME           = TRUE
    MAX_CONCURRENCY_LEVEL = 8;

-- Adaptive: equivalent resource envelope to Classic above — Small max per-query, 5× throughput.
-- Same cost ceiling, but no cluster spin-up lag.
-- "Flipping" the Classic warehouse to Adaptive without changing the budget.
CREATE OR REPLACE WAREHOUSE ZW_ADAPTIVE_WH
  WITH
    WAREHOUSE_TYPE               = ADAPTIVE
    MAX_QUERY_PERFORMANCE_LEVEL  = SMALL
    QUERY_THROUGHPUT_MULTIPLIER  = 5;

-- =============================================================================
-- Admin: execution log
-- =============================================================================

USE SCHEMA ZW_SCH_ADMIN;

CREATE OR REPLACE SEQUENCE ZW_SEQ_EXEC_ID START = 1 INCREMENT = 1;

CREATE OR REPLACE TRANSIENT TABLE ZW_T_EXEC_LOG (
    EXEC_ID        NUMBER(38,0) DEFAULT ZW_SEQ_EXEC_ID.NEXTVAL,
    WAREHOUSE_TYPE VARCHAR(20),
    START_TIME     TIMESTAMP_NTZ,
    END_TIME       TIMESTAMP_NTZ
);

-- =============================================================================
-- Cleanup stored procedure (drops all _LIVE_ tables created by the app)
-- =============================================================================

CREATE OR REPLACE PROCEDURE ZW_SP_CLEANUP_LIVE_TABLES()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var stmt = snowflake.createStatement({
    sqlText: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_SCHEMA = 'ZW_SCH_TABLES'
                AND TABLE_NAME LIKE '%_LIVE_%'`
  });
  var result = stmt.execute();
  var dropped = 0;
  while (result.next()) {
    var tname = result.getColumnValue(1);
    snowflake.createStatement({
      sqlText: `DROP TABLE IF EXISTS ZW_DB_ADAPTIVE.ZW_SCH_TABLES.` + tname
    }).execute();
    dropped++;
  }
  return 'Dropped ' + dropped + ' tables.';
$$;

-- =============================================================================
-- Simple views (ZW_V_S_*)
-- =============================================================================

USE SCHEMA ZW_SCH_VIEWS;

CREATE OR REPLACE VIEW ZW_V_S_REVENUE_BY_NATION AS
SELECT
    n.n_name                                        AS NATION,
    SUM(l.l_extendedprice * (1 - l.l_discount))    AS TOTAL_REVENUE,
    COUNT(*)                                         AS ORDER_LINE_COUNT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS   o ON l.l_orderkey  = o.o_orderkey
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER c ON o.o_custkey   = c.c_custkey
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION   n ON c.c_nationkey = n.n_nationkey
GROUP BY n.n_name;

CREATE OR REPLACE VIEW ZW_V_S_TOP_CUSTOMERS AS
SELECT
    c.c_custkey,
    c.c_name,
    n.n_name                    AS NATION,
    SUM(o.o_totalprice)         AS TOTAL_SPEND,
    COUNT(DISTINCT o.o_orderkey) AS ORDER_COUNT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER c
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS   o ON c.c_custkey   = o.o_custkey
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION   n ON c.c_nationkey = n.n_nationkey
GROUP BY c.c_custkey, c.c_name, n.n_name;

CREATE OR REPLACE VIEW ZW_V_S_MONTHLY_ORDERS AS
SELECT
    DATE_TRUNC('MONTH', o.o_orderdate) AS ORDER_MONTH,
    o.o_orderpriority                   AS PRIORITY,
    COUNT(*)                            AS ORDER_COUNT,
    SUM(o.o_totalprice)                 AS TOTAL_VALUE,
    AVG(o.o_totalprice)                 AS AVG_ORDER_VALUE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS o
GROUP BY DATE_TRUNC('MONTH', o.o_orderdate), o.o_orderpriority;

CREATE OR REPLACE VIEW ZW_V_S_SUPPLIER_PARTS AS
SELECT
    s.s_name          AS SUPPLIER_NAME,
    n.n_name          AS NATION,
    COUNT(DISTINCT ps.ps_partkey) AS PARTS_SUPPLIED,
    SUM(ps.ps_availqty)           AS TOTAL_AVAILABLE_QTY,
    AVG(ps.ps_supplycost)         AS AVG_SUPPLY_COST
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.SUPPLIER s
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PARTSUPP ps ON s.s_suppkey  = ps.ps_suppkey
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION   n  ON s.s_nationkey = n.n_nationkey
GROUP BY s.s_name, n.n_name;

CREATE OR REPLACE VIEW ZW_V_S_SHIPPING_PERFORMANCE AS
SELECT
    l.l_shipmode,
    SUM(CASE WHEN l.l_receiptdate > l.l_commitdate THEN 1 ELSE 0 END)  AS LATE_COUNT,
    SUM(CASE WHEN l.l_receiptdate <= l.l_commitdate THEN 1 ELSE 0 END) AS ONTIME_COUNT,
    COUNT(*)                                                             AS TOTAL_SHIPMENTS,
    ROUND(SUM(CASE WHEN l.l_receiptdate > l.l_commitdate THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                                         AS LATE_PCT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l
GROUP BY l.l_shipmode;

-- =============================================================================
-- Medium views (ZW_V_M_*)
-- =============================================================================

CREATE OR REPLACE VIEW ZW_V_M_REVENUE_YOY_GROWTH AS
WITH yearly_revenue AS (
    SELECT
        n.n_name                                     AS NATION,
        YEAR(o.o_orderdate)                          AS ORDER_YEAR,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS REVENUE
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS   o ON l.l_orderkey  = o.o_orderkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER c ON o.o_custkey   = c.c_custkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION   n ON c.c_nationkey = n.n_nationkey
    GROUP BY n.n_name, YEAR(o.o_orderdate)
)
SELECT
    NATION, ORDER_YEAR, REVENUE,
    LAG(REVENUE) OVER (PARTITION BY NATION ORDER BY ORDER_YEAR) AS PREV_YEAR_REVENUE,
    ROUND(
        (REVENUE - LAG(REVENUE) OVER (PARTITION BY NATION ORDER BY ORDER_YEAR))
        / NULLIF(LAG(REVENUE) OVER (PARTITION BY NATION ORDER BY ORDER_YEAR), 0) * 100
    , 2) AS YOY_GROWTH_PCT
FROM yearly_revenue;

CREATE OR REPLACE VIEW ZW_V_M_CUSTOMER_SEGMENTS AS
WITH customer_metrics AS (
    SELECT
        c.c_custkey, c.c_name, n.n_name AS NATION,
        COUNT(DISTINCT o.o_orderkey)    AS ORDER_COUNT,
        SUM(o.o_totalprice)             AS TOTAL_SPEND,
        DATEDIFF('day', MIN(o.o_orderdate), MAX(o.o_orderdate)) AS CUSTOMER_LIFESPAN_DAYS,
        MAX(o.o_orderdate)              AS LAST_ORDER_DATE
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER c
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS   o ON c.c_custkey   = o.o_custkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION   n ON c.c_nationkey = n.n_nationkey
    GROUP BY c.c_custkey, c.c_name, n.n_name
)
SELECT *,
    NTILE(4) OVER (ORDER BY TOTAL_SPEND  DESC) AS SPEND_QUARTILE,
    NTILE(4) OVER (ORDER BY ORDER_COUNT  DESC) AS FREQUENCY_QUARTILE,
    CASE
        WHEN NTILE(4) OVER (ORDER BY TOTAL_SPEND DESC) = 1
         AND NTILE(4) OVER (ORDER BY ORDER_COUNT DESC) = 1 THEN 'HIGH_VALUE'
        WHEN NTILE(4) OVER (ORDER BY TOTAL_SPEND DESC) <= 2  THEN 'MID_VALUE'
        ELSE 'LOW_VALUE'
    END AS CUSTOMER_SEGMENT
FROM customer_metrics;

CREATE OR REPLACE VIEW ZW_V_M_SUPPLIER_RANKING AS
WITH supplier_stats AS (
    SELECT
        s.s_suppkey, s.s_name, n.n_name AS NATION, r.r_name AS REGION,
        COUNT(DISTINCT l.l_orderkey)                      AS ORDERS_FULFILLED,
        SUM(l.l_quantity)                                 AS TOTAL_QTY_SHIPPED,
        SUM(l.l_extendedprice * (1 - l.l_discount))      AS TOTAL_REVENUE,
        AVG(DATEDIFF('day', l.l_shipdate, l.l_receiptdate)) AS AVG_DELIVERY_DAYS,
        SUM(CASE WHEN l.l_receiptdate > l.l_commitdate THEN 1 ELSE 0 END) AS LATE_SHIPMENTS,
        COUNT(*)                                          AS TOTAL_SHIPMENTS
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.SUPPLIER s
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l  ON s.s_suppkey  = l.l_suppkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION   n  ON s.s_nationkey = n.n_nationkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.REGION   r  ON n.n_regionkey = r.r_regionkey
    GROUP BY s.s_suppkey, s.s_name, n.n_name, r.r_name
)
SELECT *,
    ROUND(LATE_SHIPMENTS * 100.0 / NULLIF(TOTAL_SHIPMENTS, 0), 2) AS LATE_PCT,
    RANK() OVER (PARTITION BY REGION ORDER BY TOTAL_REVENUE    DESC) AS REVENUE_RANK_IN_REGION,
    RANK() OVER (PARTITION BY REGION ORDER BY LATE_SHIPMENTS   ASC)  AS RELIABILITY_RANK_IN_REGION
FROM supplier_stats;

CREATE OR REPLACE VIEW ZW_V_M_PART_PROFITABILITY AS
WITH part_sales AS (
    SELECT
        p.p_partkey, p.p_name, p.p_type, p.p_size,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS NET_REVENUE,
        SUM(l.l_quantity)                            AS TOTAL_QTY_SOLD,
        COUNT(DISTINCT l.l_orderkey)                 AS DISTINCT_ORDERS
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PART     p
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l ON p.p_partkey = l.l_partkey
    GROUP BY p.p_partkey, p.p_name, p.p_type, p.p_size
),
part_costs AS (
    SELECT ps.ps_partkey,
        AVG(ps.ps_supplycost)        AS AVG_SUPPLY_COST,
        COUNT(DISTINCT ps.ps_suppkey) AS SUPPLIER_COUNT
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PARTSUPP ps
    GROUP BY ps.ps_partkey
)
SELECT
    s.p_partkey, s.p_name, s.p_type, s.p_size,
    s.NET_REVENUE, s.TOTAL_QTY_SOLD, s.DISTINCT_ORDERS,
    c.AVG_SUPPLY_COST, c.SUPPLIER_COUNT,
    ROUND(s.NET_REVENUE - (s.TOTAL_QTY_SOLD * c.AVG_SUPPLY_COST), 2)                            AS ESTIMATED_PROFIT,
    ROUND((s.NET_REVENUE - (s.TOTAL_QTY_SOLD * c.AVG_SUPPLY_COST))
          / NULLIF(s.NET_REVENUE, 0) * 100, 2)                                                   AS MARGIN_PCT
FROM part_sales s
JOIN part_costs c ON s.p_partkey = c.ps_partkey;

CREATE OR REPLACE VIEW ZW_V_M_ORDER_PIPELINE AS
WITH order_details AS (
    SELECT
        o.o_orderkey, o.o_custkey, o.o_orderdate, o.o_orderstatus,
        o.o_totalprice, o.o_orderpriority,
        COUNT(l.l_linenumber)   AS LINE_ITEM_COUNT,
        SUM(l.l_quantity)       AS TOTAL_QTY,
        MIN(l.l_shipdate)       AS FIRST_SHIP_DATE,
        MAX(l.l_shipdate)       AS LAST_SHIP_DATE,
        SUM(CASE WHEN l.l_receiptdate > l.l_commitdate THEN 1 ELSE 0 END) AS LATE_ITEMS,
        AVG(DATEDIFF('day', o.o_orderdate, l.l_shipdate))                  AS AVG_DAYS_TO_SHIP
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS   o
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l ON o.o_orderkey = l.l_orderkey
    GROUP BY o.o_orderkey, o.o_custkey, o.o_orderdate, o.o_orderstatus,
             o.o_totalprice, o.o_orderpriority
)
SELECT *,
    DATEDIFF('day', FIRST_SHIP_DATE, LAST_SHIP_DATE) AS FULFILLMENT_SPAN_DAYS,
    CASE
        WHEN LATE_ITEMS = 0               THEN 'FULLY_ON_TIME'
        WHEN LATE_ITEMS < LINE_ITEM_COUNT THEN 'PARTIALLY_LATE'
        ELSE 'FULLY_LATE'
    END AS FULFILLMENT_STATUS,
    PERCENT_RANK() OVER (ORDER BY o_totalprice) AS PRICE_PERCENTILE
FROM order_details;

-- =============================================================================
-- Complex views (ZW_V_C_*)
-- =============================================================================

CREATE OR REPLACE VIEW ZW_V_C_MARKET_SHARE_ANALYSIS AS
WITH regional_sales AS (
    SELECT
        r.r_name AS REGION, n.n_name AS NATION,
        YEAR(o.o_orderdate)    AS ORDER_YEAR,
        QUARTER(o.o_orderdate) AS ORDER_QUARTER,
        p.p_type               AS PART_TYPE,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS REVENUE
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM  l
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS    o ON l.l_orderkey  = o.o_orderkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER  c ON o.o_custkey   = c.c_custkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION    n ON c.c_nationkey = n.n_nationkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.REGION    r ON n.n_regionkey = r.r_regionkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PART      p ON l.l_partkey   = p.p_partkey
    GROUP BY r.r_name, n.n_name, YEAR(o.o_orderdate), QUARTER(o.o_orderdate), p.p_type
),
region_totals AS (
    SELECT REGION, ORDER_YEAR, ORDER_QUARTER,
           SUM(REVENUE) AS REGION_TOTAL_REVENUE
    FROM regional_sales
    GROUP BY REGION, ORDER_YEAR, ORDER_QUARTER
),
nation_share AS (
    SELECT rs.*,
        rt.REGION_TOTAL_REVENUE,
        ROUND(rs.REVENUE / NULLIF(rt.REGION_TOTAL_REVENUE, 0) * 100, 4) AS MARKET_SHARE_PCT
    FROM regional_sales rs
    JOIN region_totals  rt ON rs.REGION = rt.REGION
                           AND rs.ORDER_YEAR = rt.ORDER_YEAR
                           AND rs.ORDER_QUARTER = rt.ORDER_QUARTER
),
ranked AS (
    SELECT *,
        AVG(MARKET_SHARE_PCT) OVER (
            PARTITION BY NATION, PART_TYPE
            ORDER BY ORDER_YEAR, ORDER_QUARTER
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS ROLLING_4Q_AVG_SHARE,
        LAG(MARKET_SHARE_PCT, 4) OVER (
            PARTITION BY NATION, PART_TYPE
            ORDER BY ORDER_YEAR, ORDER_QUARTER
        ) AS SAME_QUARTER_PREV_YEAR_SHARE,
        DENSE_RANK() OVER (
            PARTITION BY REGION, ORDER_YEAR, ORDER_QUARTER
            ORDER BY REVENUE DESC
        ) AS NATION_RANK_IN_REGION
    FROM nation_share
)
SELECT *,
    ROUND(MARKET_SHARE_PCT - COALESCE(SAME_QUARTER_PREV_YEAR_SHARE, 0), 4) AS SHARE_CHANGE_YOY,
    CASE
        WHEN MARKET_SHARE_PCT > ROLLING_4Q_AVG_SHARE * 1.1 THEN 'GAINING'
        WHEN MARKET_SHARE_PCT < ROLLING_4Q_AVG_SHARE * 0.9 THEN 'LOSING'
        ELSE 'STABLE'
    END AS TREND_STATUS
FROM ranked;

CREATE OR REPLACE VIEW ZW_V_C_SUPPLY_CHAIN_RISK AS
WITH supplier_delivery AS (
    SELECT
        s.s_suppkey, s.s_name, n.n_name AS SUPPLIER_NATION, r.r_name AS SUPPLIER_REGION,
        COUNT(*)      AS TOTAL_LINES,
        SUM(CASE WHEN l.l_receiptdate > l.l_commitdate THEN 1 ELSE 0 END) AS LATE_LINES,
        AVG(DATEDIFF('day', l.l_commitdate, l.l_receiptdate)) AS AVG_DELIVERY_VARIANCE_DAYS,
        STDDEV(DATEDIFF('day', l.l_commitdate, l.l_receiptdate))           AS DELIVERY_VARIANCE_STDDEV,
        MAX(DATEDIFF('day', l.l_commitdate, l.l_receiptdate))              AS WORST_DELAY_DAYS,
        COUNT(DISTINCT c.c_nationkey)                                      AS CUSTOMER_NATIONS_SERVED
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.SUPPLIER  s
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM   l ON s.s_suppkey   = l.l_suppkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS     o ON l.l_orderkey  = o.o_orderkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER   c ON o.o_custkey   = c.c_custkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION     n ON s.s_nationkey = n.n_nationkey
    JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.REGION     r ON n.n_regionkey = r.r_regionkey
    GROUP BY s.s_suppkey, s.s_name, n.n_name, r.r_name
),
supplier_concentration AS (
    SELECT ps.ps_partkey, COUNT(DISTINCT ps.ps_suppkey) AS NUM_SUPPLIERS
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PARTSUPP ps
    GROUP BY ps.ps_partkey
),
single_source_parts AS (
    SELECT ps.ps_suppkey, COUNT(*) AS SINGLE_SOURCE_PART_COUNT
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PARTSUPP ps
    JOIN supplier_concentration sc ON ps.ps_partkey = sc.ps_partkey
    WHERE sc.NUM_SUPPLIERS = 1
    GROUP BY ps.ps_suppkey
),
revenue_dependency AS (
    SELECT l.l_suppkey,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS SUPPLIER_REVENUE,
        SUM(l.l_extendedprice * (1 - l.l_discount))
            / NULLIF(SUM(SUM(l.l_extendedprice * (1 - l.l_discount))) OVER (), 0) * 100
            AS REVENUE_CONCENTRATION_PCT
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM l
    GROUP BY l.l_suppkey
),
scored AS (
    SELECT
        sd.s_suppkey, sd.s_name, sd.SUPPLIER_NATION, sd.SUPPLIER_REGION,
        sd.TOTAL_LINES, sd.LATE_LINES,
        ROUND(sd.LATE_LINES * 100.0 / NULLIF(sd.TOTAL_LINES, 0), 2) AS LATE_PCT,
        sd.AVG_DELIVERY_VARIANCE_DAYS, sd.DELIVERY_VARIANCE_STDDEV,
        sd.WORST_DELAY_DAYS, sd.CUSTOMER_NATIONS_SERVED,
        COALESCE(ssp.SINGLE_SOURCE_PART_COUNT, 0) AS SINGLE_SOURCE_PARTS,
        rd.SUPPLIER_REVENUE, rd.REVENUE_CONCENTRATION_PCT,
        ROUND(
            (LEAST(sd.LATE_LINES * 100.0 / NULLIF(sd.TOTAL_LINES, 0), 100) * 0.30)
          + (LEAST(GREATEST(sd.AVG_DELIVERY_VARIANCE_DAYS, 0) * 5, 100) * 0.20)
          + (LEAST(COALESCE(sd.DELIVERY_VARIANCE_STDDEV, 0) * 3, 100) * 0.15)
          + (LEAST(COALESCE(ssp.SINGLE_SOURCE_PART_COUNT, 0) * 10, 100) * 0.20)
          + (LEAST(rd.REVENUE_CONCENTRATION_PCT * 20, 100) * 0.15)
        , 2) AS RISK_SCORE
    FROM supplier_delivery sd
    LEFT JOIN single_source_parts ssp ON sd.s_suppkey = ssp.ps_suppkey
    JOIN  revenue_dependency      rd  ON sd.s_suppkey = rd.l_suppkey
)
SELECT *,
    CASE
        WHEN RISK_SCORE >= 70 THEN 'CRITICAL'
        WHEN RISK_SCORE >= 50 THEN 'HIGH'
        WHEN RISK_SCORE >= 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS RISK_TIER,
    RANK() OVER (ORDER BY RISK_SCORE DESC)                        AS OVERALL_RISK_RANK,
    RANK() OVER (PARTITION BY SUPPLIER_REGION ORDER BY RISK_SCORE DESC) AS REGIONAL_RISK_RANK
FROM scored;

-- =============================================================================
-- Base tables (templates for INSERT ... SELECT FROM view pattern)
-- =============================================================================

USE SCHEMA ZW_SCH_TABLES;

CREATE OR REPLACE TRANSIENT TABLE ZW_T_S_REVENUE_BY_NATION  (NATION VARCHAR(25), TOTAL_REVENUE NUMBER(38,4), ORDER_LINE_COUNT NUMBER(38,0));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_S_TOP_CUSTOMERS       (C_CUSTKEY NUMBER(38,0), C_NAME VARCHAR(25), NATION VARCHAR(25), TOTAL_SPEND NUMBER(38,4), ORDER_COUNT NUMBER(38,0));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_S_MONTHLY_ORDERS      (ORDER_MONTH DATE, PRIORITY VARCHAR(15), ORDER_COUNT NUMBER(38,0), TOTAL_VALUE NUMBER(38,4), AVG_ORDER_VALUE NUMBER(38,4));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_S_SUPPLIER_PARTS      (SUPPLIER_NAME VARCHAR(25), NATION VARCHAR(25), PARTS_SUPPLIED NUMBER(38,0), TOTAL_AVAILABLE_QTY NUMBER(38,0), AVG_SUPPLY_COST NUMBER(38,4));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_S_SHIPPING_PERFORMANCE(L_SHIPMODE VARCHAR(10), LATE_COUNT NUMBER(38,0), ONTIME_COUNT NUMBER(38,0), TOTAL_SHIPMENTS NUMBER(38,0), LATE_PCT NUMBER(5,2));

CREATE OR REPLACE TRANSIENT TABLE ZW_T_M_REVENUE_YOY_GROWTH  (NATION VARCHAR(25), ORDER_YEAR NUMBER(4,0), REVENUE NUMBER(38,4), PREV_YEAR_REVENUE NUMBER(38,4), YOY_GROWTH_PCT NUMBER(10,2));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_M_CUSTOMER_SEGMENTS   (C_CUSTKEY NUMBER(38,0), C_NAME VARCHAR(25), NATION VARCHAR(25), ORDER_COUNT NUMBER(38,0), TOTAL_SPEND NUMBER(38,4), CUSTOMER_LIFESPAN_DAYS NUMBER(38,0), LAST_ORDER_DATE DATE, SPEND_QUARTILE NUMBER(38,0), FREQUENCY_QUARTILE NUMBER(38,0), CUSTOMER_SEGMENT VARCHAR(10));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_M_SUPPLIER_RANKING    (S_SUPPKEY NUMBER(38,0), S_NAME VARCHAR(25), NATION VARCHAR(25), REGION VARCHAR(25), ORDERS_FULFILLED NUMBER(38,0), TOTAL_QTY_SHIPPED NUMBER(38,4), TOTAL_REVENUE NUMBER(38,4), AVG_DELIVERY_DAYS NUMBER(10,2), LATE_SHIPMENTS NUMBER(38,0), TOTAL_SHIPMENTS NUMBER(38,0), LATE_PCT NUMBER(5,2), REVENUE_RANK_IN_REGION NUMBER(38,0), RELIABILITY_RANK_IN_REGION NUMBER(38,0));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_M_PART_PROFITABILITY  (P_PARTKEY NUMBER(38,0), P_NAME VARCHAR(55), P_TYPE VARCHAR(25), P_SIZE NUMBER(38,0), NET_REVENUE NUMBER(38,4), TOTAL_QTY_SOLD NUMBER(38,4), DISTINCT_ORDERS NUMBER(38,0), AVG_SUPPLY_COST NUMBER(38,4), SUPPLIER_COUNT NUMBER(38,0), ESTIMATED_PROFIT NUMBER(38,2), MARGIN_PCT NUMBER(10,2));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_M_ORDER_PIPELINE      (O_ORDERKEY NUMBER(38,0), O_CUSTKEY NUMBER(38,0), O_ORDERDATE DATE, O_ORDERSTATUS VARCHAR(1), O_TOTALPRICE NUMBER(38,4), O_ORDERPRIORITY VARCHAR(15), LINE_ITEM_COUNT NUMBER(38,0), TOTAL_QTY NUMBER(38,4), FIRST_SHIP_DATE DATE, LAST_SHIP_DATE DATE, LATE_ITEMS NUMBER(38,0), AVG_DAYS_TO_SHIP NUMBER(10,2), FULFILLMENT_SPAN_DAYS NUMBER(38,0), FULFILLMENT_STATUS VARCHAR(15), PRICE_PERCENTILE FLOAT);

CREATE OR REPLACE TRANSIENT TABLE ZW_T_C_MARKET_SHARE_ANALYSIS(REGION VARCHAR(25), NATION VARCHAR(25), ORDER_YEAR NUMBER(4,0), ORDER_QUARTER NUMBER(1,0), PART_TYPE VARCHAR(25), REVENUE NUMBER(38,4), REGION_TOTAL_REVENUE NUMBER(38,4), MARKET_SHARE_PCT NUMBER(10,4), ROLLING_4Q_AVG_SHARE NUMBER(10,4), SAME_QUARTER_PREV_YEAR_SHARE NUMBER(10,4), NATION_RANK_IN_REGION NUMBER(38,0), SHARE_CHANGE_YOY NUMBER(10,4), TREND_STATUS VARCHAR(7));
CREATE OR REPLACE TRANSIENT TABLE ZW_T_C_SUPPLY_CHAIN_RISK   (S_SUPPKEY NUMBER(38,0), S_NAME VARCHAR(25), SUPPLIER_NATION VARCHAR(25), SUPPLIER_REGION VARCHAR(25), TOTAL_LINES NUMBER(38,0), LATE_LINES NUMBER(38,0), LATE_PCT NUMBER(5,2), AVG_DELIVERY_VARIANCE_DAYS NUMBER(10,2), DELIVERY_VARIANCE_STDDEV FLOAT, WORST_DELAY_DAYS NUMBER(38,0), CUSTOMER_NATIONS_SERVED NUMBER(38,0), SINGLE_SOURCE_PARTS NUMBER(38,0), SUPPLIER_REVENUE NUMBER(38,4), REVENUE_CONCENTRATION_PCT FLOAT, RISK_SCORE NUMBER(5,2), RISK_TIER VARCHAR(8), OVERALL_RISK_RANK NUMBER(38,0), REGIONAL_RISK_RANK NUMBER(38,0));

-- =============================================================================
-- Results table
-- =============================================================================

USE SCHEMA ZW_SCH_ADMIN;

CREATE TABLE IF NOT EXISTS ZW_RESULTS (
    RUN_ID           VARCHAR,
    RUN_TS           TIMESTAMP_NTZ,
    SCENARIO         VARCHAR,
    WAREHOUSE_NAME   VARCHAR,
    QUERY_TEXT       VARCHAR,
    EXECUTION_STATUS VARCHAR,
    EXEC_SEC         FLOAT,
    QUEUED_SEC       FLOAT,
    ELAPSED_SEC      FLOAT,
    COMPLEXITY       VARCHAR,
    START_TIME       TIMESTAMP_LTZ,
    END_TIME         TIMESTAMP_LTZ,
    RUN_META         VARCHAR
);

-- =============================================================================
-- Stored procedures
-- =============================================================================

CREATE OR REPLACE PROCEDURE ZW_RUN_WORKLOAD("SCENARIO_NAME" VARCHAR, "SIMPLE_COUNT" NUMBER(38,0), "MEDIUM_COUNT" NUMBER(38,0), "COMPLEX_COUNT" NUMBER(38,0), "ADAPTIVE_WH" VARCHAR, "CLASSIC_WH" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_workload'
EXECUTE AS CALLER
AS '
import time
import uuid
import json
from datetime import datetime

def run_workload(session, scenario_name, simple_count, medium_count, complex_count, adaptive_wh, classic_wh):
    DB = "ZW_DB_ADAPTIVE"
    SCH_V = "ZW_SCH_VIEWS"

    SIMPLE_VIEWS = [
        "ZW_V_S_REVENUE_BY_NATION", "ZW_V_S_TOP_CUSTOMERS",
        "ZW_V_S_MONTHLY_ORDERS", "ZW_V_S_SUPPLIER_PARTS", "ZW_V_S_SHIPPING_PERFORMANCE"
    ]
    MEDIUM_VIEWS = [
        "ZW_V_M_REVENUE_YOY_GROWTH", "ZW_V_M_CUSTOMER_SEGMENTS",
        "ZW_V_M_SUPPLIER_RANKING", "ZW_V_M_PART_PROFITABILITY", "ZW_V_M_ORDER_PIPELINE"
    ]
    COMPLEX_VIEWS = [
        "ZW_V_C_MARKET_SHARE_ANALYSIS", "ZW_V_C_SUPPLY_CHAIN_RISK"
    ]

    all_views = (
        [SIMPLE_VIEWS[i % len(SIMPLE_VIEWS)] for i in range(simple_count)]
      + [MEDIUM_VIEWS[i % len(MEDIUM_VIEWS)] for i in range(medium_count)]
      + [COMPLEX_VIEWS[i % len(COMPLEX_VIEWS)] for i in range(complex_count)]
    )

    run_id = str(uuid.uuid4())
    run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    total_per_wh = len(all_views)

    a_rows = session.sql(f"SHOW WAREHOUSES LIKE ''{adaptive_wh}''").collect()
    c_rows = session.sql(f"SHOW WAREHOUSES LIKE ''{classic_wh}''").collect()
    a_meta = a_rows[0].as_dict() if a_rows else {}
    c_meta = c_rows[0].as_dict() if c_rows else {}
    run_meta = json.dumps({
        "adaptive": {
            "name": adaptive_wh,
            "type": str(a_meta.get("type", "")),
            "max_perf": str(a_meta.get("max_query_performance_level", "")),
            "throughput": str(a_meta.get("query_throughput_multiplier", ""))
        },
        "classic": {
            "name": classic_wh,
            "type": str(c_meta.get("type", "")),
            "size": str(c_meta.get("size", "")),
            "max_clusters": str(c_meta.get("max_cluster_count", ""))
        },
        "dataset": "TPCH_SF10"
    })
    run_meta_escaped = run_meta.replace("''", "''''")

    raw = session.connection

    raw.cursor().execute(f"USE WAREHOUSE {adaptive_wh}")
    adap_qids = []
    for v in all_views:
        cur = raw.cursor()
        cur.execute_async(f"SELECT *, RANDOM() AS _nc FROM {DB}.{SCH_V}.{v}")
        qid = cur.sfqid
        if qid:
            adap_qids.append(qid)

    raw.cursor().execute(f"USE WAREHOUSE {classic_wh}")
    cls_qids = []
    for v in all_views:
        cur = raw.cursor()
        cur.execute_async(f"SELECT *, RANDOM() AS _nc FROM {DB}.{SCH_V}.{v}")
        qid = cur.sfqid
        if qid:
            cls_qids.append(qid)

    raw.cursor().execute(f"USE WAREHOUSE {adaptive_wh}")

    all_qids = adap_qids + cls_qids
    all_qid_sql = ",".join(["''" + q + "''" for q in all_qids])

    # Poll via connector status (no ACCOUNT_USAGE latency)
    pending = set(all_qids)
    max_wait = 300
    elapsed = 0
    while pending and elapsed < max_wait:
        time.sleep(3)
        elapsed += 3
        done = set()
        for qid in list(pending):
            try:
                status = raw.get_query_status(qid)
                if not raw.is_still_running(status):
                    done.add(qid)
            except Exception:
                done.add(qid)
        pending -= done

    # Insert from INFORMATION_SCHEMA (near-real-time, same session)
    session.sql(
        f"INSERT INTO {DB}.ZW_SCH_ADMIN.ZW_RESULTS "
        f"SELECT "
        f"''{run_id}'', "
        f"''{run_ts}''::TIMESTAMP_NTZ, "
        f"''{scenario_name}'', "
        f"WAREHOUSE_NAME, "
        f"LEFT(QUERY_TEXT, 500), "
        f"EXECUTION_STATUS, "
        f"EXECUTION_TIME / 1000.0, "
        f"QUEUED_OVERLOAD_TIME / 1000.0, "
        f"TOTAL_ELAPSED_TIME / 1000.0, "
        f"CASE "
        f"  WHEN QUERY_TEXT ILIKE ''%ZW_V_S_%'' THEN ''Simple'' "
        f"  WHEN QUERY_TEXT ILIKE ''%ZW_V_M_%'' THEN ''Medium'' "
        f"  WHEN QUERY_TEXT ILIKE ''%ZW_V_C_%'' THEN ''Complex'' "
        f"  ELSE ''Other'' "
        f"END, "
        f"START_TIME, "
        f"END_TIME, "
        f"''{run_meta_escaped}'' "
        f"FROM TABLE({DB}.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10000)) "
        f"WHERE QUERY_ID IN ({all_qid_sql})"
    ).collect()

    return f"{run_id}|{total_per_wh * 2} queries submitted"
';

CREATE OR REPLACE PROCEDURE ZW_FIRE_BURST("WAREHOUSE_NAME" VARCHAR, "N_SIMPLE" NUMBER(38,0), "N_MEDIUM" NUMBER(38,0), "N_COMPLEX" NUMBER(38,0))
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
DB  = ''ZW_DB_ADAPTIVE''
SCH = ''ZW_SCH_VIEWS''
SIMPLE  = [''ZW_V_S_REVENUE_BY_NATION'',''ZW_V_S_TOP_CUSTOMERS'',''ZW_V_S_MONTHLY_ORDERS'',''ZW_V_S_SUPPLIER_PARTS'',''ZW_V_S_SHIPPING_PERFORMANCE'']
MEDIUM  = [''ZW_V_M_REVENUE_YOY_GROWTH'',''ZW_V_M_CUSTOMER_SEGMENTS'',''ZW_V_M_SUPPLIER_RANKING'',''ZW_V_M_PART_PROFITABILITY'',''ZW_V_M_ORDER_PIPELINE'']
COMPLEX = [''ZW_V_C_MARKET_SHARE_ANALYSIS'',''ZW_V_C_SUPPLY_CHAIN_RISK'']

def run(session, warehouse_name, n_simple, n_medium, n_complex):
    views = (
        [SIMPLE[i  % len(SIMPLE)]   for i in range(n_simple)]
      + [MEDIUM[i  % len(MEDIUM)]   for i in range(n_medium)]
      + [COMPLEX[i % len(COMPLEX)]  for i in range(n_complex)]
    )
    session.sql(f''USE WAREHOUSE {warehouse_name}'').collect()
    try:
        raw = session._conn._conn
        for v in views:
            sql = f''SELECT *, RANDOM() AS _nc FROM {DB}.{SCH}.{v}''
            raw.cursor().execute_async(sql)
    except Exception:
        for v in views:
            sql = f''SELECT *, RANDOM() AS _nc FROM {DB}.{SCH}.{v}''
            session.sql(sql).collect_nowait()
    return f''Fired {len(views)} queries to {warehouse_name}''
';
