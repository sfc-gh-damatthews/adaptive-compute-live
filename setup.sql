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

-- Driver: the app and ZW_RUN_WORKLOAD run here, never on a warehouse under test.
-- The procedure spends most of its life polling for async query completion; billing
-- that to a measured warehouse would both cost money and skew the comparison.
CREATE WAREHOUSE IF NOT EXISTS ZW_DRIVER_WH
  WITH
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Neutral driver warehouse for ZW_RUN_WORKLOAD.';

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

CREATE OR REPLACE PROCEDURE ZW_RUN_WORKLOAD("SCENARIO_NAME" VARCHAR, "SIMPLE_COUNT" NUMBER(38,0), "MEDIUM_COUNT" NUMBER(38,0), "COMPLEX_COUNT" NUMBER(38,0), "WH_A" VARCHAR, "WH_B" VARCHAR, "RUN_META_JSON" VARCHAR)
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

def run_workload(session, scenario_name, simple_count, medium_count, complex_count, wh_a, wh_b, run_meta_json):
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

    # SHOW WAREHOUSES reports sizes with different keywords to the DDL
    SIZE_ALIASES = {"2XLARGE": "XXLARGE", "3XLARGE": "XXXLARGE", "4XLARGE": "X4LARGE"}

    def canon_size(raw_size):
        s = str(raw_size).upper().replace("-", "").replace(" ", "")
        return SIZE_ALIASES.get(s, s)

    def describe(name):
        # Build a label from the live warehouse, so calling this procedure straight
        # from SQL still records what actually ran.
        try:
            rows = session.sql("SHOW WAREHOUSES LIKE ''" + name + "''").collect()
            m = rows[0].as_dict() if rows else {}
        except Exception:
            m = {}
        wtype = str(m.get("type", "")).upper()
        if wtype == "ADAPTIVE":
            perf = canon_size(m.get("max_query_performance_level", "?"))
            thr = m.get("query_throughput_multiplier", "?")
            try:
                thr_s = "unlimited" if int(thr) == 0 else str(int(thr)) + "x"
            except (TypeError, ValueError):
                thr_s = str(thr) + "x"
            return {"name": name, "type": "adaptive", "label": "Adaptive " + perf + " " + thr_s}
        gen = str(m.get("generation", "?"))
        size = canon_size(m.get("size", "?"))
        maxc = str(m.get("max_cluster_count", "?"))
        return {"name": name, "type": "standard",
                "label": "Gen" + gen + " " + size + " " + maxc + "cl"}

    # Callers that already know the config (the Streamlit app) pass it in. Called
    # directly from SQL the argument is NULL, so derive it here instead.
    if not run_meta_json:
        run_meta_json = json.dumps({
            "mode": "direct SQL",
            "side_a": describe(wh_a),
            "side_b": describe(wh_b),
            "dataset": "TPCH_SF10"
        })

    run_meta_escaped = (run_meta_json or "{}").replace("''", "''''")

    try:
        _m = json.loads(run_meta_json or "{}")
    except Exception:
        _m = {}
    label_a = str((_m.get("side_a") or {}).get("label", wh_a))
    label_b = str((_m.get("side_b") or {}).get("label", wh_b))

    raw = session.connection

    def sql(stmt):
        raw.cursor().execute(stmt)

    def set_tag(side, wh, label):
        # QUERY_TAG lands in QUERY_HISTORY and QUERY_ATTRIBUTION_HISTORY, so the
        # workload queries can be costed per side and joined back on run_id.
        try:
            tag = json.dumps({
                "demo": "zw_adaptive_compare",
                "run_id": run_id,
                "scenario": scenario_name,
                "side": side,
                "wh": wh,
                "label": label
            }).replace("''", "''''")
            sql("ALTER SESSION SET QUERY_TAG = ''" + tag + "''")
        except Exception:
            pass

    def clear_tag():
        try:
            sql("ALTER SESSION UNSET QUERY_TAG")
        except Exception:
            pass

    # Remember the caller warehouse. Without this the USE WAREHOUSE below leaks
    # into the session, so the next CALL executes on a warehouse under test and
    # charges its polling time to one side of the comparison.
    orig_wh = None
    try:
        row = raw.cursor().execute("SELECT CURRENT_WAREHOUSE()").fetchone()
        orig_wh = row[0] if row and row[0] else None
    except Exception:
        pass

    def target(wh, side, label):
        # Clear before switching so the USE WAREHOUSE statement itself is not
        # attributed to the wrong side, then tag only the workload that follows.
        clear_tag()
        sql("USE WAREHOUSE " + wh)
        set_tag(side, wh, label)

    a_qids = []
    b_qids = []
    try:
        target(wh_a, "A", label_a)
        for v in all_views:
            cur = raw.cursor()
            cur.execute_async("SELECT *, RANDOM() AS _nc FROM " + DB + "." + SCH_V + "." + v)
            if cur.sfqid:
                a_qids.append(cur.sfqid)

        target(wh_b, "B", label_b)
        for v in all_views:
            cur = raw.cursor()
            cur.execute_async("SELECT *, RANDOM() AS _nc FROM " + DB + "." + SCH_V + "." + v)
            if cur.sfqid:
                b_qids.append(cur.sfqid)
    finally:
        # Hand the session back before polling so the wait and the INSERT are not
        # billed to either warehouse under test.
        clear_tag()
        if orig_wh:
            try:
                sql("USE WAREHOUSE " + orig_wh)
            except Exception:
                pass

    all_qids = a_qids + b_qids
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

-- =============================================================================
-- Analysis views
-- =============================================================================

-- One row per run per warehouse. Turns comparison into a single SELECT.
CREATE OR REPLACE VIEW ZW_V_RUN_SUMMARY AS
SELECT
    r.RUN_ID,
    r.RUN_TS,
    r.SCENARIO,
    r.WAREHOUSE_NAME,
    -- Side identity and labels come from the run's own metadata, so historic runs
    -- describe the configuration they actually used. Falls back to the legacy
    -- adaptive/classic keys for rows written before the side_a/side_b schema.
    COALESCE(
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'side_a.name')::VARCHAR,
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'adaptive.name')::VARCHAR
    )                                              AS SIDE_A_WH,
    COALESCE(
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'side_b.name')::VARCHAR,
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'classic.name')::VARCHAR
    )                                              AS SIDE_B_WH,
    COALESCE(
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'side_a.label')::VARCHAR,
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'adaptive.label')::VARCHAR
    )                                              AS SIDE_A_LABEL,
    COALESCE(
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'side_b.label')::VARCHAR,
        GET_PATH(TRY_PARSE_JSON(r.RUN_META), 'classic.label')::VARCHAR
    )                                              AS SIDE_B_LABEL,
    COUNT(*)                                       AS QUERIES,
    ROUND(AVG(r.EXEC_SEC), 2)                      AS AVG_EXEC_SEC,
    ROUND(AVG(r.QUEUED_SEC), 2)                    AS AVG_QUEUE_SEC,
    ROUND(MAX(r.QUEUED_SEC), 2)                    AS MAX_QUEUE_SEC,
    ROUND(AVG(r.ELAPSED_SEC), 2)                   AS AVG_ELAPSED_SEC,
    ROUND(APPROX_PERCENTILE(r.EXEC_SEC, 0.90), 2)  AS P90_EXEC_SEC,
    ROUND(MAX(r.EXEC_SEC), 2)                      AS MAX_EXEC_SEC
FROM ZW_RESULTS r
WHERE r.EXECUTION_STATUS = 'SUCCESS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

-- Side-by-side: one row per run with both warehouses on the same line.
CREATE OR REPLACE VIEW ZW_V_RUN_COMPARISON AS
WITH s AS (SELECT * FROM ZW_V_RUN_SUMMARY)
SELECT
    a.RUN_TS,
    a.SCENARIO,
    a.SIDE_A_LABEL                           AS SIDE_A,
    b.SIDE_B_LABEL                           AS SIDE_B,
    a.QUERIES                                AS QUERIES_PER_SIDE,
    a.AVG_EXEC_SEC                           AS A_AVG_EXEC,
    b.AVG_EXEC_SEC                           AS B_AVG_EXEC,
    a.AVG_QUEUE_SEC                          AS A_AVG_QUEUE,
    b.AVG_QUEUE_SEC                          AS B_AVG_QUEUE,
    a.AVG_ELAPSED_SEC                        AS A_AVG_ELAPSED,
    b.AVG_ELAPSED_SEC                        AS B_AVG_ELAPSED,
    -- >1 means side B was slower end to end
    ROUND(b.AVG_ELAPSED_SEC / NULLIF(a.AVG_ELAPSED_SEC, 0), 2) AS B_OVER_A_ELAPSED,
    a.RUN_ID
FROM s a
JOIN s b
  ON a.RUN_ID = b.RUN_ID
 AND a.WAREHOUSE_NAME = a.SIDE_A_WH
 AND b.WAREHOUSE_NAME = b.SIDE_B_WH;

-- =============================================================================
-- DEMO USAGE
--
-- Everything above is setup and only needs running once. From here down the
-- statements are the demo itself: reconfigure the pair, fire a burst, compare.
--
-- Always drive from ZW_DRIVER_WH. The procedure spends most of its life polling
-- for async completion, and running that on a warehouse under test would both
-- cost credits and skew its own measurement.
--
-- Two adaptive properties matter, and they are orthogonal, so each needs the
-- opposite shape of workload to show up:
--
--   QUERY_THROUGHPUT_MULTIPLIER  how much runs at once -> many cheap queries
--   MAX_QUERY_PERFORMANCE_LEVEL  how fast one query goes -> few heavy queries
-- =============================================================================

USE WAREHOUSE ZW_DRIVER_WH;
USE SCHEMA ZW_DB_ADAPTIVE.ZW_SCH_ADMIN;

-- Pass NULL for the last argument and the procedure describes the warehouses
-- itself from SHOW WAREHOUSES. The Streamlit app passes real JSON instead, since
-- it already knows the configuration it applied.
--
-- (A 6-argument SQL wrapper was tried so NULL could be omitted, but a SQL
-- procedure calling this Python one runs it without caller rights, which breaks
-- its USE WAREHOUSE. Passing NULL explicitly avoids that.)


-- -----------------------------------------------------------------------------
-- Demo 1: Standard vs Adaptive
--
-- Matched cost envelope. 100 light queries swamp Standard's ~24 concurrent slots
-- (MAX_CONCURRENCY_LEVEL 8 x 3 clusters) while Adaptive absorbs them.
--
-- Expect: AVG_EXEC near identical on both, which is the control that proves
-- neither side got more per-query compute. The whole difference lands in
-- AVG_QUEUE. Measured 0.0s vs 6.1s, worst case 18.5s.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE ZW_ADAPTIVE_WH
    WAREHOUSE_TYPE = 'ADAPTIVE'
    MAX_QUERY_PERFORMANCE_LEVEL = SMALL
    QUERY_THROUGHPUT_MULTIPLIER = 5;

CREATE OR REPLACE WAREHOUSE ZW_CLASSIC_WH
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'SMALL'
    GENERATION = '2'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    MAX_CONCURRENCY_LEVEL = 8
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    ENABLE_QUERY_ACCELERATION = TRUE;

USE WAREHOUSE ZW_DRIVER_WH;
CALL ZW_RUN_WORKLOAD('Small Burst', 100, 0, 0, 'ZW_ADAPTIVE_WH', 'ZW_CLASSIC_WH', NULL);

-- -----------------------------------------------------------------------------
-- Demo 2: Query throughput multiplier
--
-- Both sides adaptive at the same performance level, so the multiplier is the
-- only variable.
--
-- Expect: AVG_QUEUE diverges (measured 1.5s at 2x vs 0.0s at 10x) while AVG_EXEC
-- moves far less. The multiplier buys admission, not speed. Exec does shift a
-- little because queued queries land in a busier warehouse.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE ZW_ADAPTIVE_WH
    WAREHOUSE_TYPE = 'ADAPTIVE'
    MAX_QUERY_PERFORMANCE_LEVEL = LARGE
    QUERY_THROUGHPUT_MULTIPLIER = 2;

CREATE OR REPLACE WAREHOUSE ZW_WH_B
    WAREHOUSE_TYPE = 'ADAPTIVE'
    MAX_QUERY_PERFORMANCE_LEVEL = LARGE
    QUERY_THROUGHPUT_MULTIPLIER = 10;

USE WAREHOUSE ZW_DRIVER_WH;
CALL ZW_RUN_WORKLOAD('Small Burst', 100, 0, 0, 'ZW_ADAPTIVE_WH', 'ZW_WH_B', NULL);

-- -----------------------------------------------------------------------------
-- Demo 3: Max query performance level
--
-- Both sides adaptive at the same multiplier. Only 3 queries, so queuing cannot
-- account for any difference.
--
-- Expect: AVG_EXEC drops sharply on the X4LARGE side (measured 10.9s vs 2.6s)
-- with queue at 0.0s on both. Note Snowflake only optimises up to the ceiling
-- when it is confident, so a higher ceiling permits speed rather than
-- guaranteeing it.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE ZW_ADAPTIVE_WH
    WAREHOUSE_TYPE = 'ADAPTIVE'
    MAX_QUERY_PERFORMANCE_LEVEL = XSMALL
    QUERY_THROUGHPUT_MULTIPLIER = 4;

CREATE OR REPLACE WAREHOUSE ZW_WH_B
    WAREHOUSE_TYPE = 'ADAPTIVE'
    MAX_QUERY_PERFORMANCE_LEVEL = X4LARGE
    QUERY_THROUGHPUT_MULTIPLIER = 4;

USE WAREHOUSE ZW_DRIVER_WH;
CALL ZW_RUN_WORKLOAD('Heavy Single', 0, 0, 3, 'ZW_ADAPTIVE_WH', 'ZW_WH_B', NULL);

-- =============================================================================
-- RESULTS
-- =============================================================================

-- Side by side, most recent first
SELECT * FROM ZW_V_RUN_COMPARISON ORDER BY RUN_TS DESC;

-- Per warehouse detail
SELECT * FROM ZW_V_RUN_SUMMARY ORDER BY RUN_TS DESC, WAREHOUSE_NAME;

-- Break a single run down by query complexity
SELECT WAREHOUSE_NAME, COMPLEXITY, COUNT(*) AS QUERIES,
       ROUND(AVG(EXEC_SEC), 2)   AS AVG_EXEC_SEC,
       ROUND(AVG(QUEUED_SEC), 2) AS AVG_QUEUE_SEC
FROM ZW_RESULTS
WHERE RUN_ID = (SELECT RUN_ID FROM ZW_RESULTS ORDER BY RUN_TS DESC LIMIT 1)
  AND EXECUTION_STATUS = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Credits per side for the last run. Workload queries carry a JSON QUERY_TAG,
-- and QUERY_TAG reaches QUERY_ATTRIBUTION_HISTORY, which is per query and does
-- carry credits. Note QUERY_TAG does NOT reach WAREHOUSE_METERING_HISTORY, so
-- this is not a general chargeback mechanism.
-- Adaptive credits surface via QUERY_METERING_HISTORY instead, with up to an
-- hour of latency, so the adaptive side may be empty immediately after a run.
SELECT
    GET_PATH(TRY_PARSE_JSON(QUERY_TAG), 'side')::VARCHAR  AS SIDE,
    GET_PATH(TRY_PARSE_JSON(QUERY_TAG), 'label')::VARCHAR AS LABEL,
    WAREHOUSE_NAME,
    COUNT(*)                                              AS QUERIES,
    ROUND(SUM(CREDITS_ATTRIBUTED_COMPUTE), 6)             AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
WHERE QUERY_TAG ILIKE '%zw_adaptive_compare%'
  AND START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY 1;

-- =============================================================================
-- TEARDOWN
-- =============================================================================
-- DROP WAREHOUSE IF EXISTS ZW_ADAPTIVE_WH;
-- DROP WAREHOUSE IF EXISTS ZW_CLASSIC_WH;
-- DROP WAREHOUSE IF EXISTS ZW_WH_A;
-- DROP WAREHOUSE IF EXISTS ZW_WH_B;
-- DROP WAREHOUSE IF EXISTS ZW_DRIVER_WH;
-- DROP DATABASE IF EXISTS ZW_DB_ADAPTIVE;
