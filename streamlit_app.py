import os

import os

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DB = "ZW_DB_ADAPTIVE"
SCH_A = "ZW_SCH_ADMIN"
TPC_DATASET = "TPCH_SF10"

SCENARIOS = {
    "Morning Rush": {
        "description": "20 analysts opening Monday morning dashboards simultaneously.",
        "simple": 16, "medium": 10, "complex": 6,
    },
    "End of Quarter Crunch": {
        "description": "Finance kicks off all quarterly reports — mixed heavy workload.",
        "simple": 10, "medium": 14, "complex": 10,
    },
    "Ad-hoc Storm": {
        "description": "Data science launches exploratory analysis across all workload types.",
        "simple": 6, "medium": 10, "complex": 14,
    },
    "dbt Transformation Run": {
        "description": "~90% lightweight queries alongside large full-refresh transformations.",
        "simple": 30, "medium": 0, "complex": 4,
    },
    "Grafana Dashboard Burst": {
        "description": "High-concurrency BI burst — many medium-complexity queries at once.",
        "simple": 4, "medium": 30, "complex": 0,
    },
    "Panic": {
        "description": "150 queries hitting both warehouses simultaneously.",
        "simple": 100, "medium": 40, "complex": 10,
    },
}

def _get_session():
    try:
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        return Session.builder.config("connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME", "SFSEEUROPE-DAMATTHEWS_USWEST2_2")).create()

if "session" not in st.session_state:
    st.session_state["session"] = _get_session()
session = st.session_state["session"]

def _safe_sql(query):
    try:
        return session.sql(query)
    except Exception as e:
        if "390114" in str(e) or "Authentication token has expired" in str(e):
            st.session_state["session"] = _get_session()
            return st.session_state["session"].sql(query)
        raise

st.set_page_config(page_title="Adaptive vs Classic Warehouse", layout="wide")

if "selected_run" not in st.session_state:
    st.session_state["selected_run"] = None

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Setup")

    st.markdown("**1. TPC Dataset**")
    tpc_options = ["TPCH_SF1", "TPCH_SF10", "TPCH_SF100", "TPCH_SF1000"]
    tpc_dataset = st.selectbox("Dataset", tpc_options, index=tpc_options.index(TPC_DATASET), key="tpc_dataset_select")

    st.markdown("**2. Classic Warehouse**")
    cls_size = st.selectbox("Size", ["X-Small", "Small", "Medium", "Large", "X-Large"], index=1, key="cls_size_select")
    cls_clusters = st.number_input("Max Clusters", min_value=1, max_value=10, value=3, key="cls_clusters_input")
    cls_wh = st.text_input("Classic WH", "ZW_CLASSIC_WH", key="classic_wh_input")

    st.markdown("**3. Create Adaptive from Classic**")
    adap_wh = st.text_input("Adaptive WH", "ZW_ADAPTIVE_WH", key="adaptive_wh_input")

    if st.button("Create Both", use_container_width=True):
        size_map = {"X-Small": "XSMALL", "Small": "SMALL", "Medium": "MEDIUM", "Large": "LARGE", "X-Large": "XLARGE"}
        sf_size = size_map[cls_size]
        try:
            session.sql(f"""
                CREATE OR REPLACE WAREHOUSE {cls_wh}
                    WAREHOUSE_SIZE = '{sf_size}'
                    MIN_CLUSTER_COUNT = 1
                    MAX_CLUSTER_COUNT = {cls_clusters}
                    SCALING_POLICY = 'STANDARD'
                    AUTO_SUSPEND = 60
                    AUTO_RESUME = TRUE
                    ENABLE_QUERY_ACCELERATION = TRUE
            """).collect()
            session.sql(f"""
                CREATE OR REPLACE WAREHOUSE {adap_wh}
                    WAREHOUSE_SIZE = '{sf_size}'
                    AUTO_RESUME = TRUE
            """).collect()
            session.sql(f"""
                ALTER WAREHOUSE {adap_wh} SET WAREHOUSE_TYPE = 'ADAPTIVE'
            """).collect()
            st.success(f"Created both warehouses ({sf_size})")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")

# ── Header ────────────────────────────────────────────────────────────────────
st.title("⚡ Adaptive vs Classic Warehouse")

@st.cache_data(ttl=30)
def _wh_info(wh):
    try:
        rows = session.sql(f"SHOW WAREHOUSES LIKE '{wh}'").collect()
        return rows[0].as_dict() if rows else {}
    except Exception:
        return {}

a_info = _wh_info(adap_wh)
c_info = _wh_info(cls_wh)

col_a, col_c = st.columns(2)
with col_a:
    if a_info:
        st.markdown(
            f"**🔵 Adaptive** — `{adap_wh}` · State: **{a_info.get('state', '—')}** · "
            f"Max Perf: **{a_info.get('max_query_performance_level', '—')}** · "
            f"Throughput: **{a_info.get('query_throughput_multiplier', '—')}x**"
        )
with col_c:
    if c_info:
        st.markdown(
            f"**🟠 Classic** — `{cls_wh}` · State: **{c_info.get('state', '—')}** · "
            f"Size: **{c_info.get('size', '—')}** · "
            f"Clusters: **{c_info.get('min_cluster_count', '—')}/{c_info.get('max_cluster_count', '—')}**"
        )

# ── Controls ──────────────────────────────────────────────────────────────────
sc_col, btn_col = st.columns([4, 2])
with sc_col:
    scenario = st.selectbox("Scenario", list(SCENARIOS.keys()), key="scenario_select", label_visibility="collapsed")
with btn_col:
    run_clicked = st.button("▶ Run Workload", type="primary", use_container_width=True)

sc = SCENARIOS[scenario]
total_q = sc["simple"] + sc["medium"] + sc["complex"]
st.caption(f"*{sc['description']}* — **{total_q}** queries/wh: {sc['simple']}S · {sc['medium']}M · {sc['complex']}C")

if run_clicked:
    with st.spinner(f"Running {total_q * 2} queries across both warehouses..."):
        result = session.sql(f"""
            CALL {DB}.{SCH_A}.ZW_RUN_WORKLOAD(
                '{scenario}', {sc['simple']}, {sc['medium']}, {sc['complex']},
                '{adap_wh}', '{cls_wh}'
            )
        """).collect()
        run_id = str(result[0][0]).split("|")[0] if result else None
        if run_id:
            st.session_state["selected_run"] = run_id
            st.cache_data.clear()
            st.rerun()

# ── Run selector ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=5)
def get_runs():
    return session.sql(f"""
        SELECT RUN_ID, RUN_TS, SCENARIO,
               ANY_VALUE(RUN_META) AS RUN_META
        FROM {DB}.{SCH_A}.ZW_RESULTS
        GROUP BY RUN_ID, RUN_TS, SCENARIO
        ORDER BY RUN_TS DESC
        LIMIT 20
    """).to_pandas()

runs_df = get_runs()

if runs_df.empty:
    st.info("No runs yet. Click **Run Workload** to start.")
    st.stop()

def _build_run_label(row):
    ts = str(row["RUN_TS"])[:19]
    meta = row.get("RUN_META")
    if meta:
        try:
            import json
            m = json.loads(meta)
            a = m.get("adaptive", {})
            c = m.get("classic", {})
            dataset = m.get("dataset", "")
            a_desc = f"Adaptive {a.get('max_perf', '?')} {a.get('throughput', '?')}x"
            c_desc = f"Classic {c.get('size', '?')} {c.get('max_clusters', '?')}cl"
            return f"{row['SCENARIO']} — {a_desc} vs {c_desc} — {dataset} — {ts}"
        except Exception:
            pass
    a_desc = f"Adaptive {a_info.get('max_query_performance_level', '?')} {a_info.get('query_throughput_multiplier', '?')}x" if a_info else "Adaptive"
    c_size = c_info.get('size', '?')
    c_max = c_info.get('max_cluster_count', '?')
    c_desc = f"Classic {c_size} {c_max}cl" if c_info else "Classic"
    return f"{row['SCENARIO']} — {a_desc} vs {c_desc} — {TPC_DATASET} — {ts}"

runs_df["LABEL"] = runs_df.apply(_build_run_label, axis=1)
run_options = runs_df["LABEL"].tolist()
run_ids = runs_df["RUN_ID"].tolist()

if st.session_state["selected_run"] in run_ids:
    default_idx = run_ids.index(st.session_state["selected_run"])
else:
    default_idx = 0
    st.session_state["selected_run"] = run_ids[0]

selected_label = st.selectbox("Past Runs", run_options, index=default_idx, key="run_selector")
selected_run_id = run_ids[run_options.index(selected_label)]
st.session_state["selected_run"] = selected_run_id

# ── Load results ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=5)
def load_results(run_id):
    return session.sql(f"""
        SELECT * FROM {DB}.{SCH_A}.ZW_RESULTS
        WHERE RUN_ID = '{run_id}'
    """).to_pandas()

df = load_results(selected_run_id)

if df.empty:
    st.warning("No results for this run yet.")
    st.stop()

for col in ["EXEC_SEC", "QUEUED_SEC", "ELAPSED_SEC"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

adap_df = df[df["WAREHOUSE_NAME"] == adap_wh].copy()
cls_df = df[df["WAREHOUSE_NAME"] == cls_wh].copy()

# ── Metrics ───────────────────────────────────────────────────────────────────
def _stat(d, col, fn):
    if d.empty or col not in d.columns:
        return None
    vals = d[d["EXECUTION_STATUS"] == "SUCCESS"][col].dropna()
    if vals.empty:
        return None
    return round(float(fn(vals)), 2)

def _fmt(v):
    return f"{v:.2f}" if v is not None else "—"

# ── Outstanding Queries Over Time ─────────────────────────────────────────────
if "END_TIME" in df.columns and df["END_TIME"].notna().any():
    st.markdown("#### Outstanding Queries Over Time")
    adap_ends = adap_df[adap_df["EXECUTION_STATUS"] == "SUCCESS"]["END_TIME"].dropna().sort_values()
    cls_ends = cls_df[cls_df["EXECUTION_STATUS"] == "SUCCESS"]["END_TIME"].dropna().sort_values()
    all_ends = df[df["EXECUTION_STATUS"] == "SUCCESS"]["END_TIME"].dropna()
    if not all_ends.empty:
        min_start = df[df["EXECUTION_STATUS"] == "SUCCESS"]["START_TIME"].dropna().min()
        max_end = all_ends.max()
        total_sec = int((max_end - min_start).total_seconds()) + 1
        a_total = len(adap_ends)
        c_total = len(cls_ends)
        rows = []
        for sec in range(0, total_sec + 1):
            cutoff = min_start + pd.Timedelta(seconds=sec)
            a_done = int((adap_ends <= cutoff).sum())
            c_done = int((cls_ends <= cutoff).sum())
            rows.append({"Elapsed (s)": sec, "Adaptive": a_total - a_done, "Classic": c_total - c_done})
        chart_df = pd.DataFrame(rows).set_index("Elapsed (s)")
        st.line_chart(chart_df, height=200)
        st.caption(f"Queries remaining over time (started with {a_total}/{c_total} per warehouse)")

# ── Metrics + Distribution ────────────────────────────────────────────────────
left_col, right_col = st.columns(2)

with left_col:
    st.markdown("#### Performance Metrics")
    metrics = {
        "Metric": ["Total Queries", "Avg Elapsed (s)", "Avg Exec (s)", "Avg Queue (s)", "P90 Exec (s)", "Max Exec (s)"],
        "🔵 Adaptive": [
            str(len(adap_df[adap_df["EXECUTION_STATUS"] == "SUCCESS"])),
            _fmt(_stat(adap_df, "ELAPSED_SEC", lambda s: s.mean())),
            _fmt(_stat(adap_df, "EXEC_SEC", lambda s: s.mean())),
            _fmt(_stat(adap_df, "QUEUED_SEC", lambda s: s.mean())),
            _fmt(_stat(adap_df, "EXEC_SEC", lambda s: s.quantile(0.90))),
            _fmt(_stat(adap_df, "EXEC_SEC", lambda s: s.max())),
        ],
        "🟠 Classic": [
            str(len(cls_df[cls_df["EXECUTION_STATUS"] == "SUCCESS"])),
            _fmt(_stat(cls_df, "ELAPSED_SEC", lambda s: s.mean())),
            _fmt(_stat(cls_df, "EXEC_SEC", lambda s: s.mean())),
            _fmt(_stat(cls_df, "QUEUED_SEC", lambda s: s.mean())),
            _fmt(_stat(cls_df, "EXEC_SEC", lambda s: s.quantile(0.90))),
            _fmt(_stat(cls_df, "EXEC_SEC", lambda s: s.max())),
        ],
    }
    st.dataframe(metrics, use_container_width=True, hide_index=True)

with right_col:
    st.markdown("#### Execution Time Distribution")

    def _make_hist(d, label):
        done = d[(d["EXECUTION_STATUS"] == "SUCCESS") & (d["EXEC_SEC"] > 0) & (d["COMPLEXITY"] != "Other")].copy()
        if done.empty:
            return None
        done["EXEC_SEC"] = done["EXEC_SEC"].astype(float)
        bins = pd.cut(done["EXEC_SEC"], bins=15)
        hist = done.groupby([bins, "COMPLEXITY"], observed=True).size().unstack(fill_value=0)
        hist.index = [f"{i.left:.0f}-{i.right:.0f}" for i in hist.index]
        return hist

    c1, c2 = st.columns(2)
    with c1:
        st.caption(f"🔵 Adaptive")
        hist = _make_hist(adap_df, "Adaptive")
        if hist is not None:
            st.bar_chart(hist, height=180)
        else:
            st.caption("No data")
    with c2:
        st.caption(f"🟠 Classic")
        hist = _make_hist(cls_df, "Classic")
        if hist is not None:
            st.bar_chart(hist, height=180)
        else:
            st.caption("No data")

# ── Per-complexity breakdown ──────────────────────────────────────────────────
st.markdown("#### By Complexity")
comp_cols = st.columns(3)
for i, comp in enumerate(["Simple", "Medium", "Complex"]):
    with comp_cols[i]:
        a_comp = adap_df[(adap_df["COMPLEXITY"] == comp) & (adap_df["EXECUTION_STATUS"] == "SUCCESS")]
        c_comp = cls_df[(cls_df["COMPLEXITY"] == comp) & (cls_df["EXECUTION_STATUS"] == "SUCCESS")]
        a_avg = f"{a_comp['EXEC_SEC'].mean():.1f}" if not a_comp.empty else "—"
        c_avg = f"{c_comp['EXEC_SEC'].mean():.1f}" if not c_comp.empty else "—"
        a_q = f"{a_comp['QUEUED_SEC'].mean():.1f}" if not a_comp.empty else "—"
        c_q = f"{c_comp['QUEUED_SEC'].mean():.1f}" if not c_comp.empty else "—"
        st.markdown(f"**{comp}**")
        st.markdown(f"Exec: 🔵 {a_avg}s · 🟠 {c_avg}s")
        st.markdown(f"Queue: 🔵 {a_q}s · 🟠 {c_q}s")

# ── Credit Consumption ────────────────────────────────────────────────────────
st.markdown("#### Credit Consumption (Last 12 Hours)")

@st.cache_data(ttl=60)
def get_credits_combined(wh_a, wh_c):
    return session.sql(f"""
        SELECT DATE_TRUNC('HOUR', START_TIME) AS HOUR, WAREHOUSE_NAME, SUM(CREDITS_USED) AS CREDITS
        FROM (
            SELECT * FROM TABLE({DB}.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
                DATE_RANGE_START => DATEADD('HOUR', -12, CURRENT_TIMESTAMP()),
                WAREHOUSE_NAME => '{wh_a}'
            ))
            UNION ALL
            SELECT * FROM TABLE({DB}.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
                DATE_RANGE_START => DATEADD('HOUR', -12, CURRENT_TIMESTAMP()),
                WAREHOUSE_NAME => '{wh_c}'
            ))
        )
        GROUP BY 1, 2 ORDER BY 1
    """).to_pandas()

cred_df = get_credits_combined(adap_wh, cls_wh)
if not cred_df.empty:
    cred_df["Warehouse"] = cred_df["WAREHOUSE_NAME"].map({adap_wh: "Adaptive", cls_wh: "Classic"})
    cred_df["Hour"] = cred_df["HOUR"].dt.strftime("%H:%M")
    chart = alt.Chart(cred_df).mark_bar().encode(
        x=alt.X("Hour:N", title="Hour"),
        y=alt.Y("CREDITS:Q", title="Credits"),
        color=alt.Color("Warehouse:N", scale=alt.Scale(domain=["Adaptive", "Classic"], range=["#1f77b4", "#ff7f0e"])),
        xOffset="Warehouse:N"
    ).properties(height=250)
    st.altair_chart(chart, use_container_width=True)
    a_total = cred_df[cred_df["Warehouse"] == "Adaptive"]["CREDITS"].sum()
    c_total = cred_df[cred_df["Warehouse"] == "Classic"]["CREDITS"].sum()
    st.caption(f"Total credits — 🔵 Adaptive: {a_total:.2f} · 🟠 Classic: {c_total:.2f}")
else:
    st.info("No credit data yet (account usage may have ~45 min latency)")
