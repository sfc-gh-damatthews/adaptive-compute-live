import json
import os

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DB = "ZW_DB_ADAPTIVE"
SCH_A = "ZW_SCH_ADMIN"
TPC_DATASET = "TPCH_SF10"

# Neutral warehouse the procedure itself runs on, so its polling and insert
# overhead is never billed to (or measured against) either side of the comparison.
DRIVER_WH = "ZW_DRIVER_WH"

# Comparison modes → (side A warehouse type, side B warehouse type)
MODES = {
    "Adaptive vs Standard": ("adaptive", "standard"),
    "Adaptive vs Adaptive": ("adaptive", "adaptive"),
    "Standard vs Standard": ("standard", "standard"),
}

# Adaptive MAX_QUERY_PERFORMANCE_LEVEL and standard WAREHOUSE_SIZE share this scale
SIZES = ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE"]


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
    "Small Burst": {
        "description": "100 lightweight queries at once — pure concurrency pressure.",
        "simple": 100, "medium": 0, "complex": 0,
    },
    "Medium Burst": {
        "description": "50 mid-weight queries at once — balanced concurrency and compute.",
        "simple": 0, "medium": 50, "complex": 0,
    },
    "Complex Burst": {
        "description": "25 heavy analytical queries at once — maximum compute pressure.",
        "simple": 0, "medium": 0, "complex": 25,
    },
    "Heavy Single": {
        "description": "3 heavy queries only — low concurrency, so per-query speed is isolated from queuing.",
        "simple": 0, "medium": 0, "complex": 3,
    },
}

# Preset configurations for the three headline demos. Each sets the comparison
# mode, both sides' tuning, and the scenario that actually exposes the mechanism.
DEMO_PRESETS = {
    "Custom (manual setup)": None,
    "1 · Standard vs Adaptive": {
        "mode": "Adaptive vs Standard",
        "scenario": "Small Burst",
        "wh_a": "ZW_ADAPTIVE_WH",
        "wh_b": "ZW_CLASSIC_WH",
        "a": {"max_perf": "SMALL", "throughput": 5},
        "b": {"size": "SMALL", "generation": "2", "min_clusters": 1, "max_clusters": 3,
              "scaling_policy": "STANDARD", "max_concurrency": 8,
              "qas": True, "qas_scale": 8, "auto_suspend": 60, "auto_resume": True},
        "watch": "**Avg Exec is near identical** (3.5s vs 3.6s measured) which proves neither "
                 "side got more per-query compute — so the whole difference is **Queue**: 0.0s vs "
                 "6.1s average, 18.5s worst case, and Avg Elapsed 4.5s vs 10.1s. 100 light queries "
                 "swamp Standard's ~24 concurrent slots (8 x 3 clusters). Watch the **Outstanding "
                 "Queries** curve: the Standard plateau is cluster spin-up lag, which Adaptive "
                 "does not have.",
    },
    "2 · Query Throughput Multiplier": {
        "mode": "Adaptive vs Adaptive",
        "scenario": "Small Burst",
        "wh_a": "ZW_ADAPTIVE_WH",
        "wh_b": "ZW_WH_B",
        "a": {"max_perf": "LARGE", "throughput": 2},
        "b": {"max_perf": "LARGE", "throughput": 10},
        "watch": "**Avg Queue** is where the effect concentrates — measured 1.5s at 2x "
                 "versus 0.0s at 10x, with Avg Elapsed 3.5s versus 1.4s. Perf level is held "
                 "identical, so the multiplier is the only variable: it buys admission, not "
                 "raw speed. Exec time does shift a little too (1.3s versus 0.9s) since queued "
                 "queries land in a more contended warehouse. Use Panic for a wider gap.",
    },
    "3 · Max Query Performance Level": {
        "mode": "Adaptive vs Adaptive",
        "scenario": "Heavy Single",
        "wh_a": "ZW_ADAPTIVE_WH",
        "wh_b": "ZW_WH_B",
        "a": {"max_perf": "XSMALL", "throughput": 4},
        "b": {"max_perf": "X4LARGE", "throughput": 4},
        "watch": "**Avg / P90 / Max Exec** drop sharply on the X4LARGE side — measured 10.9s "
                 "versus 2.6s, a 4.2x gap — with **Queue at 0.0s on both**. Multiplier is held "
                 "identical and only 3 queries run, so neither queuing nor admission can explain "
                 "it: this is purely per-query performance headroom.",
    },
}

# cfg field -> render_config widget key suffix
FIELD_KEYS = {
    "max_perf": "perf", "throughput": "thr",
    "size": "size", "generation": "gen", "min_clusters": "minc", "max_clusters": "maxc",
    "scaling_policy": "scale", "max_concurrency": "conc",
    "qas": "qas", "qas_scale": "qasf", "auto_suspend": "susp", "auto_resume": "res",
    "stmt_timeout": "stmt", "queued_timeout": "qd",
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

st.set_page_config(page_title="Warehouse Comparison", layout="wide")

if "selected_run" not in st.session_state:
    st.session_state["selected_run"] = None


# ── Config builders ───────────────────────────────────────────────────────────
def build_label(wh_type, cfg):
    """Short self-documenting label derived from the config, e.g. 'Adaptive LARGE 5x'."""
    if wh_type == "adaptive":
        thr = "unlimited" if int(cfg["throughput"]) == 0 else f"{cfg['throughput']}x"
        return f"Adaptive {cfg['max_perf']} {thr}"
    return f"Gen{cfg['generation']} {cfg['size']} {cfg['max_clusters']}cl"


def build_create_sql(name, wh_type, cfg):
    """DDL for one warehouse. Adaptive and standard accept mutually exclusive
    property sets, so each type emits only its own valid properties."""
    if wh_type == "adaptive":
        return f"""
            CREATE OR REPLACE WAREHOUSE {name}
                WAREHOUSE_TYPE = 'ADAPTIVE'
                MAX_QUERY_PERFORMANCE_LEVEL = {cfg['max_perf']}
                QUERY_THROUGHPUT_MULTIPLIER = {cfg['throughput']}
                STATEMENT_TIMEOUT_IN_SECONDS = {cfg['stmt_timeout']}
                STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = {cfg['queued_timeout']}
        """
    return f"""
        CREATE OR REPLACE WAREHOUSE {name}
            WAREHOUSE_TYPE = 'STANDARD'
            WAREHOUSE_SIZE = '{cfg['size']}'
            GENERATION = '{cfg['generation']}'
            MIN_CLUSTER_COUNT = {cfg['min_clusters']}
            MAX_CLUSTER_COUNT = {cfg['max_clusters']}
            SCALING_POLICY = '{cfg['scaling_policy']}'
            MAX_CONCURRENCY_LEVEL = {cfg['max_concurrency']}
            AUTO_SUSPEND = {cfg['auto_suspend']}
            AUTO_RESUME = {str(cfg['auto_resume']).upper()}
            ENABLE_QUERY_ACCELERATION = {str(cfg['qas']).upper()}
            QUERY_ACCELERATION_MAX_SCALE_FACTOR = {cfg['qas_scale']}
            STATEMENT_TIMEOUT_IN_SECONDS = {cfg['stmt_timeout']}
            STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = {cfg['queued_timeout']}
    """


def render_config(side, wh_type, default_name):
    """Render the tuning controls for one side. Returns (warehouse_name, cfg)."""
    k = f"{side}_{wh_type}_"
    cfg = {}
    name = st.text_input("Warehouse name", default_name, key=f"{k}name")

    if wh_type == "adaptive":
        cfg["max_perf"] = st.selectbox(
            "Max query performance level", SIZES,
            index=SIZES.index("XLARGE"), key=f"{k}perf",
        )
        cfg["throughput"] = st.number_input(
            "Query throughput multiplier", min_value=0, max_value=100, value=2,
            help="0 = unlimited burst capacity. Higher reduces queuing at the cost of instantaneous spend.",
            key=f"{k}thr",
        )
        st.caption("Adaptive manages size, clusters, QAS and suspend/resume automatically.")
    else:
        cfg["size"] = st.selectbox("Size", SIZES, index=SIZES.index("SMALL"), key=f"{k}size")
        cfg["generation"] = st.selectbox("Generation", ["1", "2"], index=1, key=f"{k}gen")
        c1, c2 = st.columns(2)
        with c1:
            cfg["min_clusters"] = st.number_input("Min clusters", 1, 10, 1, key=f"{k}minc")
        with c2:
            cfg["max_clusters"] = st.number_input("Max clusters", 1, 10, 3, key=f"{k}maxc")
        cfg["scaling_policy"] = st.selectbox(
            "Scaling policy", ["STANDARD", "ECONOMY"], key=f"{k}scale",
        )
        cfg["max_concurrency"] = st.number_input(
            "Max concurrency level", 1, 100, 8,
            help="Queries beyond this queue on each cluster. Snowflake default is 8.",
            key=f"{k}conc",
        )
        cfg["qas"] = st.checkbox("Enable query acceleration", value=True, key=f"{k}qas")
        cfg["qas_scale"] = st.number_input("QAS max scale factor", 0, 100, 8, key=f"{k}qasf")
        cfg["auto_suspend"] = st.number_input("Auto-suspend (s)", 30, 3600, 60, key=f"{k}susp")
        cfg["auto_resume"] = st.checkbox("Auto-resume", value=True, key=f"{k}res")

    cfg["stmt_timeout"] = st.number_input(
        "Statement timeout (s)", 30, 172800, 3600, key=f"{k}stmt",
    )
    cfg["queued_timeout"] = st.number_input(
        "Queued timeout (s)", 0, 172800, 0,
        help="0 = no queue timeout. Queries are cancelled after waiting this long.",
        key=f"{k}qd",
    )
    return name, cfg


def apply_preset(preset):
    """Push a preset into the widget session state. Must run before the widgets
    are instantiated, so callers rerun immediately afterwards."""
    type_a, type_b = MODES[preset["mode"]]
    st.session_state["mode_select"] = preset["mode"]
    st.session_state["scenario_select"] = preset["scenario"]
    for side, wh_type, cfg, name in [
        ("a", type_a, preset["a"], preset["wh_a"]),
        ("b", type_b, preset["b"], preset["wh_b"]),
    ]:
        k = f"{side}_{wh_type}_"
        st.session_state[f"{k}name"] = name
        for field, val in cfg.items():
            suffix = FIELD_KEYS.get(field)
            if suffix:
                st.session_state[f"{k}{suffix}"] = val


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Setup")

    preset_name = st.selectbox("Demo preset", list(DEMO_PRESETS.keys()), key="preset_select")
    preset = DEMO_PRESETS[preset_name]
    if preset:
        st.caption(preset["watch"].replace("**", ""))
        if st.button("Apply preset", use_container_width=True):
            apply_preset(preset)
            st.session_state["active_preset"] = preset_name
            st.rerun()

    st.divider()

    mode = st.selectbox("Comparison mode", list(MODES.keys()), key="mode_select")
    type_a, type_b = MODES[mode]

    tpc_options = ["TPCH_SF1", "TPCH_SF10", "TPCH_SF100", "TPCH_SF1000"]
    tpc_dataset = st.selectbox(
        "TPC dataset", tpc_options, index=tpc_options.index(TPC_DATASET),
        key="tpc_dataset_select",
    )

    # Defaults keep the original names in the default mode so existing demos still work
    default_a = "ZW_ADAPTIVE_WH" if type_a == "adaptive" else "ZW_WH_A"
    default_b = "ZW_CLASSIC_WH" if type_b == "standard" else "ZW_WH_B"

    with st.expander(f"🔵 Side A — {type_a.title()}", expanded=True):
        wh_a, cfg_a = render_config("a", type_a, default_a)
    with st.expander(f"🟠 Side B — {type_b.title()}", expanded=True):
        wh_b, cfg_b = render_config("b", type_b, default_b)

    label_a = build_label(type_a, cfg_a)
    label_b = build_label(type_b, cfg_b)

    same_name = wh_a.strip().upper() == wh_b.strip().upper()
    if same_name:
        st.error("Side A and Side B must use different warehouse names.")

    if st.button("Create Both", use_container_width=True, disabled=same_name):
        for side, name, wh_type, cfg in [
            ("A", wh_a, type_a, cfg_a),
            ("B", wh_b, type_b, cfg_b),
        ]:
            try:
                session.sql(build_create_sql(name, wh_type, cfg)).collect()
                st.success(f"Side {side}: created {name}")
            except Exception as e:
                st.error(f"Side {side} ({name}) failed: {e}")
        st.cache_data.clear()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("⚡ Warehouse Comparison")


@st.cache_data(ttl=30)
def _wh_info(wh):
    try:
        rows = session.sql(f"SHOW WAREHOUSES LIKE '{wh}'").collect()
        return rows[0].as_dict() if rows else {}
    except Exception:
        return {}


def _wh_summary(info, wh_type):
    if not info:
        return "_not found — use Create Both_"
    state = info.get("state", "—")
    # Report against the live type, not the sidebar's, so a warehouse that is
    # currently the other type is described with its own fields.
    live_type = str(info.get("type", "")).upper()
    if live_type == "ADAPTIVE":
        return (
            f"State: **{state}** · Max Perf: **{info.get('max_query_performance_level', '—')}** · "
            f"Throughput: **{info.get('query_throughput_multiplier', '—')}x**"
        )
    return (
        f"State: **{state}** · Size: **{info.get('size', '—')}** · "
        f"Gen: **{info.get('generation', '—')}** · "
        f"Clusters: **{info.get('min_cluster_count', '—')}/{info.get('max_cluster_count', '—')}**"
    )


def label_from_info(info, wh_type):
    """Label derived from the live warehouse rather than the sidebar, so both the
    display and the recorded metadata describe what will actually run. Returns
    None when the warehouse does not exist yet."""
    if not info:
        return None
    live_type = str(info.get("type", "")).upper()
    if live_type == "ADAPTIVE":
        perf = str(info.get("max_query_performance_level", "?")).upper().replace("-", "")
        thr = info.get("query_throughput_multiplier", "?")
        try:
            thr_s = "unlimited" if int(thr) == 0 else f"{int(thr)}x"
        except (TypeError, ValueError):
            thr_s = f"{thr}x"
        return f"Adaptive {perf} {thr_s}"
    size = str(info.get("size", "?")).upper().replace("-", "")
    return f"Gen{info.get('generation', '?')} {size} {info.get('max_cluster_count', '?')}cl"


a_info = _wh_info(wh_a)
b_info = _wh_info(wh_b)

# Prefer the live definition. The sidebar only expresses intent until Create Both
# has been run, and labelling a run with unapplied config would record results
# against a configuration that never executed.
live_label_a = label_from_info(a_info, type_a)
live_label_b = label_from_info(b_info, type_b)
eff_label_a = live_label_a or label_a
eff_label_b = live_label_b or label_b

drift_a = live_label_a is not None and live_label_a != label_a
drift_b = live_label_b is not None and live_label_b != label_b

col_a, col_b = st.columns(2)
for col, dot, eff, wh, info, wh_type, drift, want in [
    (col_a, "🔵", eff_label_a, wh_a, a_info, type_a, drift_a, label_a),
    (col_b, "🟠", eff_label_b, wh_b, b_info, type_b, drift_b, label_b),
]:
    with col:
        st.markdown(f"**{dot} {eff}** — `{wh}`")
        st.caption(_wh_summary(info, wh_type))
        if drift:
            st.warning(f"Live warehouse is **{eff}**, sidebar is set to **{want}**. "
                       f"Click **Create Both** to apply, or results will be recorded as {eff}.")

if drift_a or drift_b:
    st.caption("_Labels above reflect the live warehouses, not the pending sidebar config._")


# ── Controls ──────────────────────────────────────────────────────────────────
sc_col, btn_col = st.columns([4, 2])
with sc_col:
    scenario = st.selectbox("Scenario", list(SCENARIOS.keys()), key="scenario_select", label_visibility="collapsed")
with btn_col:
    run_clicked = st.button("▶ Run Workload", type="primary", use_container_width=True)

sc = SCENARIOS[scenario]
total_q = sc["simple"] + sc["medium"] + sc["complex"]
st.caption(f"*{sc['description']}* — **{total_q}** queries/wh: {sc['simple']}S · {sc['medium']}M · {sc['complex']}C")

# Show the preset's guidance once it has actually been applied, so it cannot
# describe a configuration that is no longer loaded.
_active = st.session_state.get("active_preset")
if _active and DEMO_PRESETS.get(_active):
    _p = DEMO_PRESETS[_active]
    if mode == _p["mode"] and scenario == _p["scenario"]:
        st.info(f"**{_active}** — what to watch: {_p['watch']}")
    else:
        st.caption(f"_Preset **{_active}** loaded, but mode or scenario has since been changed._")

if run_clicked:
    if same_name:
        st.error("Side A and Side B must use different warehouse names.")
        st.stop()
    # Record the live labels so stored results always describe the warehouses that
    # actually ran, even if the sidebar holds unapplied changes.
    run_meta = json.dumps({
        "mode": mode,
        "side_a": {"name": wh_a, "type": type_a, "label": eff_label_a,
                   **{k: str(v) for k, v in cfg_a.items()}},
        "side_b": {"name": wh_b, "type": type_b, "label": eff_label_b,
                   **{k: str(v) for k, v in cfg_b.items()}},
        "dataset": tpc_dataset,
    }).replace("'", "''")
    with st.spinner(f"Running {total_q * 2} queries across both warehouses..."):
        # Drive from the neutral warehouse so the CALL itself never executes on a
        # warehouse under test.
        try:
            session.sql(f"USE WAREHOUSE {DRIVER_WH}").collect()
        except Exception as e:
            st.warning(f"Could not switch to {DRIVER_WH}: {e}")
        result = session.sql(f"""
            CALL {DB}.{SCH_A}.ZW_RUN_WORKLOAD(
                '{scenario}', {sc['simple']}, {sc['medium']}, {sc['complex']},
                '{wh_a}', '{wh_b}', '{run_meta}'
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

def _meta_sides(meta_json):
    """Return (side_a, side_b) dicts from RUN_META, each with a 'label' and 'name'.
    Handles both the current side_a/side_b schema and the legacy adaptive/classic one."""
    try:
        m = json.loads(meta_json) if meta_json else {}
    except Exception:
        return None, None, ""
    dataset = m.get("dataset", "")

    if "side_a" in m or "side_b" in m:
        return m.get("side_a", {}), m.get("side_b", {}), dataset

    # Legacy rows: adaptive/classic keys with no stored label
    a, c = m.get("adaptive"), m.get("classic")
    if a is None and c is None:
        return None, None, dataset
    a = dict(a or {})
    c = dict(c or {})
    a.setdefault("label", f"Adaptive {a.get('max_perf', '?')} {a.get('throughput', '?')}x")
    c.setdefault("label", f"Classic {c.get('size', '?')} {c.get('max_clusters', '?')}cl")
    return a, c, dataset


def _build_run_label(row):
    ts = str(row["RUN_TS"])[:19]
    sa, sb, dataset = _meta_sides(row.get("RUN_META"))
    if sa or sb:
        a_desc = (sa or {}).get("label", "Side A")
        b_desc = (sb or {}).get("label", "Side B")
        parts = [row["SCENARIO"], f"{a_desc} vs {b_desc}"]
        if dataset:
            parts.append(dataset)
        parts.append(ts)
        return " — ".join(parts)
    return f"{row['SCENARIO']} — {eff_label_a} vs {eff_label_b} — {TPC_DATASET} — {ts}"

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

# Resolve names/labels from the selected run's metadata so past runs render with
# the config they actually used, not whatever the sidebar currently shows.
_sel_row = runs_df[runs_df["RUN_ID"] == selected_run_id]
_sel_meta = _sel_row.iloc[0]["RUN_META"] if not _sel_row.empty else None
_sa, _sb, _ = _meta_sides(_sel_meta)

run_wh_a = (_sa or {}).get("name") or wh_a
run_wh_b = (_sb or {}).get("name") or wh_b
run_label_a = (_sa or {}).get("label") or eff_label_a
run_label_b = (_sb or {}).get("label") or eff_label_b

a_df = df[df["WAREHOUSE_NAME"] == run_wh_a].copy()
b_df = df[df["WAREHOUSE_NAME"] == run_wh_b].copy()

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
    a_ends = a_df[a_df["EXECUTION_STATUS"] == "SUCCESS"]["END_TIME"].dropna().sort_values()
    b_ends = b_df[b_df["EXECUTION_STATUS"] == "SUCCESS"]["END_TIME"].dropna().sort_values()
    all_ends = df[df["EXECUTION_STATUS"] == "SUCCESS"]["END_TIME"].dropna()
    if not all_ends.empty:
        min_start = df[df["EXECUTION_STATUS"] == "SUCCESS"]["START_TIME"].dropna().min()
        max_end = all_ends.max()
        total_sec = int((max_end - min_start).total_seconds()) + 1
        a_total = len(a_ends)
        b_total = len(b_ends)
        rows = []
        for sec in range(0, total_sec + 1):
            cutoff = min_start + pd.Timedelta(seconds=sec)
            a_done = int((a_ends <= cutoff).sum())
            b_done = int((b_ends <= cutoff).sum())
            rows.append({
                "Elapsed (s)": sec,
                run_label_a: a_total - a_done,
                run_label_b: b_total - b_done,
            })
        chart_df = pd.DataFrame(rows).set_index("Elapsed (s)")
        st.line_chart(chart_df, height=200)
        st.caption(f"Queries remaining over time (started with {a_total}/{b_total} per warehouse)")

# ── Metrics + Distribution ────────────────────────────────────────────────────
left_col, right_col = st.columns(2)

with left_col:
    st.markdown("#### Performance Metrics")

    def _metric_col(d):
        return [
            str(len(d[d["EXECUTION_STATUS"] == "SUCCESS"])),
            _fmt(_stat(d, "ELAPSED_SEC", lambda s: s.mean())),
            _fmt(_stat(d, "EXEC_SEC", lambda s: s.mean())),
            _fmt(_stat(d, "QUEUED_SEC", lambda s: s.mean())),
            _fmt(_stat(d, "EXEC_SEC", lambda s: s.quantile(0.90))),
            _fmt(_stat(d, "EXEC_SEC", lambda s: s.max())),
        ]

    metrics = {
        "Metric": ["Total Queries", "Avg Elapsed (s)", "Avg Exec (s)", "Avg Queue (s)", "P90 Exec (s)", "Max Exec (s)"],
        f"🔵 {run_label_a}": _metric_col(a_df),
        f"🟠 {run_label_b}": _metric_col(b_df),
    }
    st.dataframe(metrics, use_container_width=True, hide_index=True)

with right_col:
    st.markdown("#### Execution Time Distribution")

    def _make_hist(d):
        done = d[(d["EXECUTION_STATUS"] == "SUCCESS") & (d["EXEC_SEC"] > 0) & (d["COMPLEXITY"] != "Other")].copy()
        if done.empty:
            return None
        done["EXEC_SEC"] = done["EXEC_SEC"].astype(float)
        bins = pd.cut(done["EXEC_SEC"], bins=15)
        hist = done.groupby([bins, "COMPLEXITY"], observed=True).size().unstack(fill_value=0)
        hist.index = [f"{i.left:.0f}-{i.right:.0f}" for i in hist.index]
        return hist

    c1, c2 = st.columns(2)
    for col, d, lbl, dot in [(c1, a_df, run_label_a, "🔵"), (c2, b_df, run_label_b, "🟠")]:
        with col:
            st.caption(f"{dot} {lbl}")
            hist = _make_hist(d)
            if hist is not None:
                st.bar_chart(hist, height=180)
            else:
                st.caption("No data")

# ── Per-complexity breakdown ──────────────────────────────────────────────────
st.markdown("#### By Complexity")
st.caption(f"🔵 {run_label_a} · 🟠 {run_label_b}")
comp_cols = st.columns(3)
for i, comp in enumerate(["Simple", "Medium", "Complex"]):
    with comp_cols[i]:
        a_comp = a_df[(a_df["COMPLEXITY"] == comp) & (a_df["EXECUTION_STATUS"] == "SUCCESS")]
        b_comp = b_df[(b_df["COMPLEXITY"] == comp) & (b_df["EXECUTION_STATUS"] == "SUCCESS")]
        a_avg = f"{a_comp['EXEC_SEC'].mean():.1f}" if not a_comp.empty else "—"
        b_avg = f"{b_comp['EXEC_SEC'].mean():.1f}" if not b_comp.empty else "—"
        a_q = f"{a_comp['QUEUED_SEC'].mean():.1f}" if not a_comp.empty else "—"
        b_q = f"{b_comp['QUEUED_SEC'].mean():.1f}" if not b_comp.empty else "—"
        st.markdown(f"**{comp}**")
        st.markdown(f"Exec: 🔵 {a_avg}s · 🟠 {b_avg}s")
        st.markdown(f"Queue: 🔵 {a_q}s · 🟠 {b_q}s")

# ── Credit Consumption ────────────────────────────────────────────────────────
st.markdown("#### Credit Consumption (Last 12 Hours)")

@st.cache_data(ttl=60)
def get_credits_combined(name_a, name_b):
    return session.sql(f"""
        SELECT DATE_TRUNC('HOUR', START_TIME) AS HOUR, WAREHOUSE_NAME, SUM(CREDITS_USED) AS CREDITS
        FROM (
            SELECT * FROM TABLE({DB}.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
                DATE_RANGE_START => DATEADD('HOUR', -12, CURRENT_TIMESTAMP()),
                WAREHOUSE_NAME => '{name_a}'
            ))
            UNION ALL
            SELECT * FROM TABLE({DB}.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
                DATE_RANGE_START => DATEADD('HOUR', -12, CURRENT_TIMESTAMP()),
                WAREHOUSE_NAME => '{name_b}'
            ))
        )
        GROUP BY 1, 2 ORDER BY 1
    """).to_pandas()

cred_df = get_credits_combined(run_wh_a, run_wh_b)
if not cred_df.empty:
    cred_df["Warehouse"] = cred_df["WAREHOUSE_NAME"].map({run_wh_a: run_label_a, run_wh_b: run_label_b})
    cred_df["Hour"] = cred_df["HOUR"].dt.strftime("%H:%M")
    chart = alt.Chart(cred_df).mark_bar().encode(
        x=alt.X("Hour:N", title="Hour"),
        y=alt.Y("CREDITS:Q", title="Credits"),
        color=alt.Color("Warehouse:N", scale=alt.Scale(
            domain=[run_label_a, run_label_b], range=["#1f77b4", "#ff7f0e"])),
        xOffset="Warehouse:N"
    ).properties(height=250)
    st.altair_chart(chart, use_container_width=True)
    a_cred = cred_df[cred_df["Warehouse"] == run_label_a]["CREDITS"].sum()
    b_cred = cred_df[cred_df["Warehouse"] == run_label_b]["CREDITS"].sum()
    st.caption(f"Total credits — 🔵 {run_label_a}: {a_cred:.2f} · 🟠 {run_label_b}: {b_cred:.2f}")
else:
    st.info("No credit data yet (account usage may have ~45 min latency)")

