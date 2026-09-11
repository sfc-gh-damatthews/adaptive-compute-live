---
name: "configurable warehouse comparison modes"
created: "2026-09-11T15:13:50.767Z"
status: pending
---

# Configurable warehouse comparison modes

## Goal

Replace the fixed "Adaptive vs Classic" setup with a mode selector supporting three comparisons, each side independently tunable, with labels derived automatically from the chosen config.

## Constraints from Snowflake (confirmed in docs)

Adaptive and standard warehouses accept **mutually exclusive** property sets — setting the wrong one is an error, so the DDL builder must branch on type:

| Property                                                            | Adaptive | Standard/Gen2 |
| ------------------------------------------------------------------- | -------- | ------------- |
| `MAX_QUERY_PERFORMANCE_LEVEL` (XSMALL–X4LARGE, default XLARGE)      | yes      | **no**        |
| `QUERY_THROUGHPUT_MULTIPLIER` (int, 0 = unlimited, default 2)       | yes      | **no**        |
| `WAREHOUSE_SIZE`                                                    | **no**   | yes           |
| `GENERATION` ('1' / '2')                                            | **no**   | yes           |
| `MIN_CLUSTER_COUNT` / `MAX_CLUSTER_COUNT`                           | **no**   | yes           |
| `SCALING_POLICY` (STANDARD / ECONOMY)                               | **no**   | yes           |
| `MAX_CONCURRENCY_LEVEL` (default 8)                                 | **no**   | yes           |
| `ENABLE_QUERY_ACCELERATION` / `QUERY_ACCELERATION_MAX_SCALE_FACTOR` | **no**   | yes           |
| `AUTO_SUSPEND` / `AUTO_RESUME`                                      | **no**   | yes           |
| `STATEMENT_TIMEOUT_IN_SECONDS`                                      | yes      | yes           |
| `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS`                               | yes      | yes           |

Adaptive manages sizing, scaling, QAS and suspend/resume itself. Auto-suspend/auto-resume will always be emitted for standard sides and omitted for adaptive, with a sidebar caption noting why.

## Changes

### 1. `streamlit_app.py` — sidebar

```python
MODES = {
    "Adaptive vs Standard": ("adaptive", "standard"),
    "Adaptive vs Adaptive": ("adaptive", "adaptive"),
    "Standard vs Standard": ("standard", "standard"),
}
```

Mode selectbox resolves to `(type_a, type_b)`. Two expanders — **Side A** / **Side B** — each rendered by one shared function keyed by side so widget keys stay unique. Default warehouse names stay distinct per mode (`ZW_WH_A` / `ZW_WH_B`, prefilled with the existing `ZW_ADAPTIVE_WH` / `ZW_CLASSIC_WH` in the default mode so current demos keep working).

### 2. DDL builder

`build_create_sql(name, wh_type, cfg)` emits only valid properties for the type. Adaptive uses `WAREHOUSE_TYPE = 'ADAPTIVE'` via standard `CREATE WAREHOUSE` syntax (equivalent to `CREATE ADAPTIVE WAREHOUSE`, and keeps one code path). **Create Both** runs both statements and reports each independently so one failure doesn't hide the other.

### 3. Label builder

```python
def build_label(wh_type, cfg):
    if wh_type == "adaptive":
        thr = "unlimited" if int(cfg["throughput"]) == 0 else f"{cfg['throughput']}x"
        return f"Adaptive {cfg['max_perf']} {thr}"
    return f"Gen{cfg['generation']} {cfg['size']} {cfg['max_clusters']}cl"
```

Produces `Adaptive LARGE 5x`, `Adaptive LARGE 2x`, `Gen2 Small 3cl`. Used for column headers, chart series names, section headers, and persisted in `RUN_META`.

### 4. `ZW_RUN_WORKLOAD` — add `RUN_META_JSON`

Currently the proc runs `SHOW WAREHOUSES` and hardcodes `adaptive` / `classic` keys into `RUN_META`. Since the app already knows the full config it just applied, it's cleaner for the app to build the metadata and pass it in:

```sql
ZW_RUN_WORKLOAD(SCENARIO_NAME, SIMPLE_COUNT, MEDIUM_COUNT, COMPLEX_COUNT,
                WH_A, WH_B, RUN_META_JSON)
```

New shape:

```json
{
  "mode": "Adaptive vs Adaptive",
  "side_a": {"name": "ZW_WH_A", "type": "adaptive", "label": "Adaptive LARGE 5x", "max_perf": "LARGE", "throughput": "5"},
  "side_b": {"name": "ZW_WH_B", "type": "adaptive", "label": "Adaptive LARGE 2x", "max_perf": "LARGE", "throughput": "2"},
  "dataset": "TPCH_SF10"
}
```

This drops the `SHOW WAREHOUSES` round-trips from the proc. Everything else in the proc (async fire, connector-status polling, `QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10000)` insert) is unchanged.

### 5. Result rendering

`adap_df` / `cls_df` become `a_df` / `b_df`, still selected by `WAREHOUSE_NAME`. Every hardcoded "Adaptive" / "Classic" string and the 🔵 / 🟠 pairing move to the generated labels — colours stay blue/orange but now denote **side A / side B**, not warehouse type. Affects: warehouse header row, Outstanding Queries chart, Performance Metrics table, Execution Time Distribution, By Complexity, Credit Consumption.

### 6. Backwards compatibility

`_build_run_label` will read `side_a` / `side_b` when present and fall back to the legacy `adaptive` / `classic` keys otherwise, so the 2,988 existing rows still render in the Past Runs dropdown.

## Guarding against a real footgun

In the two same-type modes both sides must have **different warehouse names** — identical names would make the app compare a warehouse against itself and silently show identical stats. I'll block **Create Both** and **Run Workload** with a clear error if the names match.

## Testing

For each of the three modes: create both warehouses, run a small scenario (e.g. 5S/2M/1C to keep it quick), confirm rows land in `ZW_RESULTS` with the expected labels and that both sides are populated. Then commit and push to `sfc-gh-damatthews/adaptive-compute-live`.

## Files touched

- `streamlit_app.py` — sidebar, DDL/label builders, all result rendering
- `setup.sql` — updated `ZW_RUN_WORKLOAD` signature and body
