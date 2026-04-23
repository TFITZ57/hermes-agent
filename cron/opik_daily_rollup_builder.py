from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cron.opik_hourly_watch_builder import (
    HERMES_HOME,
    HERMES_PROJECT,
    MEMORY_TOOL_NAMES,
    STATE_DB_PATH,
    detect_delegated_batches,
    fetch_span_rows,
    fetch_trace_rows,
    finalization_status,
    get_nested,
    load_local_session_rows,
    normalize_tool_span_name,
    parse_num,
    summarize_memory_spans,
    to_aware_datetime,
    trace_is_self,
    trace_metrics,
    workload_name,
)

OUT_DIR = HERMES_HOME / "cron" / "output" / "d16b5967a31f" / "structured"
HOURLY_DIR = HERMES_HOME / "cron" / "output" / "ce223a54c764" / "structured"


def load_hourly_rollups(output_dir: Path, from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    if not output_dir.exists():
        return []
    for path in sorted(output_dir.glob("*.json")):
        if path.name == "latest.json":
            continue
        try:
            report = json.loads(path.read_text())
        except Exception:
            continue
        if not isinstance(report, dict) or report.get("report_type") != "opik_hourly_watch":
            continue
        window = report.get("window") or {}
        to_raw = window.get("to_utc") or report.get("generated_at_utc")
        to_hour = to_aware_datetime(to_raw)
        if to_hour is None or to_hour < from_dt or to_hour > to_dt:
            continue
        key = str(window.get("to_utc") or report.get("generated_at_utc") or path.name)
        deduped[key] = report
    return [deduped[key] for key in sorted(deduped)]


def summarize_hourly_rollups(hourly_reports: list[dict[str, Any]]) -> dict[str, Any]:
    freqs = {
        "backlog_pressure": 0,
        "telemetry_loss": 0,
        "true_empty_dispatch": 0,
        "memory_active": 0,
        "delegated_batches_present": 0,
        "provider_shift": 0,
        "rate_limit_caveat": 0,
    }
    peak_tokens = (None, -1)
    peak_traces = (None, -1)
    peak_backlog = (None, -1)
    previous_mix: tuple[tuple[str, ...], tuple[str, ...]] | None = None

    for report in hourly_reports:
        alerts = {str((alert or {}).get("code") or "") for alert in (report.get("alerts") or [])}
        caveats = {str(code) for code in (report.get("caveats") or [])}
        mix = report.get("mix") or {}
        providers = tuple(sorted((mix.get("providers") or {}).keys()))
        platforms = tuple(sorted((mix.get("platforms") or {}).keys()))
        report_hour = str((report.get("window") or {}).get("to_utc") or "")

        if "backlog_pressure" in alerts:
            freqs["backlog_pressure"] += 1
        if "telemetry_loss" in alerts:
            freqs["telemetry_loss"] += 1
        if "true_empty_dispatch" in alerts:
            freqs["true_empty_dispatch"] += 1
        if str((report.get("memory") or {}).get("status") or "none") == "active":
            freqs["memory_active"] += 1
        if int(((report.get("delegation") or {}).get("proven_parent_batches") or 0)) > 0:
            freqs["delegated_batches_present"] += 1
        if "span_crawl_rate_limited" in caveats or "span_crawl_rate_limiting" in alerts:
            freqs["rate_limit_caveat"] += 1
        if len(providers) > 1 or len(platforms) > 1 or (previous_mix is not None and previous_mix != (providers, platforms)):
            freqs["provider_shift"] += 1
        previous_mix = (providers, platforms)

        tokens = int(((report.get("volume") or {}).get("tokens_total_measured") or 0))
        traces = int(((report.get("counts") or {}).get("kept_finalized_workload_count") or 0))
        backlog = int(((report.get("counts") or {}).get("excluded_nonfinal_count") or 0))
        if tokens > peak_tokens[1]:
            peak_tokens = (report_hour, tokens)
        if traces > peak_traces[1]:
            peak_traces = (report_hour, traces)
        if backlog > peak_backlog[1]:
            peak_backlog = (report_hour, backlog)

    return {
        "hourly_alert_frequencies": freqs,
        "peaks": {
            "peak_hour_by_tokens_utc": peak_tokens[0] or "",
            "peak_hour_by_trace_count_utc": peak_traces[0] or "",
            "peak_hour_by_backlog_utc": peak_backlog[0] or "",
        },
    }


def write_structured_outputs(report: dict[str, Any], output_dir: Path, report_date: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    latest = output_dir / "latest.json"
    dated = output_dir / f"{report_date}.json"
    payload = json.dumps(report, indent=2)
    latest.write_text(payload)
    dated.write_text(payload)
    return latest, dated


def build_daily_rollup(
    now: datetime | None = None,
    client: Any | None = None,
    state_db_path: Path = STATE_DB_PATH,
    hourly_dir: Path = HOURLY_DIR,
) -> dict[str, Any]:
    if now is None:
        now = datetime.now(timezone.utc)
    if client is None:
        from opik import Opik

        client = Opik()

    to_dt = now
    from_dt = now - timedelta(hours=24)

    project_id = ""
    try:
        projects = client.rest_client.projects.get_projects(page=1, size=100)
        rows = projects.model_dump().get("content") if hasattr(projects, "model_dump") else []
        for row in rows:
            if (row or {}).get("name") == HERMES_PROJECT:
                project_id = (row or {}).get("id") or ""
                break
    except Exception:
        project_id = ""

    parsed = [trace_metrics(r) for r in fetch_trace_rows(client, from_dt, to_dt)]
    raw_trace_count = len(parsed)
    workload_candidates = [tr for tr in parsed if not trace_is_self(tr) and workload_name(tr["name"])]
    excluded_self_count = sum(1 for tr in parsed if trace_is_self(tr))
    excluded_diagnostic_count = sum(1 for tr in parsed if not trace_is_self(tr) and not workload_name(tr["name"]))
    kept_finalized = [tr for tr in workload_candidates if finalization_status(tr)]
    excluded_nonfinal = [tr for tr in workload_candidates if not finalization_status(tr)]

    completed_with_telemetry_count = 0
    completed_telemetry_loss_count = 0
    completed_failed_or_interrupted_count = 0
    true_empty_dispatch_count = sum(1 for tr in excluded_nonfinal if tr["tokens"] == 0 and (tr["duration_seconds"] or 0) == 0)

    workload_tokens = 0.0
    workload_duration = 0.0
    workload_duration_max = 0.0
    api_calls_total = 0
    tool_calls_total = 0
    providers_counts: Counter[str] = Counter()
    platform_counts: Counter[str] = Counter()
    model_counts: Counter[str] = Counter()

    max_tokens_trace = {"trace_id": "", "session_id": "", "platform": "", "provider": "", "model": "", "metric_name": "tokens_total", "metric_value": 0.0}
    max_duration_trace = {"trace_id": "", "session_id": "", "platform": "", "provider": "", "model": "", "metric_name": "duration_seconds", "metric_value": 0.0}
    max_tool_calls_trace = {"trace_id": "", "session_id": "", "platform": "", "provider": "", "model": "", "metric_name": "tool_calls", "metric_value": 0}

    for tr in kept_finalized:
        providers = tr["providers"] or ["unknown"]
        model = tr["model"] or "unknown"
        platform = tr["platform"] or "unknown"
        for provider in providers:
            providers_counts[provider] += 1
        platform_counts[platform] += 1
        model_counts[model] += 1
        workload_tokens += tr["tokens"]
        workload_duration += tr["duration_seconds"] or 0.0
        workload_duration_max = max(workload_duration_max, tr["duration_seconds"] or 0.0)
        api_calls_total += tr["api_calls"]
        tool_calls_total += tr["tool_calls"]
        if tr["tokens"] > max_tokens_trace["metric_value"]:
            max_tokens_trace.update({"trace_id": tr["trace_id"], "session_id": tr["session_id"] or "", "platform": platform, "provider": providers[0], "model": model, "metric_value": tr["tokens"]})
        if (tr["duration_seconds"] or 0.0) > max_duration_trace["metric_value"]:
            max_duration_trace.update({"trace_id": tr["trace_id"], "session_id": tr["session_id"] or "", "platform": platform, "provider": providers[0], "model": model, "metric_value": tr["duration_seconds"] or 0.0})
        if tr["tool_calls"] > max_tool_calls_trace["metric_value"]:
            max_tool_calls_trace.update({"trace_id": tr["trace_id"], "session_id": tr["session_id"] or "", "platform": platform, "provider": providers[0], "model": model, "metric_value": tr["tool_calls"]})
        usage = tr["usage"] or {}
        has_usage = parse_num(get_nested(usage, "total_tokens")) > 0 or parse_num(get_nested(usage, "prompt_tokens")) > 0 or parse_num(get_nested(usage, "completion_tokens")) > 0
        if not has_usage or tr["llm_span_count"] <= 0:
            completed_telemetry_loss_count += 1
        else:
            completed_with_telemetry_count += 1
        out = tr["output"] or {}
        if out.get("success") is False or out.get("interrupted") is True:
            completed_failed_or_interrupted_count += 1

    llm_num = sum(1 for tr in kept_finalized if tr["llm_span_count"] > 0)
    kept_identifiable_sessions = {tr["session_id"] for tr in kept_finalized if tr["session_id"]}
    local_session_rows, local_tools_counts, local_memory_counts = load_local_session_rows(kept_identifiable_sessions, state_db_path)

    span_rows, span_crawl_attempted, span_crawl_rate_limited = fetch_span_rows(client, from_dt, to_dt)
    kept_trace_ids = {tr["trace_id"] for tr in kept_finalized}
    trace_visible_memory, lifecycle_counts = summarize_memory_spans(span_rows, kept_trace_ids)
    span_tool_counts = Counter()
    for span in span_rows:
        trace_id = str((span or {}).get("trace_id") or (span or {}).get("traceId") or "")
        if trace_id not in kept_trace_ids:
            continue
        stype = str((span or {}).get("type") or (span or {}).get("span_type") or "").lower()
        if stype != "tool":
            continue
        end_t = to_aware_datetime((span or {}).get("end_time") or (span or {}).get("endTime") or get_nested(span, "output", "end_time") or get_nested(span, "output", "endTime"))
        output = (span or {}).get("output") or {}
        if end_t is None and not output:
            continue
        tool_name = normalize_tool_span_name(str((span or {}).get("name") or ""), (span or {}).get("metadata") if isinstance((span or {}).get("metadata"), dict) else None)
        if tool_name:
            span_tool_counts[tool_name] += 1

    family_map: dict[str, dict[str, float]] = defaultdict(lambda: {"token_total": 0.0, "duration_total": 0.0, "trace_count": 0})
    for tr in kept_finalized:
        sid = tr["session_id"]
        if not sid:
            continue
        family_map[sid]["token_total"] += tr["tokens"]
        family_map[sid]["duration_total"] += tr["duration_seconds"] or 0.0
        family_map[sid]["trace_count"] += 1
    top_families = sorted(family_map.items(), key=lambda kv: kv[1]["token_total"], reverse=True)
    top_session_families = [
        {
            "session_id": sid,
            "trace_count": int(values["trace_count"]),
            "tokens_total": float(values["token_total"]),
            "duration_seconds_total": float(values["duration_total"]),
        }
        for sid, values in top_families[:5]
    ]
    largest_family_share = (top_families[0][1]["token_total"] / workload_tokens * 100.0) if top_families and workload_tokens > 0 else 0.0

    parent_batches = detect_delegated_batches(from_dt, to_dt, state_db_path, workload_candidates, kept_finalized)

    hourly_reports = load_hourly_rollups(hourly_dir, from_dt, to_dt)
    hourly_summary = summarize_hourly_rollups(hourly_reports)

    llm_pct = (llm_num / len(kept_finalized) * 100.0) if kept_finalized else 0.0
    local_den = len(kept_identifiable_sessions)
    local_cov = {
        "value_pct": round((len(local_session_rows) / local_den * 100.0), 3) if local_den else 0.0,
        "numerator": int(len(local_session_rows)),
        "denominator": int(local_den),
    }
    mem_status = "none"
    total_mem = sum(local_memory_counts.values()) + sum(trace_visible_memory.values())
    if total_mem > 0:
        mem_status = "active" if sum(trace_visible_memory.values()) > 0 else "low"

    escalation_codes: list[str] = []
    frequencies = hourly_summary["hourly_alert_frequencies"]
    if frequencies["backlog_pressure"] > 0:
        escalation_codes.append("backlog_pressure")
    if completed_telemetry_loss_count > 0 or frequencies["telemetry_loss"] > 0:
        escalation_codes.append("telemetry_loss")
    if true_empty_dispatch_count > 0 or frequencies["true_empty_dispatch"] > 0:
        escalation_codes.append("true_empty_dispatch")
    if len(parent_batches) > 0 or frequencies["delegated_batches_present"] > 0:
        escalation_codes.append("delegated_batches_present")
    if frequencies["rate_limit_caveat"] > 0 or span_crawl_rate_limited:
        escalation_codes.append("rate_limit_caveat")

    report: dict[str, Any] = {
        "schema_version": 1,
        "report_type": "opik_daily_digest_rollup",
        "generated_at_utc": now.isoformat().replace("+00:00", "Z"),
        "window": {
            "project_name": HERMES_PROJECT,
            "project_id": project_id,
            "from_utc": from_dt.isoformat().replace("+00:00", "Z"),
            "to_utc": to_dt.isoformat().replace("+00:00", "Z"),
            "duration_hours": 24,
        },
        "counts": {
            "raw_trace_count": int(raw_trace_count),
            "excluded_self_count": int(excluded_self_count),
            "excluded_nonfinal_count": int(len(excluded_nonfinal)),
            "excluded_diagnostic_count": int(excluded_diagnostic_count),
            "completed_telemetry_loss_count": int(completed_telemetry_loss_count),
            "true_empty_dispatch_count": int(true_empty_dispatch_count),
            "kept_finalized_workload_count": int(len(kept_finalized)),
            "completed_failed_or_interrupted_count": int(completed_failed_or_interrupted_count),
        },
        "coverage": {
            "llm_span_coverage": {"value_pct": round(llm_pct, 3), "numerator": int(llm_num), "denominator": int(len(kept_finalized))},
            "local_session_coverage": local_cov,
        },
        "volume": {
            "tokens_total_measured": int(workload_tokens),
            "tokens_usage_total": int(sum(parse_num(get_nested(tr["usage"], "total_tokens")) for tr in kept_finalized)),
            "tokens_fallback_total": int(sum(tr["tokens"] for tr in kept_finalized if tr["tokens_source"] != "usage")),
            "duration_seconds_total": float(workload_duration),
            "duration_seconds_avg": float(workload_duration / len(kept_finalized)) if kept_finalized else 0.0,
            "duration_seconds_max": float(workload_duration_max),
            "api_calls_total": int(api_calls_total),
            "tool_calls_total": int(tool_calls_total),
        },
        "mix": {"providers": dict(providers_counts), "platforms": dict(platform_counts), "models": dict(model_counts)},
        "hourly_alert_frequencies": frequencies,
        "peaks": hourly_summary["peaks"],
        "concentration": {
            "largest_session_family_token_share_pct": float(round(largest_family_share, 3)),
            "top_session_families": top_session_families,
        },
        "tool_hotspots": {
            "opik_completed_tool_spans": {
                "is_lower_bound": bool(span_crawl_rate_limited or not span_crawl_attempted),
                "lower_bound_reason": "span crawl partial or unavailable" if (span_crawl_rate_limited or not span_crawl_attempted) else "",
                "counts": dict(span_tool_counts),
            },
            "local_assistant_tool_calls": {
                "is_lower_bound": True,
                "lower_bound_reason": "local matched sessions only",
                "counts": dict(local_tools_counts),
            },
        },
        "outliers": {
            "max_tokens_trace": max_tokens_trace,
            "max_duration_trace": max_duration_trace,
            "max_tool_calls_trace": max_tool_calls_trace,
        },
        "memory": {
            "status": mem_status,
            "trace_visible_tools": trace_visible_memory,
            "local_tools_lower_bound": {key: int(local_memory_counts.get(key, 0)) for key in MEMORY_TOOL_NAMES},
            "lifecycle_spans_lower_bound": {
                "memory_inject": int(lifecycle_counts["memory:inject"]),
                "memory_recall": int(lifecycle_counts["memory:recall"]),
                "memory_sync": int(lifecycle_counts["memory:sync"]),
            },
        },
        "delegation": {
            "proven_parent_batches": int(len(parent_batches)),
            "child_sessions": int(sum(len(batch["child_sessions"]) for batch in parent_batches)),
            "child_traces": int(sum(len(batch["child_traces"]) for batch in parent_batches)),
            "finalized_child_traces": int(sum(len(batch["child_finalized"]) for batch in parent_batches)),
            "delegated_tagged_child_traces": int(sum(1 for batch in parent_batches for trace in batch["child_traces"] if "delegated" in trace["tags"])),
            "parent_session_id_marked_child_traces": int(sum(1 for batch in parent_batches for trace in batch["child_traces"] if (trace.get("metadata") or {}).get("parent_session_id") == batch["parent_session_id"])),
            "child_tokens_total": int(sum(sum(trace["tokens"] for trace in batch["child_traces"]) for batch in parent_batches)),
            "child_api_calls_total": int(sum(sum(trace["api_calls"] for trace in batch["child_traces"]) for batch in parent_batches)),
            "child_tool_calls_total": int(sum(sum(trace["tool_calls"] for trace in batch["child_traces"]) for batch in parent_batches)),
        },
        "escalation_codes": escalation_codes,
        "caveats": [],
    }
    if len(local_session_rows) < len(kept_identifiable_sessions):
        report["caveats"].append("local_session_coverage_partial")
    if not span_crawl_attempted:
        report["caveats"].append("span_crawl_not_attempted")
    if span_crawl_rate_limited:
        report["caveats"].append("span_crawl_rate_limited")
    if completed_telemetry_loss_count > 0:
        report["caveats"].append("telemetry_loss_detected")
    return report


def main() -> None:
    from cron.opik_rollup_persistence import persist_daily_rollup

    rollup = build_daily_rollup()
    report_date = str((rollup.get("window") or {}).get("to_utc") or "")[:10]
    latest, dated = write_structured_outputs(rollup, OUT_DIR, report_date)
    rollup.setdefault("collection", {})["path_latest"] = str(latest)
    rollup.setdefault("collection", {})["path_stamped"] = str(dated)
    persist_daily_rollup(rollup)
    payload = json.dumps(rollup, indent=2)
    latest.write_text(payload)
    dated.write_text(payload)
    print(payload)


if __name__ == "__main__":
    main()
