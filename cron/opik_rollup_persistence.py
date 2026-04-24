from __future__ import annotations

import json
import os
from typing import Any

try:
    import psycopg
except ImportError:  # pragma: no cover - depends on optional postgres extra
    psycopg = None


JSON = dict[str, Any]


def get_hq_postgres_dsn() -> str:
    dsn = os.getenv("HERMES_HQ_DATABASE_URL") or os.getenv("HERMES_HQ_DATABASE_DIRECT_CONNECTION_STRING")
    if not dsn:
        raise RuntimeError("HERMES_HQ_DATABASE_URL or HERMES_HQ_DATABASE_DIRECT_CONNECTION_STRING is required")
    return dsn


def _coverage_block(report: JSON, key: str) -> JSON:
    return ((report.get("coverage") or {}).get(key) or {})


def _count_block(report: JSON, key: str) -> JSON:
    return ((report.get("counts") or {}).get(key) or {})


def _dump_json(value: Any) -> str:
    return json.dumps(value if value is not None else {})


def build_hourly_rollup_base(report: JSON) -> JSON:
    window = report.get("window") or {}
    collection = report.get("collection") or {}
    counts = report.get("counts") or {}
    coverage = report.get("coverage") or {}
    volume = report.get("volume") or {}
    mix = report.get("mix") or {}
    load = report.get("load") or {}
    memory = report.get("memory") or {}
    delegation = report.get("delegation") or {}
    outliers = report.get("outliers") or {}
    max_tokens = outliers.get("max_tokens_trace") or {}
    max_duration = outliers.get("max_duration_trace") or {}
    max_tool_calls = outliers.get("max_tool_calls_trace") or {}
    llm_cov = coverage.get("llm_span_coverage") or {}
    local_cov = coverage.get("local_session_coverage") or {}
    mem_visibility = coverage.get("memory_tool_span_visibility") or {}
    backlog = load.get("nonfinal_backlog_share") or {}
    largest_batch = delegation.get("largest_batch") or {}
    memory_visible = memory.get("trace_visible_tools") or {}
    memory_local = memory.get("local_tools_lower_bound") or {}
    memory_lifecycle = memory.get("lifecycle_spans_lower_bound") or {}

    return {
        "project_name": window.get("project_name") or "",
        "project_id": window.get("project_id") or None,
        "generated_at_utc": report.get("generated_at_utc"),
        "window_from_utc": window.get("from_utc"),
        "window_to_utc": window.get("to_utc"),
        "duration_minutes": int(window.get("duration_minutes") or 90),
        "frozen_cutoff": bool(window.get("frozen_cutoff", True)),
        "trace_pull_method": collection.get("trace_pull_method"),
        "mcp_baseline_checked": bool(collection.get("mcp_baseline_checked", False)),
        "sdk_paged": bool(collection.get("sdk_paged", False)),
        "trace_page_size": collection.get("trace_page_size"),
        "span_crawl_attempted": bool(collection.get("span_crawl_attempted", False)),
        "span_crawl_rate_limited": bool(collection.get("span_crawl_rate_limited", False)),
        "prior_report_path": collection.get("prior_report_path"),
        "prior_report_found": bool(collection.get("prior_report_found", False)),
        "materially_changed_vs_prior": collection.get("materially_changed_vs_prior"),
        "artifact_latest_path": collection.get("path_latest"),
        "artifact_stamped_path": collection.get("path_stamped"),
        "raw_trace_count": int(counts.get("raw_trace_count") or 0),
        "workload_trace_count": int(counts.get("workload_trace_count") or 0),
        "kept_finalized_workload_count": int(counts.get("kept_finalized_workload_count") or 0),
        "excluded_self_count": int(counts.get("excluded_self_count") or 0),
        "excluded_nonfinal_count": int(counts.get("excluded_nonfinal_count") or 0),
        "excluded_diagnostic_count": int(counts.get("excluded_diagnostic_count") or 0),
        "completed_telemetry_loss_count": int(counts.get("completed_telemetry_loss_count") or 0),
        "true_empty_dispatch_count": int(counts.get("true_empty_dispatch_count") or 0),
        "completed_with_telemetry_count": int(counts.get("completed_with_telemetry_count") or 0),
        "completed_failed_or_interrupted_count": int(counts.get("completed_failed_or_interrupted_count") or 0),
        "llm_span_coverage_pct": float(llm_cov.get("value_pct") or 0),
        "llm_span_coverage_numerator": int(llm_cov.get("numerator") or 0),
        "llm_span_coverage_denominator": int(llm_cov.get("denominator") or 0),
        "local_session_coverage_pct": float(local_cov.get("value_pct") or 0),
        "local_session_coverage_numerator": int(local_cov.get("numerator") or 0),
        "local_session_coverage_denominator": int(local_cov.get("denominator") or 0),
        "memory_tool_span_visibility_pct": float(mem_visibility.get("value_pct") or 0),
        "memory_tool_span_visibility_numerator": int(mem_visibility.get("numerator") or 0),
        "memory_tool_span_visibility_denominator": int(mem_visibility.get("denominator") or 0),
        "tokens_total_measured": int(volume.get("tokens_total_measured") or 0),
        "tokens_usage_total": int(volume.get("tokens_usage_total") or 0),
        "tokens_fallback_total": int(volume.get("tokens_fallback_total") or 0),
        "duration_seconds_total": float(volume.get("duration_seconds_total") or 0),
        "duration_seconds_avg": float(volume.get("duration_seconds_avg") or 0),
        "duration_seconds_max": float(volume.get("duration_seconds_max") or 0),
        "api_calls_total": int(volume.get("api_calls_total") or 0),
        "api_calls_avg": float(volume.get("api_calls_avg") or 0),
        "tool_calls_total": int(volume.get("tool_calls_total") or 0),
        "tool_calls_avg": float(volume.get("tool_calls_avg") or 0),
        "providers_json": mix.get("providers") or {},
        "platforms_json": mix.get("platforms") or {},
        "models_json": mix.get("models") or {},
        "nonfinal_backlog_share_pct": float(backlog.get("value_pct") or 0),
        "nonfinal_backlog_numerator": int(backlog.get("numerator") or 0),
        "nonfinal_backlog_denominator": int(backlog.get("denominator") or 0),
        "largest_session_family_token_share_pct": float(load.get("largest_session_family_token_share_pct") or 0),
        "memory_status": memory.get("status") or "none",
        "memory_trace_visible_memory": int(memory_visible.get("memory") or 0),
        "memory_trace_visible_session_search": int(memory_visible.get("session_search") or 0),
        "memory_trace_visible_brv_query": int(memory_visible.get("brv_query") or 0),
        "memory_trace_visible_brv_curate": int(memory_visible.get("brv_curate") or 0),
        "memory_local_lower_bound_memory": int(memory_local.get("memory") or 0),
        "memory_local_lower_bound_session_search": int(memory_local.get("session_search") or 0),
        "memory_local_lower_bound_brv_query": int(memory_local.get("brv_query") or 0),
        "memory_local_lower_bound_brv_curate": int(memory_local.get("brv_curate") or 0),
        "memory_lifecycle_inject": int(memory_lifecycle.get("memory_inject") or 0),
        "memory_lifecycle_recall": int(memory_lifecycle.get("memory_recall") or 0),
        "memory_lifecycle_sync": int(memory_lifecycle.get("memory_sync") or 0),
        "delegation_proven_parent_batches": int(delegation.get("proven_parent_batches") or 0),
        "delegation_child_sessions": int(delegation.get("child_sessions") or 0),
        "delegation_child_traces": int(delegation.get("child_traces") or 0),
        "delegation_finalized_child_traces": int(delegation.get("finalized_child_traces") or 0),
        "delegation_delegated_tagged_child_traces": int(delegation.get("delegated_tagged_child_traces") or 0),
        "delegation_parent_session_id_marked_child_traces": int(delegation.get("parent_session_id_marked_child_traces") or 0),
        "delegation_child_tokens_total": int(delegation.get("child_tokens_total") or 0),
        "delegation_child_api_calls_total": int(delegation.get("child_api_calls_total") or 0),
        "delegation_child_tool_calls_total": int(delegation.get("child_tool_calls_total") or 0),
        "largest_batch_parent_session_id": largest_batch.get("parent_session_id") or None,
        "largest_batch_parent_trace_ids_json": largest_batch.get("parent_trace_ids") or [],
        "largest_batch_child_trace_count": int(largest_batch.get("child_trace_count") or 0),
        "largest_batch_child_tokens_total": int(largest_batch.get("child_tokens_total") or 0),
        "max_tokens_trace_id": max_tokens.get("trace_id") or None,
        "max_tokens_session_id": max_tokens.get("session_id") or None,
        "max_tokens_platform": max_tokens.get("platform") or None,
        "max_tokens_provider": max_tokens.get("provider") or None,
        "max_tokens_model": max_tokens.get("model") or None,
        "max_tokens_value": float(max_tokens.get("metric_value") or 0),
        "max_duration_trace_id": max_duration.get("trace_id") or None,
        "max_duration_session_id": max_duration.get("session_id") or None,
        "max_duration_platform": max_duration.get("platform") or None,
        "max_duration_provider": max_duration.get("provider") or None,
        "max_duration_model": max_duration.get("model") or None,
        "max_duration_value": float(max_duration.get("metric_value") or 0),
        "max_tool_calls_trace_id": max_tool_calls.get("trace_id") or None,
        "max_tool_calls_session_id": max_tool_calls.get("session_id") or None,
        "max_tool_calls_platform": max_tool_calls.get("platform") or None,
        "max_tool_calls_provider": max_tool_calls.get("provider") or None,
        "max_tool_calls_model": max_tool_calls.get("model") or None,
        "max_tool_calls_value": int(max_tool_calls.get("metric_value") or 0),
        "caveats_json": report.get("caveats") or [],
        "raw_report_json": report,
    }


def build_hourly_rollup_session_families(report: JSON) -> list[JSON]:
    families = ((report.get("load") or {}).get("top_session_families") or [])
    rows: list[JSON] = []
    for idx, family in enumerate(families, start=1):
        rows.append(
            {
                "rank": idx,
                "session_id": family.get("session_id") or "",
                "trace_count": int(family.get("trace_count") or 0),
                "tokens_total": int(family.get("tokens_total") or 0),
                "duration_seconds_total": float(family.get("duration_seconds_total") or 0),
            }
        )
    return rows


def build_hourly_rollup_tool_counts(report: JSON) -> list[JSON]:
    tool_hotspots = report.get("tool_hotspots") or {}
    rows: list[JSON] = []
    for source in ("opik_completed_tool_spans", "local_assistant_tool_calls"):
        block = tool_hotspots.get(source) or {}
        counts = block.get("counts") or {}
        for tool_name in sorted(counts):
            rows.append(
                {
                    "source": source,
                    "tool_name": tool_name,
                    "call_count": int(counts.get(tool_name) or 0),
                    "is_lower_bound": bool(block.get("is_lower_bound", False)),
                    "lower_bound_reason": block.get("lower_bound_reason") or "",
                }
            )
    return rows


def build_hourly_rollup_alerts(report: JSON) -> list[JSON]:
    alerts = report.get("alerts") or []
    return [
        {
            "code": alert.get("code") or "",
            "severity": alert.get("severity") or "info",
            "metric": alert.get("metric"),
            "metric_value": float(alert.get("value") or 0),
            "threshold_value": float(alert.get("threshold") or 0),
            "why": alert.get("why") or None,
        }
        for alert in alerts
    ]


def build_daily_rollup_base(rollup: JSON, markdown_report: str | None = None) -> JSON:
    window = rollup.get("window") or {}
    counts = rollup.get("counts") or {}
    coverage = rollup.get("coverage") or {}
    volume = rollup.get("volume") or {}
    mix = rollup.get("mix") or {}
    hourly_alerts = rollup.get("hourly_alert_frequencies") or {}
    peaks = rollup.get("peaks") or {}
    concentration = rollup.get("concentration") or {}
    memory = rollup.get("memory") or {}
    delegation = rollup.get("delegation") or {}
    outliers = rollup.get("outliers") or {}
    max_tokens = outliers.get("max_tokens_trace") or {}
    max_duration = outliers.get("max_duration_trace") or {}
    max_tool_calls = outliers.get("max_tool_calls_trace") or {}
    memory_visible = memory.get("trace_visible_tools") or {}
    memory_local = memory.get("local_tools_lower_bound") or {}
    memory_lifecycle = memory.get("lifecycle_spans_lower_bound") or {}
    llm_cov = coverage.get("llm_span_coverage") or {}
    local_cov = coverage.get("local_session_coverage") or {}
    report_date = str(window.get("to_utc") or "")[:10] or None

    return {
        "project_name": window.get("project_name") or "",
        "project_id": window.get("project_id") or None,
        "report_date": report_date,
        "generated_at_utc": rollup.get("generated_at_utc"),
        "window_from_utc": window.get("from_utc"),
        "window_to_utc": window.get("to_utc"),
        "duration_hours": int(window.get("duration_hours") or 24),
        "artifact_latest_path": ((rollup.get("collection") or {}).get("path_latest")),
        "artifact_stamped_path": ((rollup.get("collection") or {}).get("path_stamped")),
        "markdown_report": markdown_report,
        "raw_trace_count": int(counts.get("raw_trace_count") or 0),
        "excluded_self_count": int(counts.get("excluded_self_count") or 0),
        "excluded_nonfinal_count": int(counts.get("excluded_nonfinal_count") or 0),
        "excluded_diagnostic_count": int(counts.get("excluded_diagnostic_count") or 0),
        "completed_telemetry_loss_count": int(counts.get("completed_telemetry_loss_count") or 0),
        "true_empty_dispatch_count": int(counts.get("true_empty_dispatch_count") or 0),
        "kept_finalized_workload_count": int(counts.get("kept_finalized_workload_count") or 0),
        "completed_failed_or_interrupted_count": int(counts.get("completed_failed_or_interrupted_count") or 0),
        "llm_span_coverage_pct": float(llm_cov.get("value_pct") or 0),
        "llm_span_coverage_numerator": int(llm_cov.get("numerator") or 0),
        "llm_span_coverage_denominator": int(llm_cov.get("denominator") or 0),
        "local_session_coverage_pct": float(local_cov.get("value_pct") or 0),
        "local_session_coverage_numerator": int(local_cov.get("numerator") or 0),
        "local_session_coverage_denominator": int(local_cov.get("denominator") or 0),
        "tokens_total_measured": int(volume.get("tokens_total_measured") or 0),
        "tokens_usage_total": int(volume.get("tokens_usage_total") or 0),
        "tokens_fallback_total": int(volume.get("tokens_fallback_total") or 0),
        "duration_seconds_total": float(volume.get("duration_seconds_total") or 0),
        "duration_seconds_avg": float(volume.get("duration_seconds_avg") or 0),
        "duration_seconds_max": float(volume.get("duration_seconds_max") or 0),
        "api_calls_total": int(volume.get("api_calls_total") or 0),
        "tool_calls_total": int(volume.get("tool_calls_total") or 0),
        "providers_json": mix.get("providers") or {},
        "platforms_json": mix.get("platforms") or {},
        "models_json": mix.get("models") or {},
        "hourly_backlog_pressure_count": int(hourly_alerts.get("backlog_pressure") or 0),
        "hourly_telemetry_loss_count": int(hourly_alerts.get("telemetry_loss") or 0),
        "hourly_true_empty_dispatch_count": int(hourly_alerts.get("true_empty_dispatch") or 0),
        "hourly_memory_active_count": int(hourly_alerts.get("memory_active") or 0),
        "hourly_delegated_batches_present_count": int(hourly_alerts.get("delegated_batches_present") or 0),
        "hourly_provider_shift_count": int(hourly_alerts.get("provider_shift") or 0),
        "hourly_rate_limit_caveat_count": int(hourly_alerts.get("rate_limit_caveat") or 0),
        "peak_hour_by_tokens_utc": peaks.get("peak_hour_by_tokens_utc") or None,
        "peak_hour_by_trace_count_utc": peaks.get("peak_hour_by_trace_count_utc") or None,
        "peak_hour_by_backlog_utc": peaks.get("peak_hour_by_backlog_utc") or None,
        "largest_session_family_token_share_pct": float(concentration.get("largest_session_family_token_share_pct") or 0),
        "memory_status": memory.get("status") or "none",
        "memory_trace_visible_memory": int(memory_visible.get("memory") or 0),
        "memory_trace_visible_session_search": int(memory_visible.get("session_search") or 0),
        "memory_trace_visible_brv_query": int(memory_visible.get("brv_query") or 0),
        "memory_trace_visible_brv_curate": int(memory_visible.get("brv_curate") or 0),
        "memory_local_lower_bound_memory": int(memory_local.get("memory") or 0),
        "memory_local_lower_bound_session_search": int(memory_local.get("session_search") or 0),
        "memory_local_lower_bound_brv_query": int(memory_local.get("brv_query") or 0),
        "memory_local_lower_bound_brv_curate": int(memory_local.get("brv_curate") or 0),
        "memory_lifecycle_inject": int(memory_lifecycle.get("memory_inject") or 0),
        "memory_lifecycle_recall": int(memory_lifecycle.get("memory_recall") or 0),
        "memory_lifecycle_sync": int(memory_lifecycle.get("memory_sync") or 0),
        "delegation_proven_parent_batches": int(delegation.get("proven_parent_batches") or 0),
        "delegation_child_sessions": int(delegation.get("child_sessions") or 0),
        "delegation_child_traces": int(delegation.get("child_traces") or 0),
        "delegation_finalized_child_traces": int(delegation.get("finalized_child_traces") or 0),
        "delegation_delegated_tagged_child_traces": int(delegation.get("delegated_tagged_child_traces") or 0),
        "delegation_parent_session_id_marked_child_traces": int(delegation.get("parent_session_id_marked_child_traces") or 0),
        "delegation_child_tokens_total": int(delegation.get("child_tokens_total") or 0),
        "delegation_child_api_calls_total": int(delegation.get("child_api_calls_total") or 0),
        "delegation_child_tool_calls_total": int(delegation.get("child_tool_calls_total") or 0),
        "max_tokens_trace_id": max_tokens.get("trace_id") or None,
        "max_tokens_session_id": max_tokens.get("session_id") or None,
        "max_tokens_platform": max_tokens.get("platform") or None,
        "max_tokens_provider": max_tokens.get("provider") or None,
        "max_tokens_model": max_tokens.get("model") or None,
        "max_tokens_value": float(max_tokens.get("metric_value") or 0),
        "max_duration_trace_id": max_duration.get("trace_id") or None,
        "max_duration_session_id": max_duration.get("session_id") or None,
        "max_duration_platform": max_duration.get("platform") or None,
        "max_duration_provider": max_duration.get("provider") or None,
        "max_duration_model": max_duration.get("model") or None,
        "max_duration_value": float(max_duration.get("metric_value") or 0),
        "max_tool_calls_trace_id": max_tool_calls.get("trace_id") or None,
        "max_tool_calls_session_id": max_tool_calls.get("session_id") or None,
        "max_tool_calls_platform": max_tool_calls.get("platform") or None,
        "max_tool_calls_provider": max_tool_calls.get("provider") or None,
        "max_tool_calls_model": max_tool_calls.get("model") or None,
        "max_tool_calls_value": int(max_tool_calls.get("metric_value") or 0),
        "escalation_codes_json": rollup.get("escalation_codes") or [],
        "caveats_json": rollup.get("caveats") or [],
        "raw_rollup_json": rollup,
    }


def build_daily_rollup_session_families(rollup: JSON) -> list[JSON]:
    families = ((rollup.get("concentration") or {}).get("top_session_families") or [])
    rows: list[JSON] = []
    for idx, family in enumerate(families, start=1):
        rows.append(
            {
                "rank": idx,
                "session_id": family.get("session_id") or "",
                "trace_count": int(family.get("trace_count") or 0),
                "tokens_total": int(family.get("tokens_total") or 0),
                "duration_seconds_total": float(family.get("duration_seconds_total") or 0),
            }
        )
    return rows


def build_daily_rollup_tool_counts(rollup: JSON) -> list[JSON]:
    tool_hotspots = rollup.get("tool_hotspots") or {}
    rows: list[JSON] = []
    for source in ("opik_completed_tool_spans", "local_assistant_tool_calls"):
        block = tool_hotspots.get(source) or {}
        counts = block.get("counts") or {}
        for tool_name in sorted(counts):
            rows.append(
                {
                    "source": source,
                    "tool_name": tool_name,
                    "call_count": int(counts.get(tool_name) or 0),
                    "is_lower_bound": bool(block.get("is_lower_bound", False)),
                    "lower_bound_reason": block.get("lower_bound_reason") or "",
                }
            )
    return rows


def build_daily_rollup_escalations(rollup: JSON) -> list[JSON]:
    return [{"code": str(code)} for code in (rollup.get("escalation_codes") or [])]


_HOURLY_BASE_COLUMNS = [
    "project_name", "project_id", "generated_at_utc", "window_from_utc", "window_to_utc",
    "duration_minutes", "frozen_cutoff", "trace_pull_method", "mcp_baseline_checked", "sdk_paged",
    "trace_page_size", "span_crawl_attempted", "span_crawl_rate_limited", "prior_report_path",
    "prior_report_found", "materially_changed_vs_prior", "artifact_latest_path", "artifact_stamped_path",
    "raw_trace_count", "workload_trace_count", "kept_finalized_workload_count", "excluded_self_count",
    "excluded_nonfinal_count", "excluded_diagnostic_count", "completed_telemetry_loss_count",
    "true_empty_dispatch_count", "completed_with_telemetry_count", "completed_failed_or_interrupted_count",
    "llm_span_coverage_pct", "llm_span_coverage_numerator", "llm_span_coverage_denominator",
    "local_session_coverage_pct", "local_session_coverage_numerator", "local_session_coverage_denominator",
    "memory_tool_span_visibility_pct", "memory_tool_span_visibility_numerator", "memory_tool_span_visibility_denominator",
    "tokens_total_measured", "tokens_usage_total", "tokens_fallback_total", "duration_seconds_total",
    "duration_seconds_avg", "duration_seconds_max", "api_calls_total", "api_calls_avg", "tool_calls_total",
    "tool_calls_avg", "providers_json", "platforms_json", "models_json", "nonfinal_backlog_share_pct",
    "nonfinal_backlog_numerator", "nonfinal_backlog_denominator", "largest_session_family_token_share_pct",
    "memory_status", "memory_trace_visible_memory", "memory_trace_visible_session_search",
    "memory_trace_visible_brv_query", "memory_trace_visible_brv_curate", "memory_local_lower_bound_memory",
    "memory_local_lower_bound_session_search", "memory_local_lower_bound_brv_query",
    "memory_local_lower_bound_brv_curate", "memory_lifecycle_inject", "memory_lifecycle_recall",
    "memory_lifecycle_sync", "delegation_proven_parent_batches", "delegation_child_sessions",
    "delegation_child_traces", "delegation_finalized_child_traces", "delegation_delegated_tagged_child_traces",
    "delegation_parent_session_id_marked_child_traces", "delegation_child_tokens_total",
    "delegation_child_api_calls_total", "delegation_child_tool_calls_total", "largest_batch_parent_session_id",
    "largest_batch_parent_trace_ids_json", "largest_batch_child_trace_count", "largest_batch_child_tokens_total",
    "max_tokens_trace_id", "max_tokens_session_id", "max_tokens_platform", "max_tokens_provider", "max_tokens_model",
    "max_tokens_value", "max_duration_trace_id", "max_duration_session_id", "max_duration_platform",
    "max_duration_provider", "max_duration_model", "max_duration_value", "max_tool_calls_trace_id",
    "max_tool_calls_session_id", "max_tool_calls_platform", "max_tool_calls_provider", "max_tool_calls_model",
    "max_tool_calls_value", "caveats_json", "raw_report_json",
]

_DAILY_BASE_COLUMNS = [
    "project_name", "project_id", "report_date", "generated_at_utc", "window_from_utc", "window_to_utc",
    "duration_hours", "artifact_latest_path", "artifact_stamped_path", "markdown_report", "raw_trace_count",
    "excluded_self_count", "excluded_nonfinal_count", "excluded_diagnostic_count", "completed_telemetry_loss_count",
    "true_empty_dispatch_count", "kept_finalized_workload_count", "completed_failed_or_interrupted_count",
    "llm_span_coverage_pct", "llm_span_coverage_numerator", "llm_span_coverage_denominator",
    "local_session_coverage_pct", "local_session_coverage_numerator", "local_session_coverage_denominator",
    "tokens_total_measured", "tokens_usage_total", "tokens_fallback_total", "duration_seconds_total",
    "duration_seconds_avg", "duration_seconds_max", "api_calls_total", "tool_calls_total", "providers_json",
    "platforms_json", "models_json", "hourly_backlog_pressure_count", "hourly_telemetry_loss_count",
    "hourly_true_empty_dispatch_count", "hourly_memory_active_count", "hourly_delegated_batches_present_count",
    "hourly_provider_shift_count", "hourly_rate_limit_caveat_count", "peak_hour_by_tokens_utc",
    "peak_hour_by_trace_count_utc", "peak_hour_by_backlog_utc", "largest_session_family_token_share_pct",
    "memory_status", "memory_trace_visible_memory", "memory_trace_visible_session_search",
    "memory_trace_visible_brv_query", "memory_trace_visible_brv_curate", "memory_local_lower_bound_memory",
    "memory_local_lower_bound_session_search", "memory_local_lower_bound_brv_query",
    "memory_local_lower_bound_brv_curate", "memory_lifecycle_inject", "memory_lifecycle_recall",
    "memory_lifecycle_sync", "delegation_proven_parent_batches", "delegation_child_sessions",
    "delegation_child_traces", "delegation_finalized_child_traces", "delegation_delegated_tagged_child_traces",
    "delegation_parent_session_id_marked_child_traces", "delegation_child_tokens_total",
    "delegation_child_api_calls_total", "delegation_child_tool_calls_total",
    "max_tokens_trace_id", "max_tokens_session_id", "max_tokens_platform", "max_tokens_provider", "max_tokens_model",
    "max_tokens_value", "max_duration_trace_id", "max_duration_session_id", "max_duration_platform",
    "max_duration_provider", "max_duration_model", "max_duration_value", "max_tool_calls_trace_id",
    "max_tool_calls_session_id", "max_tool_calls_platform", "max_tool_calls_provider", "max_tool_calls_model",
    "max_tool_calls_value", "escalation_codes_json", "caveats_json", "raw_rollup_json",
]

_JSON_COLUMNS = {
    "providers_json", "platforms_json", "models_json", "largest_batch_parent_trace_ids_json",
    "caveats_json", "raw_report_json", "escalation_codes_json", "raw_rollup_json",
}


def _prepare_values(columns: list[str], row: JSON) -> list[Any]:
    values: list[Any] = []
    for col in columns:
        val = row.get(col)
        if col in _JSON_COLUMNS:
            values.append(_dump_json(val if val is not None else ([] if col.endswith('_json') and col.startswith('caveats') else {})))
        else:
            values.append(val)
    return values


def _connect(dsn: str):
    if psycopg is None:
        raise RuntimeError("psycopg is required to persist Opik rollups")
    return psycopg.connect(dsn, connect_timeout=10)


def persist_hourly_rollup(report: JSON, dsn: str | None = None) -> int:
    dsn = dsn or get_hq_postgres_dsn()
    base = build_hourly_rollup_base(report)
    families = build_hourly_rollup_session_families(report)
    tool_counts = build_hourly_rollup_tool_counts(report)
    alerts = build_hourly_rollup_alerts(report)

    insert_cols = ", ".join(_HOURLY_BASE_COLUMNS)
    value_exprs = ", ".join(f"%s::jsonb" if c in _JSON_COLUMNS else "%s" for c in _HOURLY_BASE_COLUMNS)
    update_clause = ", ".join(f"{col} = excluded.{col}" for col in _HOURLY_BASE_COLUMNS if col not in {"project_name", "window_to_utc"})

    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into signals.opik_hourly_rollups ({insert_cols})
                values ({value_exprs})
                on conflict (project_name, window_to_utc)
                do update set {update_clause}, updated_at = now()
                returning hourly_rollup_id
                """,
                _prepare_values(_HOURLY_BASE_COLUMNS, base),
            )
            hourly_rollup_id = int(cur.fetchone()[0])

            cur.execute("delete from signals.opik_hourly_rollup_session_families where hourly_rollup_id = %s", (hourly_rollup_id,))
            cur.execute("delete from signals.opik_hourly_rollup_tool_counts where hourly_rollup_id = %s", (hourly_rollup_id,))
            cur.execute("delete from signals.opik_hourly_rollup_alerts where hourly_rollup_id = %s", (hourly_rollup_id,))

            if families:
                cur.executemany(
                    """
                    insert into signals.opik_hourly_rollup_session_families (
                        hourly_rollup_id, rank, session_id, trace_count, tokens_total, duration_seconds_total
                    ) values (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            hourly_rollup_id,
                            row["rank"],
                            row["session_id"],
                            row["trace_count"],
                            row["tokens_total"],
                            row["duration_seconds_total"],
                        )
                        for row in families
                    ],
                )
            if tool_counts:
                cur.executemany(
                    """
                    insert into signals.opik_hourly_rollup_tool_counts (
                        hourly_rollup_id, source, tool_name, call_count, is_lower_bound, lower_bound_reason
                    ) values (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            hourly_rollup_id,
                            row["source"],
                            row["tool_name"],
                            row["call_count"],
                            row["is_lower_bound"],
                            row["lower_bound_reason"],
                        )
                        for row in tool_counts
                    ],
                )
            if alerts:
                cur.executemany(
                    """
                    insert into signals.opik_hourly_rollup_alerts (
                        hourly_rollup_id, code, severity, metric, metric_value, threshold_value, why
                    ) values (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            hourly_rollup_id,
                            row["code"],
                            row["severity"],
                            row["metric"],
                            row["metric_value"],
                            row["threshold_value"],
                            row["why"],
                        )
                        for row in alerts
                    ],
                )
        conn.commit()
    return hourly_rollup_id


def persist_daily_rollup(rollup: JSON, markdown_report: str | None = None, dsn: str | None = None) -> int:
    dsn = dsn or get_hq_postgres_dsn()
    base = build_daily_rollup_base(rollup, markdown_report=markdown_report)
    families = build_daily_rollup_session_families(rollup)
    tool_counts = build_daily_rollup_tool_counts(rollup)
    escalations = build_daily_rollup_escalations(rollup)

    insert_cols = ", ".join(_DAILY_BASE_COLUMNS)
    value_exprs = ", ".join(f"%s::jsonb" if c in _JSON_COLUMNS else "%s" for c in _DAILY_BASE_COLUMNS)
    update_clause = ", ".join(f"{col} = excluded.{col}" for col in _DAILY_BASE_COLUMNS if col not in {"project_name", "report_date"})

    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                insert into signals.opik_daily_rollups ({insert_cols})
                values ({value_exprs})
                on conflict (project_name, report_date)
                do update set {update_clause}, updated_at = now()
                returning daily_rollup_id
                """,
                _prepare_values(_DAILY_BASE_COLUMNS, base),
            )
            daily_rollup_id = int(cur.fetchone()[0])

            cur.execute("delete from signals.opik_daily_rollup_session_families where daily_rollup_id = %s", (daily_rollup_id,))
            cur.execute("delete from signals.opik_daily_rollup_tool_counts where daily_rollup_id = %s", (daily_rollup_id,))
            cur.execute("delete from signals.opik_daily_rollup_escalations where daily_rollup_id = %s", (daily_rollup_id,))

            if families:
                cur.executemany(
                    """
                    insert into signals.opik_daily_rollup_session_families (
                        daily_rollup_id, rank, session_id, trace_count, tokens_total, duration_seconds_total
                    ) values (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            daily_rollup_id,
                            row["rank"],
                            row["session_id"],
                            row["trace_count"],
                            row["tokens_total"],
                            row["duration_seconds_total"],
                        )
                        for row in families
                    ],
                )
            if tool_counts:
                cur.executemany(
                    """
                    insert into signals.opik_daily_rollup_tool_counts (
                        daily_rollup_id, source, tool_name, call_count, is_lower_bound, lower_bound_reason
                    ) values (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            daily_rollup_id,
                            row["source"],
                            row["tool_name"],
                            row["call_count"],
                            row["is_lower_bound"],
                            row["lower_bound_reason"],
                        )
                        for row in tool_counts
                    ],
                )
            if escalations:
                cur.executemany(
                    """
                    insert into signals.opik_daily_rollup_escalations (
                        daily_rollup_id, code
                    ) values (%s, %s)
                    """,
                    [(daily_rollup_id, row["code"]) for row in escalations],
                )
        conn.commit()
    return daily_rollup_id
