import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from cron.opik_rollup_persistence import (
    build_daily_rollup_base,
    build_daily_rollup_escalations,
    build_daily_rollup_session_families,
    build_daily_rollup_tool_counts,
    build_hourly_rollup_alerts,
    build_hourly_rollup_base,
    build_hourly_rollup_session_families,
    build_hourly_rollup_tool_counts,
)


def test_build_hourly_rollup_payload_maps_typed_fields_and_children():
    report = {
        "generated_at_utc": "2026-04-23T04:37:24.686618Z",
        "window": {
            "project_name": "hermes-agent",
            "project_id": "proj_123",
            "from_utc": "2026-04-23T03:07:24.686618Z",
            "to_utc": "2026-04-23T04:37:24.686618Z",
            "duration_minutes": 90,
            "frozen_cutoff": True,
        },
        "collection": {
            "trace_pull_method": "opik_python_sdk",
            "mcp_baseline_checked": False,
            "sdk_paged": True,
            "trace_page_size": 500,
            "span_crawl_attempted": True,
            "span_crawl_rate_limited": False,
            "prior_report_path": "/tmp/prior.json",
            "prior_report_found": True,
            "materially_changed_vs_prior": True,
            "path_latest": "/tmp/latest.json",
            "path_stamped": "/tmp/2026-04-23T04-37-24Z.json",
        },
        "counts": {
            "raw_trace_count": 79,
            "workload_trace_count": 67,
            "kept_finalized_workload_count": 46,
            "excluded_self_count": 2,
            "excluded_nonfinal_count": 21,
            "excluded_diagnostic_count": 10,
            "completed_telemetry_loss_count": 0,
            "true_empty_dispatch_count": 0,
            "completed_with_telemetry_count": 46,
            "completed_failed_or_interrupted_count": 0,
        },
        "coverage": {
            "llm_span_coverage": {"value_pct": 100.0, "numerator": 46, "denominator": 46},
            "local_session_coverage": {"value_pct": 66.667, "numerator": 26, "denominator": 39},
            "memory_tool_span_visibility": {"value_pct": 0.0, "numerator": 19, "denominator": 19},
        },
        "volume": {
            "tokens_total_measured": 68368356,
            "tokens_usage_total": 68368356,
            "tokens_fallback_total": 0,
            "duration_seconds_total": 10335.45,
            "duration_seconds_avg": 224.68,
            "duration_seconds_max": 3622.18,
            "api_calls_total": 490,
            "api_calls_avg": 10.65,
            "tool_calls_total": 880,
            "tool_calls_avg": 19.13,
        },
        "mix": {
            "providers": {"openai-codex": 46},
            "platforms": {"discord": 23, "cron": 23},
            "models": {"gpt-5.4": 36},
        },
        "load": {
            "nonfinal_backlog_share": {"value_pct": 26.582, "numerator": 21, "denominator": 79},
            "largest_session_family_token_share_pct": 40.467,
            "top_session_families": [
                {"session_id": "s1", "trace_count": 2, "tokens_total": 111, "duration_seconds_total": 22.5},
                {"session_id": "s2", "trace_count": 1, "tokens_total": 25, "duration_seconds_total": 7.5},
            ],
        },
        "tool_hotspots": {
            "opik_completed_tool_spans": {
                "is_lower_bound": False,
                "lower_bound_reason": "",
                "counts": {"terminal": 138, "memory": 15},
            },
            "local_assistant_tool_calls": {
                "is_lower_bound": True,
                "lower_bound_reason": "local matched sessions only",
                "counts": {"todo": 10},
            },
        },
        "memory": {
            "status": "active",
            "trace_visible_tools": {"memory": 15, "session_search": 2, "brv_query": 0, "brv_curate": 2},
            "local_tools_lower_bound": {"memory": 0, "session_search": 0, "brv_query": 0, "brv_curate": 0},
            "lifecycle_spans_lower_bound": {"memory_inject": 22, "memory_recall": 26, "memory_sync": 28},
        },
        "delegation": {
            "proven_parent_batches": 0,
            "child_sessions": 0,
            "child_traces": 0,
            "finalized_child_traces": 0,
            "delegated_tagged_child_traces": 0,
            "parent_session_id_marked_child_traces": 0,
            "child_tokens_total": 0,
            "child_api_calls_total": 0,
            "child_tool_calls_total": 0,
            "largest_batch": {
                "parent_session_id": "",
                "parent_trace_ids": [],
                "child_trace_count": 0,
                "child_tokens_total": 0,
            },
        },
        "outliers": {
            "max_tokens_trace": {
                "trace_id": "trace_a",
                "session_id": "s1",
                "platform": "discord",
                "provider": "openai-codex",
                "model": "gpt-5.4",
                "metric_value": 23905150.0,
            },
            "max_duration_trace": {
                "trace_id": "trace_b",
                "session_id": "s2",
                "platform": "cron",
                "provider": "openai-codex",
                "model": "gpt-5.4",
                "metric_value": 3622.18,
            },
            "max_tool_calls_trace": {
                "trace_id": "trace_c",
                "session_id": "s3",
                "platform": "cron",
                "provider": "openai-codex",
                "model": "gpt-5.4",
                "metric_value": 134,
            },
        },
        "alerts": [
            {"code": "delegated_lineage_gap", "severity": "info", "metric": "local_session_coverage_pct", "value": 66.667, "threshold": 80.0, "why": "Only partial final-session visibility in local state DB."}
        ],
        "caveats": ["local_session_coverage_partial"],
    }

    base = build_hourly_rollup_base(report)
    families = build_hourly_rollup_session_families(report)
    tool_counts = build_hourly_rollup_tool_counts(report)
    alerts = build_hourly_rollup_alerts(report)

    assert base["project_name"] == "hermes-agent"
    assert base["project_id"] == "proj_123"
    assert base["window_to_utc"] == "2026-04-23T04:37:24.686618Z"
    assert base["artifact_latest_path"] == "/tmp/latest.json"
    assert base["raw_trace_count"] == 79
    assert base["providers_json"] == {"openai-codex": 46}
    assert base["largest_batch_parent_trace_ids_json"] == []
    assert base["raw_report_json"] == report

    assert families == [
        {"rank": 1, "session_id": "s1", "trace_count": 2, "tokens_total": 111, "duration_seconds_total": 22.5},
        {"rank": 2, "session_id": "s2", "trace_count": 1, "tokens_total": 25, "duration_seconds_total": 7.5},
    ]
    assert tool_counts == [
        {"source": "opik_completed_tool_spans", "tool_name": "memory", "call_count": 15, "is_lower_bound": False, "lower_bound_reason": ""},
        {"source": "opik_completed_tool_spans", "tool_name": "terminal", "call_count": 138, "is_lower_bound": False, "lower_bound_reason": ""},
        {"source": "local_assistant_tool_calls", "tool_name": "todo", "call_count": 10, "is_lower_bound": True, "lower_bound_reason": "local matched sessions only"},
    ]
    assert alerts == [
        {"code": "delegated_lineage_gap", "severity": "info", "metric": "local_session_coverage_pct", "metric_value": 66.667, "threshold_value": 80.0, "why": "Only partial final-session visibility in local state DB."}
    ]



def test_build_daily_rollup_payload_maps_typed_fields_and_children():
    rollup = {
        "generated_at_utc": "2026-04-23T06:00:00Z",
        "window": {
            "project_name": "hermes-agent",
            "project_id": "proj_123",
            "from_utc": "2026-04-22T06:00:00Z",
            "to_utc": "2026-04-23T06:00:00Z",
            "duration_hours": 24,
        },
        "counts": {
            "raw_trace_count": 300,
            "excluded_self_count": 50,
            "excluded_nonfinal_count": 20,
            "excluded_diagnostic_count": 10,
            "completed_telemetry_loss_count": 2,
            "true_empty_dispatch_count": 1,
            "kept_finalized_workload_count": 218,
            "completed_failed_or_interrupted_count": 3,
        },
        "coverage": {
            "llm_span_coverage": {"value_pct": 99.0, "numerator": 216, "denominator": 218},
            "local_session_coverage": {"value_pct": 88.0, "numerator": 176, "denominator": 200},
        },
        "volume": {
            "tokens_total_measured": 1000,
            "tokens_usage_total": 900,
            "tokens_fallback_total": 100,
            "duration_seconds_total": 500.5,
            "duration_seconds_avg": 2.2,
            "duration_seconds_max": 10.0,
            "api_calls_total": 99,
            "tool_calls_total": 120,
        },
        "mix": {
            "providers": {"openai-codex": 200},
            "platforms": {"discord": 100, "cron": 118},
            "models": {"gpt-5.4": 210},
        },
        "hourly_alert_frequencies": {
            "backlog_pressure": 3,
            "telemetry_loss": 1,
            "true_empty_dispatch": 1,
            "memory_active": 5,
            "delegated_batches_present": 2,
            "provider_shift": 0,
            "rate_limit_caveat": 1,
        },
        "peaks": {
            "peak_hour_by_tokens_utc": "2026-04-22T20:00:00Z",
            "peak_hour_by_trace_count_utc": "2026-04-22T21:00:00Z",
            "peak_hour_by_backlog_utc": "2026-04-22T22:00:00Z",
        },
        "concentration": {
            "largest_session_family_token_share_pct": 55.5,
            "top_session_families": [
                {"session_id": "s1", "trace_count": 4, "tokens_total": 500, "duration_seconds_total": 40.0}
            ],
        },
        "tool_hotspots": {
            "opik_completed_tool_spans": {
                "is_lower_bound": False,
                "lower_bound_reason": "",
                "counts": {"memory": 3, "terminal": 9},
            },
            "local_assistant_tool_calls": {
                "is_lower_bound": True,
                "lower_bound_reason": "local matched sessions only",
                "counts": {"todo": 4},
            },
        },
        "outliers": {
            "max_tokens_trace": {
                "trace_id": "trace_daily_a",
                "session_id": "s1",
                "platform": "discord",
                "provider": "openai-codex",
                "model": "gpt-5.4",
                "metric_value": 700.0,
            },
            "max_duration_trace": {
                "trace_id": "trace_daily_b",
                "session_id": "s2",
                "platform": "cron",
                "provider": "openai-codex",
                "model": "gpt-5.4",
                "metric_value": 30.5,
            },
            "max_tool_calls_trace": {
                "trace_id": "trace_daily_c",
                "session_id": "s3",
                "platform": "discord",
                "provider": "openai-codex",
                "model": "gpt-5.4",
                "metric_value": 14,
            },
        },
        "memory": {
            "status": "active",
            "trace_visible_tools": {"memory": 4, "session_search": 3, "brv_query": 1, "brv_curate": 1},
            "local_tools_lower_bound": {"memory": 2, "session_search": 1, "brv_query": 0, "brv_curate": 0},
            "lifecycle_spans_lower_bound": {"memory_inject": 8, "memory_recall": 9, "memory_sync": 10},
        },
        "delegation": {
            "proven_parent_batches": 2,
            "child_sessions": 5,
            "child_traces": 5,
            "finalized_child_traces": 4,
            "delegated_tagged_child_traces": 4,
            "parent_session_id_marked_child_traces": 4,
            "child_tokens_total": 1234,
            "child_api_calls_total": 50,
            "child_tool_calls_total": 60,
        },
        "escalation_codes": ["telemetry_loss", "backlog_pressure"],
        "caveats": ["local_session_coverage_partial"],
    }

    base = build_daily_rollup_base(rollup, markdown_report="# Daily Digest")
    families = build_daily_rollup_session_families(rollup)
    tool_counts = build_daily_rollup_tool_counts(rollup)
    escalations = build_daily_rollup_escalations(rollup)

    assert base["project_name"] == "hermes-agent"
    assert base["report_date"] == "2026-04-23"
    assert base["markdown_report"] == "# Daily Digest"
    assert base["hourly_memory_active_count"] == 5
    assert base["memory_trace_visible_memory"] == 4
    assert base["delegation_child_tokens_total"] == 1234
    assert base["max_tokens_trace_id"] == "trace_daily_a"
    assert base["max_tool_calls_value"] == 14
    assert base["escalation_codes_json"] == ["telemetry_loss", "backlog_pressure"]
    assert base["raw_rollup_json"] == rollup

    assert families == [
        {"rank": 1, "session_id": "s1", "trace_count": 4, "tokens_total": 500, "duration_seconds_total": 40.0}
    ]
    assert tool_counts == [
        {"source": "opik_completed_tool_spans", "tool_name": "memory", "call_count": 3, "is_lower_bound": False, "lower_bound_reason": ""},
        {"source": "opik_completed_tool_spans", "tool_name": "terminal", "call_count": 9, "is_lower_bound": False, "lower_bound_reason": ""},
        {"source": "local_assistant_tool_calls", "tool_name": "todo", "call_count": 4, "is_lower_bound": True, "lower_bound_reason": "local matched sessions only"},
    ]
    assert escalations == [
        {"code": "telemetry_loss"},
        {"code": "backlog_pressure"},
    ]
