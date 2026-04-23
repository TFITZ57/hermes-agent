import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import cron.opik_daily_digest_builder as digest_builder
from cron.opik_daily_digest_builder import render_daily_digest


def _sample_rollup():
    return {
        "schema_version": 1,
        "report_type": "opik_daily_digest_rollup",
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
            "provider_shift": 1,
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
                {"session_id": "s1", "trace_count": 4, "tokens_total": 500, "duration_seconds_total": 40.0},
                {"session_id": "s2", "trace_count": 2, "tokens_total": 250, "duration_seconds_total": 20.0},
            ],
        },
        "tool_hotspots": {
            "opik_completed_tool_spans": {
                "is_lower_bound": False,
                "lower_bound_reason": "",
                "counts": {"terminal": 9, "memory": 3, "session_search": 2},
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
        "collection": {
            "path_latest": "/tmp/latest.json",
            "path_stamped": "/tmp/2026-04-23.json",
        },
    }


def test_render_daily_digest_contains_required_sections_and_uses_rollup_values():
    markdown = render_daily_digest(_sample_rollup())

    assert "# Hermes Opik Daily Digest" in markdown
    assert "## Headline" in markdown
    assert "## 24h Scorecard" in markdown
    assert "## Trend Synthesis" in markdown
    assert "## Workload Concentration" in markdown
    assert "## Tool Hot Spots" in markdown
    assert "## Memory Activity" in markdown
    assert "## Delegated / Subagent Workloads" in markdown
    assert "## Escalations / Required Fixes" in markdown
    assert "## Actions for Tomorrow" in markdown
    assert "## Appendix: Structured Daily Rollup" in markdown
    assert "trace_daily_a" in markdown
    assert "terminal" in markdown
    assert "memory: 4" in markdown
    assert "session_search: 3" in markdown
    assert "```json" in markdown


def test_main_calls_daily_rollup_builder_path_before_printing_markdown(monkeypatch, tmp_path, capsys):
    rollup = _sample_rollup()
    latest_path = tmp_path / "latest.json"
    dated_path = tmp_path / "2026-04-23.json"
    calls = []

    def fake_build_daily_rollup():
        calls.append("build")
        return rollup.copy()

    def fake_write_structured_outputs(payload, output_dir, report_date):
        calls.append("write")
        return latest_path, dated_path

    captured = {}

    def fake_persist_daily_rollup(payload, markdown_report=None):
        calls.append("persist")
        captured["markdown_report"] = markdown_report
        return 123

    monkeypatch.setattr("cron.opik_daily_rollup_builder.build_daily_rollup", fake_build_daily_rollup)
    monkeypatch.setattr("cron.opik_daily_rollup_builder.write_structured_outputs", fake_write_structured_outputs)
    monkeypatch.setattr("cron.opik_rollup_persistence.persist_daily_rollup", fake_persist_daily_rollup)

    digest_builder.main()
    output = capsys.readouterr().out

    assert calls[:3] == ["build", "write", "persist"]
    assert "# Hermes Opik Daily Digest" in output
    assert "```json" in output
    assert captured["markdown_report"] == output.rstrip("\n")
