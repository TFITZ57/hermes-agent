import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import cron.opik_daily_rollup_builder as daily_builder
from cron.opik_daily_rollup_builder import (
    build_daily_rollup,
    load_hourly_rollups,
    summarize_hourly_rollups,
    write_structured_outputs,
)


def test_load_hourly_rollups_filters_window_and_deduplicates_latest_and_stamped(tmp_path):
    hourly_dir = tmp_path / "hourly"
    hourly_dir.mkdir()

    report = {
        "report_type": "opik_hourly_watch",
        "generated_at_utc": "2026-04-23T04:05:00Z",
        "window": {"to_utc": "2026-04-23T04:05:00Z"},
        "counts": {"excluded_nonfinal_count": 1, "kept_finalized_workload_count": 2},
        "volume": {"tokens_total_measured": 100},
        "alerts": [{"code": "backlog_pressure"}],
        "memory": {"status": "active"},
        "delegation": {"proven_parent_batches": 1},
        "mix": {"providers": {"openai-codex": 2}, "platforms": {"discord": 2}},
        "caveats": [],
    }
    (hourly_dir / "latest.json").write_text(json.dumps(report))
    (hourly_dir / "2026-04-23T04-05-00Z.json").write_text(json.dumps(report))
    (hourly_dir / "2026-04-21T04-05-00Z.json").write_text(json.dumps({**report, "generated_at_utc": "2026-04-21T04:05:00Z", "window": {"to_utc": "2026-04-21T04:05:00Z"}}))

    rows = load_hourly_rollups(
        output_dir=hourly_dir,
        from_dt=datetime(2026, 4, 22, 6, 0, tzinfo=timezone.utc),
        to_dt=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert rows[0]["window"]["to_utc"] == "2026-04-23T04:05:00Z"



def test_summarize_hourly_rollups_computes_alert_frequencies_and_peaks():
    hourly_reports = [
        {
            "window": {"to_utc": "2026-04-23T01:05:00Z"},
            "counts": {"excluded_nonfinal_count": 3, "kept_finalized_workload_count": 5},
            "volume": {"tokens_total_measured": 100},
            "alerts": [{"code": "backlog_pressure"}, {"code": "telemetry_loss"}],
            "memory": {"status": "active"},
            "delegation": {"proven_parent_batches": 1},
            "mix": {"providers": {"openai-codex": 5}, "platforms": {"discord": 5}},
            "caveats": [],
        },
        {
            "window": {"to_utc": "2026-04-23T02:05:00Z"},
            "counts": {"excluded_nonfinal_count": 1, "kept_finalized_workload_count": 7},
            "volume": {"tokens_total_measured": 250},
            "alerts": [{"code": "true_empty_dispatch"}],
            "memory": {"status": "none"},
            "delegation": {"proven_parent_batches": 0},
            "mix": {"providers": {"openai-codex": 6}, "platforms": {"discord": 4, "cron": 3}},
            "caveats": ["span_crawl_rate_limited"],
        },
    ]

    summary = summarize_hourly_rollups(hourly_reports)

    assert summary["hourly_alert_frequencies"] == {
        "backlog_pressure": 1,
        "telemetry_loss": 1,
        "true_empty_dispatch": 1,
        "memory_active": 1,
        "delegated_batches_present": 1,
        "provider_shift": 1,
        "rate_limit_caveat": 1,
    }
    assert summary["peaks"] == {
        "peak_hour_by_tokens_utc": "2026-04-23T02:05:00Z",
        "peak_hour_by_trace_count_utc": "2026-04-23T02:05:00Z",
        "peak_hour_by_backlog_utc": "2026-04-23T01:05:00Z",
    }



def test_write_structured_outputs_writes_latest_and_dated_daily_files(tmp_path):
    rollup = {"schema_version": 1, "generated_at_utc": "2026-04-23T06:00:00Z"}

    latest_path, dated_path = write_structured_outputs(
        rollup,
        output_dir=tmp_path,
        report_date="2026-04-23",
    )

    assert latest_path == tmp_path / "latest.json"
    assert dated_path == tmp_path / "2026-04-23.json"
    assert json.loads(latest_path.read_text()) == rollup
    assert json.loads(dated_path.read_text()) == rollup



def test_build_daily_rollup_includes_tool_hotspots_and_outliers(monkeypatch, tmp_path):
    class _Projects:
        def model_dump(self):
            return {"content": [{"name": "hermes-agent", "id": "proj_123"}]}

    class _ProjectsApi:
        def get_projects(self, page=1, size=100):
            return _Projects()

    class _RestClient:
        projects = _ProjectsApi()

    class _Client:
        rest_client = _RestClient()

    trace_rows = [
        {
            "id": "trace_a",
            "name": "hermes-session",
            "providers": ["openai-codex"],
            "metadata": {"providers": ["openai-codex"], "model": "gpt-5.4"},
            "input": {"session_id": "s1", "platform": "discord"},
            "output": {"duration_seconds": 12, "api_calls": 2, "tool_calls": 3},
            "usage": {"total_tokens": 100},
            "end_time": "2026-04-23T05:00:00Z",
            "start_time": "2026-04-23T04:59:00Z",
            "llm_span_count": 1,
        }
    ]
    span_rows = [
        {"trace_id": "trace_a", "type": "tool", "name": "tool:terminal", "end_time": "2026-04-23T05:00:01Z"},
        {"trace_id": "trace_a", "type": "tool", "name": "tool:memory", "end_time": "2026-04-23T05:00:02Z"},
    ]

    monkeypatch.setattr(daily_builder, "fetch_trace_rows", lambda client, from_dt, to_dt: trace_rows)
    monkeypatch.setattr(daily_builder, "fetch_span_rows", lambda client, from_dt, to_dt: (span_rows, True, False))
    monkeypatch.setattr(daily_builder, "load_local_session_rows", lambda session_ids, state_db_path: ({"s1": {}}, Counter({"todo": 4}), Counter({"memory": 1})))
    monkeypatch.setattr(daily_builder, "detect_delegated_batches", lambda *args, **kwargs: [])
    monkeypatch.setattr(daily_builder, "load_hourly_rollups", lambda hourly_dir, from_dt, to_dt: [])
    monkeypatch.setattr(
        daily_builder,
        "summarize_hourly_rollups",
        lambda hourly_reports: {
            "hourly_alert_frequencies": {
                "backlog_pressure": 0,
                "telemetry_loss": 0,
                "true_empty_dispatch": 0,
                "memory_active": 0,
                "delegated_batches_present": 0,
                "provider_shift": 0,
                "rate_limit_caveat": 0,
            },
            "peaks": {
                "peak_hour_by_tokens_utc": "",
                "peak_hour_by_trace_count_utc": "",
                "peak_hour_by_backlog_utc": "",
            },
        },
    )

    rollup = build_daily_rollup(
        now=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
        client=_Client(),
        state_db_path=tmp_path / "state.db",
        hourly_dir=tmp_path / "hourly",
    )

    assert rollup["tool_hotspots"]["opik_completed_tool_spans"]["counts"] == {"memory": 1, "terminal": 1}
    assert rollup["tool_hotspots"]["local_assistant_tool_calls"]["counts"] == {"todo": 4}
    assert rollup["outliers"]["max_tokens_trace"]["trace_id"] == "trace_a"
    assert rollup["outliers"]["max_duration_trace"]["trace_id"] == "trace_a"
    assert rollup["outliers"]["max_tool_calls_trace"]["trace_id"] == "trace_a"



def test_main_persists_daily_rollup_after_writing_outputs(monkeypatch, tmp_path):
    rollup = {
        "schema_version": 1,
        "report_type": "opik_daily_digest_rollup",
        "generated_at_utc": "2026-04-23T06:00:00Z",
        "window": {"to_utc": "2026-04-23T06:00:00Z"},
        "counts": {},
        "coverage": {},
        "volume": {},
        "mix": {},
        "hourly_alert_frequencies": {},
        "peaks": {},
        "concentration": {"top_session_families": []},
        "memory": {},
        "delegation": {},
        "escalation_codes": [],
        "caveats": [],
    }
    latest_path = tmp_path / "latest.json"
    dated_path = tmp_path / "2026-04-23.json"
    calls = []

    monkeypatch.setattr(daily_builder, "build_daily_rollup", lambda: rollup.copy())
    monkeypatch.setattr(daily_builder, "write_structured_outputs", lambda *_args, **_kwargs: (latest_path, dated_path))

    def capture_persist(payload, markdown_report=None):
        calls.append((payload, markdown_report))
        return 456

    monkeypatch.setattr("cron.opik_rollup_persistence.persist_daily_rollup", capture_persist)

    daily_builder.main()

    assert len(calls) == 1
    assert calls[0][0]["collection"]["path_latest"] == str(latest_path)
    assert calls[0][0]["collection"]["path_stamped"] == str(dated_path)
