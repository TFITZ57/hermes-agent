import json
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import cron.opik_hourly_watch_builder as hourly_builder
from cron.opik_hourly_watch_builder import (
    build_memory_tool_span_visibility,
    normalize_tool_span_name,
    summarize_memory_spans,
    trace_metrics,
    write_structured_outputs,
)


def test_normalize_tool_span_name_strips_tool_prefix_for_memory_tools():
    assert normalize_tool_span_name("tool:memory") == "memory"
    assert normalize_tool_span_name("tool:session_search") == "session_search"
    assert normalize_tool_span_name("tool:brv_query") == "brv_query"
    assert normalize_tool_span_name("tool:brv_curate") == "brv_curate"


def test_summarize_memory_spans_counts_completed_explicit_tools_and_lifecycle_separately():
    kept_trace_ids = {"t1", "t2"}
    spans = [
        {"trace_id": "t1", "type": "tool", "name": "tool:memory", "end_time": "2026-04-23T01:00:00Z"},
        {"trace_id": "t1", "type": "tool", "name": "tool:session_search", "metadata": {"tool_name": "session_search"}, "output": {"ok": True}},
        {"trace_id": "t2", "type": "tool", "name": "tool:brv_query", "endTime": "2026-04-23T01:00:02Z"},
        {"trace_id": "t2", "type": "tool", "name": "tool:brv_curate", "output": {"ok": True}},
        {"trace_id": "t2", "type": "tool", "name": "tool:memory"},
        {"trace_id": "t1", "type": "general", "name": "memory:inject"},
        {"trace_id": "t1", "type": "general", "name": "memory:recall"},
        {"trace_id": "t2", "type": "general", "name": "memory:sync"},
        {"trace_id": "other", "type": "tool", "name": "tool:memory", "end_time": "2026-04-23T01:00:03Z"},
    ]

    trace_visible_tools, lifecycle_counts = summarize_memory_spans(spans, kept_trace_ids)

    assert trace_visible_tools == {
        "memory": 1,
        "session_search": 1,
        "brv_query": 1,
        "brv_curate": 1,
    }
    assert lifecycle_counts == {
        "memory:inject": 1,
        "memory:recall": 1,
        "memory:sync": 1,
    }


def test_write_structured_outputs_writes_latest_and_timestamped_files(tmp_path):
    report = {"schema_version": 1, "generated_at_utc": "2026-04-23T02:05:00Z"}

    latest_path, dated_path = write_structured_outputs(
        report,
        output_dir=tmp_path,
        to_utc="2026-04-23T02:05:00Z",
    )

    assert latest_path == tmp_path / "latest.json"
    assert dated_path == tmp_path / "2026-04-23T02-05-00Z.json"
    assert json.loads(latest_path.read_text()) == report
    assert json.loads(dated_path.read_text()) == report


def test_trace_metrics_deduplicates_provider_names_within_a_trace():
    trace = {
        "id": "t1",
        "name": "hermes-session",
        "providers": ["openai-codex"],
        "metadata": {"providers": ["openai-codex"], "model": "gpt-5.4"},
        "input": {"session_id": "s1", "platform": "discord"},
        "output": {"duration_seconds": 12, "api_calls": 1, "tool_calls": 0},
        "usage": {"total_tokens": 100},
        "end_time": "2026-04-23T01:00:00Z",
        "start_time": "2026-04-23T00:59:48Z",
    }

    parsed = trace_metrics(trace)

    assert parsed["providers"] == ["openai-codex"]


def test_memory_tool_span_visibility_handles_zero_local_denominator_without_fake_ratio():
    coverage = build_memory_tool_span_visibility(
        trace_visible_tools={"memory": 6, "session_search": 10, "brv_query": 2, "brv_curate": 1},
        local_tools_lower_bound={"memory": 0, "session_search": 0, "brv_query": 0, "brv_curate": 0},
    )

    assert coverage == {
        "value_pct": 0.0,
        "numerator": 19,
        "denominator": 19,
    }


def test_main_persists_hourly_rollup_after_writing_outputs(monkeypatch, tmp_path):
    report = {
        "schema_version": 1,
        "report_type": "opik_hourly_watch",
        "generated_at_utc": "2026-04-23T05:00:00Z",
        "window": {"to_utc": "2026-04-23T05:00:00Z"},
        "collection": {},
    }
    latest_path = tmp_path / "latest.json"
    dated_path = tmp_path / "2026-04-23T05-00-00Z.json"
    calls = []

    monkeypatch.setattr(hourly_builder, "build_hourly_report", lambda: report.copy())
    monkeypatch.setattr(hourly_builder, "write_structured_outputs", lambda *_args, **_kwargs: (latest_path, dated_path))

    def capture_persist(payload):
        calls.append(payload)
        return 123

    monkeypatch.setattr("cron.opik_rollup_persistence.persist_hourly_rollup", capture_persist)

    hourly_builder.main()

    assert len(calls) == 1
    assert calls[0]["collection"]["path_latest"] == str(latest_path)
    assert calls[0]["collection"]["path_stamped"] == str(dated_path)
