from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import cron.opik_daily_rollup_builder as daily_rollup_builder
import cron.opik_rollup_persistence as rollup_persistence


def _top_items(counts: dict[str, Any], limit: int = 5) -> list[tuple[str, int]]:
    items = []
    for key, value in (counts or {}).items():
        try:
            items.append((str(key), int(value)))
        except Exception:
            continue
    return sorted(items, key=lambda kv: (-kv[1], kv[0]))[:limit]


def _top_item(counts: dict[str, Any]) -> tuple[str, int]:
    items = _top_items(counts, limit=1)
    return items[0] if items else ("none", 0)


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except Exception:
        return "0"


def _fmt_float(value: Any, digits: int = 1) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return f"{0:.{digits}f}"


def _render_headline(rollup: dict[str, Any]) -> str:
    counts = rollup.get("counts") or {}
    coverage = rollup.get("coverage") or {}
    llm = (coverage.get("llm_span_coverage") or {}).get("value_pct") or 0
    kept = counts.get("kept_finalized_workload_count") or 0
    escalations = rollup.get("escalation_codes") or []
    memory_status = (rollup.get("memory") or {}).get("status") or "none"

    if escalations:
        return (
            f"The day carried real load with {_fmt_int(kept)} finalized workload traces and {llm:.1f}% LLM span coverage, "
            f"but the main operational flags were {', '.join(escalations[:3])}. Memory activity ended the day at `{memory_status}`."
        )
    return (
        f"The day was mostly stable: {_fmt_int(kept)} finalized workload traces, {llm:.1f}% LLM span coverage, "
        f"and no explicit escalation codes. Memory activity ended the day at `{memory_status}`."
    )


def _render_scorecard(rollup: dict[str, Any]) -> str:
    counts = rollup.get("counts") or {}
    coverage = rollup.get("coverage") or {}
    volume = rollup.get("volume") or {}
    mix = rollup.get("mix") or {}
    concentration = rollup.get("concentration") or {}
    top_provider, top_provider_count = _top_item(mix.get("providers") or {})
    top_platform, top_platform_count = _top_item(mix.get("platforms") or {})
    llm_cov = coverage.get("llm_span_coverage") or {}
    rows = [
        ("raw traces", _fmt_int(counts.get("raw_trace_count") or 0)),
        ("kept finalized workload traces", _fmt_int(counts.get("kept_finalized_workload_count") or 0)),
        ("llm span coverage", f"{_fmt_float(llm_cov.get('value_pct') or 0, 1)}% ({_fmt_int(llm_cov.get('numerator') or 0)}/{_fmt_int(llm_cov.get('denominator') or 0)})"),
        ("measured tokens", _fmt_int(volume.get("tokens_total_measured") or 0)),
        ("average duration", f"{_fmt_float(volume.get('duration_seconds_avg') or 0, 1)}s"),
        ("max duration", f"{_fmt_float(volume.get('duration_seconds_max') or 0, 1)}s"),
        ("true empty dispatches", _fmt_int(counts.get("true_empty_dispatch_count") or 0)),
        ("completed telemetry loss traces", _fmt_int(counts.get("completed_telemetry_loss_count") or 0)),
        ("finalized failed or interrupted traces", _fmt_int(counts.get("completed_failed_or_interrupted_count") or 0)),
        ("nonfinal backlog count", _fmt_int(counts.get("excluded_nonfinal_count") or 0)),
        ("top provider", f"{top_provider} ({_fmt_int(top_provider_count)})"),
        ("top platform", f"{top_platform} ({_fmt_int(top_platform_count)})"),
        ("top session family token share", f"{_fmt_float(concentration.get('largest_session_family_token_share_pct') or 0, 1)}%"),
    ]
    lines = ["| Metric | Value |", "|---|---|"]
    lines.extend(f"| {metric} | {value} |" for metric, value in rows)
    return "\n".join(lines)


def _render_trend_synthesis(rollup: dict[str, Any]) -> str:
    freqs = rollup.get("hourly_alert_frequencies") or {}
    peaks = rollup.get("peaks") or {}
    repeated = []
    if freqs.get("memory_active"):
        repeated.append(f"memory activity showed up in {freqs['memory_active']} hourly windows")
    if freqs.get("provider_shift"):
        repeated.append(f"provider or platform mix shifted in {freqs['provider_shift']} hourly windows")
    if freqs.get("delegated_batches_present"):
        repeated.append(f"delegated work appeared in {freqs['delegated_batches_present']} hourly windows")
    worsened = []
    if freqs.get("backlog_pressure"):
        worsened.append(f"backlog pressure hit {freqs['backlog_pressure']} hourly windows, peaking at {peaks.get('peak_hour_by_backlog_utc') or 'n/a'}")
    if freqs.get("telemetry_loss"):
        worsened.append(f"telemetry loss appeared in {freqs['telemetry_loss']} hourly windows")
    if freqs.get("rate_limit_caveat"):
        worsened.append(f"span rate-limit caveats appeared in {freqs['rate_limit_caveat']} hourly windows")
    improved = [f"peak trace count hour was {peaks.get('peak_hour_by_trace_count_utc') or 'n/a'} and the day still finished with a persisted structured rollup"]
    one_offs = [f"peak token hour was {peaks.get('peak_hour_by_tokens_utc') or 'n/a'}"]

    def bulletize(items: list[str], fallback: str) -> str:
        return "\n".join(f"- {item}" for item in items) if items else f"- {fallback}"

    return "\n".join(
        [
            "### What repeated across multiple hours",
            bulletize(repeated, "No repeated hourly pattern stood out beyond baseline load."),
            "",
            "### What worsened during the day",
            bulletize(worsened, "No worsening pattern crossed the current alert thresholds."),
            "",
            "### What improved during the day",
            bulletize(improved, "No clear late-day recovery signal was captured."),
            "",
            "### One offs",
            bulletize(one_offs, "No meaningful one-off anomaly stood apart from the day-level mix."),
        ]
    )


def _render_workload_concentration(rollup: dict[str, Any]) -> str:
    concentration = rollup.get("concentration") or {}
    families = concentration.get("top_session_families") or []
    outliers = rollup.get("outliers") or {}
    mix = rollup.get("mix") or {}

    family_lines = ["| Session ID | Traces | Tokens | Duration |", "|---|---:|---:|---:|"]
    for family in families[:5]:
        family_lines.append(
            f"| {family.get('session_id') or ''} | {_fmt_int(family.get('trace_count') or 0)} | {_fmt_int(family.get('tokens_total') or 0)} | {_fmt_float(family.get('duration_seconds_total') or 0, 1)}s |"
        )

    outlier_lines = []
    for label, key in [("max tokens", "max_tokens_trace"), ("max duration", "max_duration_trace"), ("max tool calls", "max_tool_calls_trace")]:
        row = outliers.get(key) or {}
        outlier_lines.append(
            f"- {label}: `{row.get('trace_id') or ''}` session `{row.get('session_id') or ''}` on {row.get('platform') or 'unknown'} / {row.get('provider') or 'unknown'} ({row.get('model') or 'unknown'})"
        )

    provider_lines = ", ".join(f"{name}={_fmt_int(count)}" for name, count in _top_items(mix.get("providers") or {}, limit=5)) or "none"
    platform_lines = ", ".join(f"{name}={_fmt_int(count)}" for name, count in _top_items(mix.get("platforms") or {}, limit=5)) or "none"

    return "\n".join(
        [
            "### Top session families",
            *family_lines,
            "",
            "### Top outlier traces",
            *outlier_lines,
            "",
            "### Provider and platform concentration",
            f"- providers: {provider_lines}",
            f"- platforms: {platform_lines}",
            f"- largest session family share: {_fmt_float(concentration.get('largest_session_family_token_share_pct') or 0, 1)}%",
        ]
    )


def _render_tool_hotspots(rollup: dict[str, Any]) -> str:
    hotspots = rollup.get("tool_hotspots") or {}
    opik = hotspots.get("opik_completed_tool_spans") or {}
    local = hotspots.get("local_assistant_tool_calls") or {}
    opik_lines = _top_items(opik.get("counts") or {}, limit=5)
    local_lines = _top_items(local.get("counts") or {}, limit=5)
    lines = ["| Source | Tool | Count | Note |", "|---|---|---:|---|"]
    for tool, count in opik_lines:
        note = "lower bound" if opik.get("is_lower_bound") else "exact within kept span set"
        lines.append(f"| opik completed spans | {tool} | {_fmt_int(count)} | {note} |")
    for tool, count in local_lines:
        note = local.get("lower_bound_reason") or "lower bound"
        lines.append(f"| local assistant tool calls | {tool} | {_fmt_int(count)} | {note} |")
    return "\n".join(lines)


def _render_memory_activity(rollup: dict[str, Any]) -> str:
    memory = rollup.get("memory") or {}
    trace_tools = memory.get("trace_visible_tools") or {}
    local_tools = memory.get("local_tools_lower_bound") or {}
    lifecycle = memory.get("lifecycle_spans_lower_bound") or {}
    return "\n".join(
        [
            f"- memory: {trace_tools.get('memory', 0)}",
            f"- session_search: {trace_tools.get('session_search', 0)}",
            f"- brv_query: {trace_tools.get('brv_query', 0)}",
            f"- brv_curate: {trace_tools.get('brv_curate', 0)}",
            f"- memory:inject: {lifecycle.get('memory_inject', 0)}",
            f"- memory:recall: {lifecycle.get('memory_recall', 0)}",
            f"- memory:sync: {lifecycle.get('memory_sync', 0)}",
            f"- local lower bounds: memory={local_tools.get('memory', 0)}, session_search={local_tools.get('session_search', 0)}, brv_query={local_tools.get('brv_query', 0)}, brv_curate={local_tools.get('brv_curate', 0)}",
            f"- status: `{memory.get('status') or 'none'}`. Visibility is trace-first, with local counts treated as lower bounds when coverage is partial.",
        ]
    )


def _render_delegation(rollup: dict[str, Any]) -> str:
    delegation = rollup.get("delegation") or {}
    lines = [
        f"- proven parent batches: {_fmt_int(delegation.get('proven_parent_batches') or 0)}",
        f"- child sessions: {_fmt_int(delegation.get('child_sessions') or 0)}",
        f"- child traces: {_fmt_int(delegation.get('child_traces') or 0)}",
        f"- finalized child traces: {_fmt_int(delegation.get('finalized_child_traces') or 0)}",
        f"- delegated marker coverage: {_fmt_int(delegation.get('delegated_tagged_child_traces') or 0)} tagged / {_fmt_int(delegation.get('parent_session_id_marked_child_traces') or 0)} with parent_session_id",
        f"- child tokens total: {_fmt_int(delegation.get('child_tokens_total') or 0)}",
        "",
        "| Batch Scope | Parent Batches | Child Sessions | Child Traces | Finalized Child Traces |",
        "|---|---:|---:|---:|---:|",
        f"| all delegated batches | {_fmt_int(delegation.get('proven_parent_batches') or 0)} | {_fmt_int(delegation.get('child_sessions') or 0)} | {_fmt_int(delegation.get('child_traces') or 0)} | {_fmt_int(delegation.get('finalized_child_traces') or 0)} |",
    ]
    return "\n".join(lines)


def _render_escalations(rollup: dict[str, Any]) -> str:
    codes = rollup.get("escalation_codes") or []
    mapping = {
        "backlog_pressure": ("Backlog pressure", "Too much work stayed non-finalized at cutoff, which hides the true system load.", "Reduce overlap or inspect the heaviest session families first."),
        "telemetry_loss": ("Telemetry loss", "Some finalized traces did not retain clean trace-level visibility, which weakens root-cause work.", "Inspect tracer coverage and re-check the affected provider or platform path."),
        "true_empty_dispatch": ("True empty dispatch", "These are real dispatch failures, not just weak observability.", "Check the failing provider path and gateway logs around the empty trace timestamps."),
        "rate_limit_caveat": ("Span rate-limit caveat", "Span-limited slices turn tool and lifecycle counts into lower bounds.", "Reduce per-run span crawl pressure or cache the wider-window span pulls."),
        "delegated_batches_present": ("Delegated workload present", "Subagent work changes the real cost footprint and can hide inside parent summaries.", "Review child trace coverage and parent/child lineage on the heaviest batch."),
    }
    if not codes:
        return "No escalations."
    lines = []
    for idx, code in enumerate(codes, start=1):
        issue, why, action = mapping.get(code, (code, "This surfaced in the machine rollup.", "Inspect the structured daily rollup and tighten the failing path."))
        lines.append(f"{idx}. Issue: {issue}\n   - Why it matters: {why}\n   - Next action: {action}")
    return "\n".join(lines)


def _render_actions_for_tomorrow(rollup: dict[str, Any]) -> str:
    actions = []
    codes = rollup.get("escalation_codes") or []
    if "backlog_pressure" in codes:
        actions.append("Trim or split the heaviest session family before the next hourly cutoff.")
    if "telemetry_loss" in codes:
        actions.append("Re-check the trace or span path that lost telemetry and compare it to a clean provider session.")
    if "true_empty_dispatch" in codes:
        actions.append("Pull gateway/provider logs for the empty dispatch window and isolate the failing request path.")
    if not actions:
        actions.append("Keep the daily rollup builder on the critical path and watch for any new escalation codes.")
    if len(actions) < 3:
        actions.append("Review the top outlier trace and top tool hotspot together to see whether one tool is driving both cost and duration.")
    if len(actions) < 3:
        actions.append("Check whether local-session coverage improved or regressed before trusting lower-bound tool and memory counts.")
    return "\n".join(f"{idx}. {action}" for idx, action in enumerate(actions[:3], start=1))


def render_daily_digest(rollup: dict[str, Any]) -> str:
    appendix = json.dumps(rollup, indent=2)
    parts = [
        "# Hermes Opik Daily Digest",
        "",
        "## Headline",
        _render_headline(rollup),
        "",
        "## 24h Scorecard",
        _render_scorecard(rollup),
        "",
        "## Trend Synthesis",
        _render_trend_synthesis(rollup),
        "",
        "## Workload Concentration",
        _render_workload_concentration(rollup),
        "",
        "## Tool Hot Spots",
        _render_tool_hotspots(rollup),
        "",
        "## Memory Activity",
        _render_memory_activity(rollup),
        "",
        "## Delegated / Subagent Workloads",
        _render_delegation(rollup),
        "",
        "## Escalations / Required Fixes",
        _render_escalations(rollup),
        "",
        "## Actions for Tomorrow",
        _render_actions_for_tomorrow(rollup),
        "",
        "## Appendix: Structured Daily Rollup",
        "```json",
        appendix,
        "```",
    ]
    return "\n".join(parts)


def main() -> None:
    rollup = daily_rollup_builder.build_daily_rollup()
    report_date = str((rollup.get("window") or {}).get("to_utc") or "")[:10]
    latest, dated = daily_rollup_builder.write_structured_outputs(rollup, daily_rollup_builder.OUT_DIR, report_date)
    rollup.setdefault("collection", {})["path_latest"] = str(latest)
    rollup.setdefault("collection", {})["path_stamped"] = str(dated)
    rollup_persistence.persist_daily_rollup(rollup)
    payload = json.dumps(rollup, indent=2)
    latest.write_text(payload)
    dated.write_text(payload)
    print(render_daily_digest(rollup))


if __name__ == "__main__":
    main()
