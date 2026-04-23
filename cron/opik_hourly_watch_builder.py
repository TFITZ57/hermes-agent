from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

HERMES_PROJECT = "hermes-agent"
HERMES_HOME = Path.home() / ".hermes"
STATE_DB_PATH = HERMES_HOME / "state.db"
OUT_DIR = HERMES_HOME / "cron" / "output" / "ce223a54c764" / "structured"
SELF_PREFIXES = ("cron_ce223a54c764_", "cron_d16b5967a31f_")
TRACE_PAGE_SIZE = 500
SPAN_PAGE_SIZE = 500
MEMORY_TOOL_NAMES = ("memory", "session_search", "brv_query", "brv_curate")
MEMORY_LIFECYCLE_NAMES = ("memory:inject", "memory:recall", "memory:sync")


def to_aware_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        if "." in s:
            base, tail = s.split(".", 1)
            tz_match = re.search(r"([+-]\d\d:\d\d)$", tail)
            if tz_match:
                frac = tail[: tz_match.start()]
                tz = tz_match.group(1)
                s = f"{base}.{(frac + '000000')[:6]}{tz}"
            else:
                s = f"{base}.{(tail + '000000')[:6]}"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", s)
            if m:
                try:
                    return datetime.fromisoformat(m.group(1) + "+00:00")
                except Exception:
                    return None
    return None


def normalize_rows(page: Any) -> list[Any]:
    if page is None:
        return []
    if hasattr(page, "model_dump"):
        raw = page.model_dump()
    elif hasattr(page, "dict"):
        raw = page.dict()
    elif hasattr(page, "to_dict"):
        raw = page.to_dict()
    else:
        raw = dict(page) if hasattr(page, "items") else {}
    rows = raw.get("content")
    if rows is None:
        rows = getattr(page, "content", None)
    if rows is None and hasattr(page, "data"):
        rows = getattr(page, "data")
    if rows is None:
        try:
            rows = list(page)
        except Exception:
            rows = []
    return list(rows or [])


def normalize_item(item: Any) -> dict[str, Any]:
    if item is None:
        return {}
    if hasattr(item, "model_dump"):
        return item.model_dump()
    if hasattr(item, "dict"):
        return item.dict()
    if hasattr(item, "to_dict"):
        return item.to_dict()
    if isinstance(item, dict):
        return item
    try:
        return dict(item)
    except Exception:
        return {}


def get_nested(d: Any, *keys: str) -> Any:
    cur = d
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur


def norm_strs(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, str):
        return [val] if val.strip() else []
    if isinstance(val, (list, tuple, set)):
        return [str(x) for x in val if x is not None]
    return [str(val)]


def parse_payload(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        p = payload.strip()
        if not p:
            return {}
        try:
            v = json.loads(p)
        except Exception:
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, list):
            return {"_": v}
        return {"value": v}
    return {}


def parse_tool_calls(raw: Any) -> list[str]:
    if not raw:
        return []
    arr: list[Any]
    if isinstance(raw, list):
        arr = raw
    else:
        obj = raw
        if isinstance(raw, str):
            try:
                obj = json.loads(raw.strip())
            except Exception:
                return []
        if isinstance(obj, dict):
            maybe = obj.get("tool_calls") or obj.get("toolCalls") or []
            arr = maybe if isinstance(maybe, list) else [maybe] if isinstance(maybe, dict) else []
        else:
            return []
    calls: list[str] = []
    for it in arr:
        if isinstance(it, str):
            calls.append(it)
            continue
        if not isinstance(it, dict):
            continue
        name = it.get("name")
        if name:
            calls.append(str(name))
            continue
        fn = it.get("function")
        if isinstance(fn, dict) and fn.get("name"):
            calls.append(str(fn["name"]))
    return calls


def parse_duration(trace_row: dict[str, Any]) -> float | None:
    out = parse_payload(get_nested(trace_row, "output"))
    candidates = [
        get_nested(out, "duration_seconds"),
        get_nested(trace_row, "duration"),
        get_nested(trace_row, "duration_seconds"),
    ]
    for c in candidates:
        if c is None:
            continue
        try:
            dur = float(c)
        except Exception:
            continue
        if dur > 100000:
            dur = dur / 1000.0
        if dur >= 0:
            return dur
    return None


def parse_int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return 0


def parse_num(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        try:
            return float(int(v))
        except Exception:
            return 0.0


def infer_provider_from_tags(tags: Sequence[str]) -> list[str]:
    known = {"openai", "anthropic", "openai-codex", "google", "gemini", "cohere", "openrouter"}
    return [t for t in tags if t in known]


def trace_meta_name(trace_row: dict[str, Any]) -> str:
    meta = parse_payload(get_nested(trace_row, "metadata"))
    return str(get_nested(meta, "model") or get_nested(meta, "response_model") or "")


def normalize_input(trace_row: dict[str, Any]) -> dict[str, Any]:
    return parse_payload(get_nested(trace_row, "input"))


def trace_start_time(trace_row: dict[str, Any]) -> datetime | None:
    for key in ("start_time", "startTime", "created_at", "createdAt"):
        dt = to_aware_datetime(get_nested(trace_row, key))
        if dt is not None:
            return dt
    inp = normalize_input(trace_row)
    for key in ("start_time", "startTime"):
        dt = to_aware_datetime(get_nested(inp, key))
        if dt is not None:
            return dt
    return None


def trace_end_time(trace_row: dict[str, Any]) -> datetime | None:
    for key in ("end_time", "endTime"):
        dt = to_aware_datetime(get_nested(trace_row, key))
        if dt is not None:
            return dt
    out = parse_payload(get_nested(trace_row, "output"))
    for key in ("end_time", "endTime"):
        dt = to_aware_datetime(get_nested(out, key))
        if dt is not None:
            return dt
    return None


def trace_tokens(trace_row: dict[str, Any]) -> tuple[float, str]:
    usage = parse_payload(get_nested(trace_row, "usage"))
    meta = parse_payload(get_nested(trace_row, "metadata"))
    out = parse_payload(get_nested(trace_row, "output"))
    tok = parse_num(get_nested(usage, "total_tokens"))
    if tok > 0:
        return tok, "usage"
    tok = parse_num(get_nested(usage, "prompt_tokens")) + parse_num(get_nested(usage, "completion_tokens"))
    if tok > 0:
        return tok, "usage"
    tok_meta = parse_num(get_nested(meta, "total_tokens"))
    if tok_meta > 0:
        return tok_meta, "metadata"
    tok_out = parse_num(get_nested(out, "total_tokens"))
    if tok_out > 0:
        return tok_out, "output"
    return 0.0, "none"


def trace_metrics(trace_row: dict[str, Any]) -> dict[str, Any]:
    trace_id = get_nested(trace_row, "id") or get_nested(trace_row, "trace_id") or ""
    name = get_nested(trace_row, "name") or ""
    inp = normalize_input(trace_row)
    session_id = get_nested(inp, "session_id") or get_nested(inp, "threadId") or get_nested(inp, "thread_id") or get_nested(trace_row, "threadId") or get_nested(trace_row, "thread_id")
    platform = get_nested(inp, "platform") or ""
    meta = parse_payload(get_nested(trace_row, "metadata"))
    if not platform:
        platform = get_nested(meta, "platform") or ""
    providers: list[str] = []
    for p in norm_strs(get_nested(trace_row, "providers")) + norm_strs(get_nested(meta, "providers")):
        if p not in providers:
            providers.append(p)
    for p in infer_provider_from_tags(norm_strs(get_nested(trace_row, "tags"))):
        if p not in providers:
            providers.append(p)
    model = trace_meta_name(trace_row) or str(get_nested(meta, "model") or get_nested(meta, "response_model") or get_nested(trace_row, "model") or get_nested(trace_row, "response_model") or "")
    duration = parse_duration(trace_row)
    start_ts = trace_start_time(trace_row)
    end_ts = trace_end_time(trace_row)
    tokens, token_src = trace_tokens(trace_row)
    out = parse_payload(get_nested(trace_row, "output"))
    usage = parse_payload(get_nested(trace_row, "usage"))
    api_calls = parse_int(get_nested(out, "api_calls")) or parse_int(get_nested(trace_row, "api_calls"))
    tool_calls = parse_int(get_nested(out, "tool_calls")) or parse_int(get_nested(trace_row, "tool_calls"))
    llm_span_count = parse_int(get_nested(trace_row, "llm_span_count")) or parse_int(get_nested(trace_row, "llmSpanCount"))
    span_count = parse_int(get_nested(trace_row, "span_count"))
    has_tool_spans = bool(get_nested(trace_row, "has_tool_spans") or parse_int(get_nested(trace_row, "hasToolSpans")))
    tags = norm_strs(get_nested(trace_row, "tags"))
    return {
        "trace_id": trace_id,
        "name": name,
        "session_id": session_id,
        "platform": platform or "unknown",
        "providers": providers,
        "tags": tags,
        "model": model or "unknown",
        "start_time": start_ts,
        "end_time": end_ts,
        "duration_seconds": duration,
        "tokens": tokens,
        "tokens_source": token_src,
        "api_calls": api_calls,
        "tool_calls": tool_calls,
        "llm_span_count": llm_span_count,
        "span_count": span_count,
        "has_tool_spans": has_tool_spans,
        "usage": usage,
        "metadata": meta,
        "output": out,
    }


def trace_is_self(trace: dict[str, Any]) -> bool:
    sid = str(trace.get("session_id") or "")
    return sid.startswith(SELF_PREFIXES)


def workload_name(name: str) -> bool:
    return not name or name == "hermes-session" or str(name).startswith("hermes-session")


def finalization_status(trace: dict[str, Any]) -> bool:
    end_ts = trace.get("end_time")
    duration = trace.get("duration_seconds")
    return bool(end_ts is not None and duration is not None and duration > 0)


def normalize_tool_span_name(name: str, metadata: dict[str, Any] | None = None) -> str:
    metadata = metadata or {}
    meta_tool_name = metadata.get("tool_name")
    if meta_tool_name:
        return str(meta_tool_name)
    raw = str(name or "")
    if raw.startswith("tool:"):
        return raw.split(":", 1)[1]
    return raw


def summarize_memory_spans(spans: Iterable[dict[str, Any]], kept_trace_ids: set[str]) -> tuple[dict[str, int], dict[str, int]]:
    explicit_counts: Counter[str] = Counter()
    lifecycle_counts: Counter[str] = Counter()
    for row in spans:
        span = normalize_item(row)
        trace_id = str(span.get("trace_id") or span.get("traceId") or "")
        if trace_id not in kept_trace_ids:
            continue
        name = str(span.get("name") or "")
        lname = name.lower()
        if lname in MEMORY_LIFECYCLE_NAMES:
            lifecycle_counts[lname] += 1
            continue
        stype = str(span.get("type") or span.get("span_type") or "").lower()
        if stype != "tool":
            continue
        end_t = to_aware_datetime(span.get("end_time") or span.get("endTime") or get_nested(span, "output", "end_time") or get_nested(span, "output", "endTime"))
        output = parse_payload(span.get("output"))
        if end_t is None and not output:
            continue
        tool_name = normalize_tool_span_name(name, parse_payload(span.get("metadata")))
        if tool_name in MEMORY_TOOL_NAMES:
            explicit_counts[tool_name] += 1
    return {
        "memory": int(explicit_counts.get("memory", 0)),
        "session_search": int(explicit_counts.get("session_search", 0)),
        "brv_query": int(explicit_counts.get("brv_query", 0)),
        "brv_curate": int(explicit_counts.get("brv_curate", 0)),
    }, {
        "memory:inject": int(lifecycle_counts.get("memory:inject", 0)),
        "memory:recall": int(lifecycle_counts.get("memory:recall", 0)),
        "memory:sync": int(lifecycle_counts.get("memory:sync", 0)),
    }


def build_memory_tool_span_visibility(trace_visible_tools: dict[str, int], local_tools_lower_bound: dict[str, int]) -> dict[str, Any]:
    numerator = int(sum(trace_visible_tools.values()))
    local_den = int(sum(local_tools_lower_bound.values()))
    denominator = local_den if local_den > 0 else numerator
    value_pct = round((numerator / denominator * 100.0), 3) if local_den > 0 and denominator > 0 else 0.0
    return {
        "value_pct": value_pct,
        "numerator": numerator,
        "denominator": denominator,
    }


def load_prior_reports(output_dir: Path) -> dict[str, Any]:
    prior = {"path": "", "found": False, "data": None, "mtime": None}
    if output_dir.exists():
        files = sorted(output_dir.glob("*.json"))
        if files:
            path = files[-1]
            prior.update({"path": str(path), "found": True, "mtime": path.stat().st_mtime})
            try:
                prior["data"] = json.loads(path.read_text())
            except Exception:
                prior["data"] = None
            return prior
    md_dir = HERMES_HOME / "cron" / "output" / "ce223a54c764"
    if not md_dir.exists():
        return prior
    files = sorted(md_dir.glob("*.md"))
    if not files:
        return prior
    path = files[-1]
    prior.update({"path": str(path), "found": True, "mtime": path.stat().st_mtime})
    try:
        text = path.read_text()
        idx = text.rfind("## Response")
        if idx != -1:
            body = text[idx + len("## Response"):].strip()
            try:
                prior["data"] = json.loads(body.split("\n\n", 1)[0].strip())
            except Exception:
                prior["data"] = None
    except Exception:
        prior["data"] = None
    return prior


def compare_prior(current_report: dict[str, Any], prior: dict[str, Any]) -> bool:
    if not prior.get("found") or not prior.get("data"):
        return True
    previous = prior["data"]
    c1 = current_report.get("counts", {})
    c2 = previous.get("counts", {}) if isinstance(previous, dict) else {}
    for key in ("raw_trace_count", "kept_finalized_workload_count", "excluded_nonfinal_count"):
        if c1.get(key) != c2.get(key):
            return True
    return False


def fetch_trace_rows(client: Any, from_dt: datetime, to_dt: datetime) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    page_num = 1
    total_expected = None
    while True:
        page = client.rest_client.traces.get_traces_by_project(
            project_name=HERMES_PROJECT,
            from_time=from_dt,
            to_time=to_dt,
            page=page_num,
            size=TRACE_PAGE_SIZE,
        )
        rows = [normalize_item(r) for r in normalize_rows(page)]
        all_rows.extend(rows)
        if total_expected is None:
            total_expected = getattr(page, "total", None)
            if total_expected is None and isinstance(page, dict):
                total_expected = page.get("total")
        if not rows:
            break
        if total_expected is not None and len(all_rows) >= int(total_expected):
            break
        if len(rows) < TRACE_PAGE_SIZE:
            break
        page_num += 1
    return all_rows


def fetch_span_rows(client: Any, from_dt: datetime, to_dt: datetime) -> tuple[list[dict[str, Any]], bool, bool]:
    span_crawl_attempted = True
    span_crawl_rate_limited = False
    rows_out: list[dict[str, Any]] = []
    try:
        page_num = 1
        while True:
            page = client.rest_client.spans.get_spans_by_project(
                project_name=HERMES_PROJECT,
                from_time=from_dt,
                to_time=to_dt,
                page=page_num,
                size=SPAN_PAGE_SIZE,
            )
            rows = [normalize_item(s) for s in normalize_rows(page)]
            if not rows:
                break
            rows_out.extend(rows)
            if len(rows) < SPAN_PAGE_SIZE:
                break
            page_num += 1
    except Exception as e:
        if "429" in str(e) or "Too Many Requests" in str(e):
            span_crawl_rate_limited = True
        else:
            span_crawl_attempted = False
    return rows_out, span_crawl_attempted, span_crawl_rate_limited


def load_local_session_rows(session_ids: set[str], state_db_path: Path) -> tuple[dict[str, dict[str, Any]], Counter[str], Counter[str]]:
    local_session_rows: dict[str, dict[str, Any]] = {}
    local_tools_counts: Counter[str] = Counter()
    local_memory_counts: Counter[str] = Counter()
    if not state_db_path.exists() or not session_ids:
        return local_session_rows, local_tools_counts, local_memory_counts
    conn = sqlite3.connect(str(state_db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    ph = ",".join(["?"] * len(session_ids))
    try:
        cur.execute(f"SELECT id, started_at, ended_at, parent_session_id FROM sessions WHERE id IN ({ph})", tuple(session_ids))
        local_session_rows = {r["id"]: dict(r) for r in cur.fetchall()}
        if local_session_rows:
            ph2 = ",".join(["?"] * len(local_session_rows))
            cur.execute(f"SELECT session_id, tool_calls FROM messages WHERE session_id IN ({ph2})", tuple(local_session_rows.keys()))
            for r in cur.fetchall():
                sid = r["session_id"]
                if sid not in local_session_rows:
                    continue
                for name in parse_tool_calls(r["tool_calls"]):
                    if not name:
                        continue
                    local_tools_counts[name] += 1
                    if name in MEMORY_TOOL_NAMES:
                        local_memory_counts[name] += 1
    finally:
        conn.close()
    return local_session_rows, local_tools_counts, local_memory_counts


def detect_delegated_batches(from_dt: datetime, to_dt: datetime, state_db_path: Path, workload_candidates: list[dict[str, Any]], kept_finalized: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not state_db_path.exists():
        return []
    conn = sqlite3.connect(str(state_db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    from_ts = from_dt.timestamp()
    to_ts = to_dt.timestamp()
    proven_parent_sessions: set[str] = set()
    try:
        cur.execute("SELECT session_id, tool_calls FROM messages WHERE timestamp >= ? AND timestamp <= ? AND tool_calls IS NOT NULL", (from_ts, to_ts))
        for row in cur.fetchall():
            sid = row["session_id"]
            if sid and any(n == "delegate_task" for n in parse_tool_calls(row["tool_calls"])):
                proven_parent_sessions.add(sid)
        parent_batches: list[dict[str, Any]] = []
        for parent_id in sorted(proven_parent_sessions):
            cur.execute("SELECT id, started_at FROM sessions WHERE parent_session_id = ?", (parent_id,))
            child_rows = cur.fetchall()
            child_sessions: list[str] = []
            for child in child_rows:
                started_at = child["started_at"] if "started_at" in child.keys() else None
                if started_at is not None and from_ts <= started_at <= to_ts:
                    child_sessions.append(child["id"])
            parent_trace_ids = [t["trace_id"] for t in workload_candidates if t["session_id"] == parent_id]
            child_trace_rows = [t for t in workload_candidates if t["session_id"] in child_sessions]
            child_finalized_rows = [t for t in kept_finalized if t["session_id"] in child_sessions]
            if child_sessions or child_trace_rows:
                parent_batches.append({
                    "parent_session_id": parent_id,
                    "child_sessions": child_sessions,
                    "child_traces": child_trace_rows,
                    "child_finalized": child_finalized_rows,
                    "parent_trace_ids": parent_trace_ids,
                })
        return parent_batches
    finally:
        conn.close()


def write_structured_outputs(report: dict[str, Any], output_dir: Path, to_utc: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    latest = output_dir / "latest.json"
    dated = output_dir / f"{to_utc.replace(':', '-')}.json"
    payload = json.dumps(report, indent=2)
    latest.write_text(payload)
    dated.write_text(payload)
    return latest, dated


def build_hourly_report(now: datetime | None = None, client: Any | None = None, state_db_path: Path = STATE_DB_PATH, output_dir: Path = OUT_DIR) -> dict[str, Any]:
    if now is None:
        now = datetime.now(timezone.utc)
    if client is None:
        from opik import Opik
        client = Opik()
    to_dt = now
    from_dt = now - timedelta(minutes=90)

    project_id = ""
    try:
        projects = client.rest_client.projects.get_projects(page=1, size=100)
        for p in normalize_rows(projects):
            d = normalize_item(p)
            if get_nested(d, "name") == HERMES_PROJECT:
                project_id = get_nested(d, "id") or ""
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
        for p in providers:
            providers_counts[p] += 1
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
    for row in span_rows:
        span = normalize_item(row)
        trace_id = str(span.get("trace_id") or span.get("traceId") or "")
        if trace_id not in kept_trace_ids:
            continue
        stype = str(span.get("type") or span.get("span_type") or "").lower()
        if stype != "tool":
            continue
        end_t = to_aware_datetime(span.get("end_time") or span.get("endTime") or get_nested(span, "output", "end_time") or get_nested(span, "output", "endTime"))
        output = parse_payload(span.get("output"))
        if end_t is None and not output:
            continue
        tool_name = normalize_tool_span_name(str(span.get("name") or ""), parse_payload(span.get("metadata")))
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
    top_session_families = [{"session_id": sid, "trace_count": int(v["trace_count"]), "tokens_total": float(v["token_total"]), "duration_seconds_total": float(v["duration_total"])} for sid, v in top_families[:5]]
    largest_family_share = (top_families[0][1]["token_total"] / workload_tokens * 100.0) if top_families and workload_tokens > 0 else 0.0

    parent_batches = detect_delegated_batches(from_dt, to_dt, state_db_path, workload_candidates, kept_finalized)
    delegated_tagged_child = 0
    parent_session_tagged_child = 0
    for batch in parent_batches:
        for tr in batch["child_traces"]:
            if "delegated" in tr["tags"]:
                delegated_tagged_child += 1
            meta_parent = tr["metadata"].get("parent_session_id") if isinstance(tr["metadata"], dict) else None
            if meta_parent and str(meta_parent) == str(batch["parent_session_id"]):
                parent_session_tagged_child += 1
    largest_batch = {"parent_session_id": "", "parent_trace_ids": [], "child_trace_count": 0, "child_tokens_total": 0}
    if parent_batches:
        best = sorted(parent_batches, key=lambda b: (len(b["child_traces"]), sum(t["tokens"] for t in b["child_traces"])), reverse=True)[0]
        largest_batch = {
            "parent_session_id": best["parent_session_id"],
            "parent_trace_ids": sorted(set(best["parent_trace_ids"])),
            "child_trace_count": len(best["child_traces"]),
            "child_tokens_total": float(sum(t["tokens"] for t in best["child_traces"])),
        }

    alerts: list[dict[str, Any]] = []
    load_backlog_share_num = len(excluded_nonfinal)
    load_backlog_share_denom = raw_trace_count
    load_backlog_share_pct = (load_backlog_share_num / load_backlog_share_denom * 100.0) if load_backlog_share_denom else 0.0
    if load_backlog_share_pct > 30.0:
        alerts.append({"code": "backlog_pressure", "severity": "warn", "metric": "nonfinal_backlog_share_pct", "value": round(load_backlog_share_pct, 3), "threshold": 30.0, "why": "A large share of in-window traces are non-finalized at cutoff."})
    if completed_telemetry_loss_count > 0:
        ratio = (completed_telemetry_loss_count / len(kept_finalized) * 100.0) if kept_finalized else 0.0
        if ratio >= 5.0:
            alerts.append({"code": "telemetry_loss", "severity": "warn", "metric": "completed_telemetry_loss_rate", "value": round(ratio, 3), "threshold": 5.0, "why": "Finalized traces with low or no telemetry are above 5% of finalized workload."})
    if true_empty_dispatch_count > 0:
        alerts.append({"code": "true_empty_dispatch", "severity": "error", "metric": "true_empty_dispatch_count", "value": int(true_empty_dispatch_count), "threshold": 0, "why": "Traces with zero endTime or duration and no meaningful output indicate dispatch failures."})
    if largest_family_share >= 70.0 and top_families:
        alerts.append({"code": "session_concentration", "severity": "warn", "metric": "top_session_family_token_share_pct", "value": round(largest_family_share, 3), "threshold": 70.0, "why": "One session family dominates most workload tokens in this window."})
    if len(local_session_rows) < len(kept_identifiable_sessions):
        ratio = (len(local_session_rows) / len(kept_identifiable_sessions) * 100.0) if kept_identifiable_sessions else 100.0
        if ratio < 80.0:
            alerts.append({"code": "delegated_lineage_gap", "severity": "info", "metric": "local_session_coverage_pct", "value": round(ratio, 3), "threshold": 80.0, "why": "Only partial final-session visibility in local state DB."})
    if not span_crawl_attempted or span_crawl_rate_limited:
        alerts.append({"code": "span_crawl_rate_limiting", "severity": "warn" if span_crawl_rate_limited else "info", "metric": "span_crawl_attempted", "value": 1, "threshold": 0, "why": "Span crawl was partial or rate limited."})

    prior = load_prior_reports(output_dir)
    llm_pct = (llm_num / len(kept_finalized) * 100.0) if kept_finalized else 0.0
    local_den = len(kept_identifiable_sessions)
    local_cov = {"value_pct": round((len(local_session_rows) / local_den * 100.0), 3) if local_den else 0.0, "numerator": int(len(local_session_rows)), "denominator": int(local_den)}
    memory_visibility = build_memory_tool_span_visibility(trace_visible_memory, {k: int(local_memory_counts.get(k, 0)) for k in MEMORY_TOOL_NAMES})
    mem_status = "none"
    total_mem = sum(local_memory_counts.values()) + sum(trace_visible_memory.values())
    if total_mem > 0:
        mem_status = "active" if sum(trace_visible_memory.values()) > 0 else "low"

    report: dict[str, Any] = {
        "schema_version": 1,
        "report_type": "opik_hourly_watch",
        "generated_at_utc": now.isoformat().replace("+00:00", "Z"),
        "window": {
            "project_name": HERMES_PROJECT,
            "project_id": project_id,
            "from_utc": from_dt.isoformat().replace("+00:00", "Z"),
            "to_utc": to_dt.isoformat().replace("+00:00", "Z"),
            "duration_minutes": 90,
            "frozen_cutoff": True,
        },
        "collection": {
            "trace_pull_method": "opik_python_sdk",
            "mcp_baseline_checked": False,
            "sdk_paged": True,
            "trace_page_size": TRACE_PAGE_SIZE,
            "span_crawl_attempted": bool(span_crawl_attempted),
            "span_crawl_rate_limited": bool(span_crawl_rate_limited),
            "prior_report_path": prior["path"],
            "prior_report_found": bool(prior["found"]),
            "materially_changed_vs_prior": compare_prior({"counts": {"raw_trace_count": raw_trace_count, "kept_finalized_workload_count": len(kept_finalized), "excluded_nonfinal_count": len(excluded_nonfinal)}}, prior),
        },
        "counts": {
            "raw_trace_count": int(raw_trace_count),
            "workload_trace_count": int(len(workload_candidates)),
            "kept_finalized_workload_count": int(len(kept_finalized)),
            "excluded_self_count": int(excluded_self_count),
            "excluded_nonfinal_count": int(len(excluded_nonfinal)),
            "excluded_diagnostic_count": int(excluded_diagnostic_count),
            "completed_telemetry_loss_count": int(completed_telemetry_loss_count),
            "true_empty_dispatch_count": int(true_empty_dispatch_count),
            "completed_with_telemetry_count": int(completed_with_telemetry_count),
            "completed_failed_or_interrupted_count": int(completed_failed_or_interrupted_count),
        },
        "coverage": {
            "llm_span_coverage": {"value_pct": round(llm_pct, 3), "numerator": int(llm_num), "denominator": int(len(kept_finalized))},
            "local_session_coverage": local_cov,
            "memory_tool_span_visibility": memory_visibility,
        },
        "volume": {
            "tokens_total_measured": int(workload_tokens),
            "tokens_usage_total": int(sum(parse_num(get_nested(tr["usage"], "total_tokens")) for tr in kept_finalized)),
            "tokens_fallback_total": int(sum(tr["tokens"] for tr in kept_finalized if tr["tokens_source"] != "usage")),
            "duration_seconds_total": float(workload_duration),
            "duration_seconds_avg": float(workload_duration / len(kept_finalized)) if kept_finalized else 0.0,
            "duration_seconds_max": float(workload_duration_max),
            "api_calls_total": int(api_calls_total),
            "api_calls_avg": float(api_calls_total / len(kept_finalized)) if kept_finalized else 0.0,
            "tool_calls_total": int(tool_calls_total),
            "tool_calls_avg": float(tool_calls_total / len(kept_finalized)) if kept_finalized else 0.0,
        },
        "mix": {"providers": dict(providers_counts), "platforms": dict(platform_counts), "models": dict(model_counts)},
        "load": {
            "nonfinal_backlog_share": {"value_pct": round(load_backlog_share_pct, 3), "numerator": int(load_backlog_share_num), "denominator": int(load_backlog_share_denom)},
            "largest_session_family_token_share_pct": float(round(largest_family_share, 3)),
            "top_session_families": top_session_families,
        },
        "tool_hotspots": {
            "opik_completed_tool_spans": {"is_lower_bound": bool(span_crawl_rate_limited or not span_crawl_attempted), "lower_bound_reason": "span crawl partial or unavailable" if (span_crawl_rate_limited or not span_crawl_attempted) else "", "counts": dict(span_tool_counts)},
            "local_assistant_tool_calls": {"is_lower_bound": True, "lower_bound_reason": "local matched sessions only", "counts": dict(local_tools_counts)},
        },
        "memory": {
            "status": mem_status,
            "trace_visible_tools": trace_visible_memory,
            "local_tools_lower_bound": {k: int(local_memory_counts.get(k, 0)) for k in MEMORY_TOOL_NAMES},
            "lifecycle_spans_lower_bound": {"memory_inject": int(lifecycle_counts["memory:inject"]), "memory_recall": int(lifecycle_counts["memory:recall"]), "memory_sync": int(lifecycle_counts["memory:sync"])},
        },
        "delegation": {
            "proven_parent_batches": int(len(parent_batches)),
            "child_sessions": int(sum(len(b["child_sessions"]) for b in parent_batches)),
            "child_traces": int(sum(len(b["child_traces"]) for b in parent_batches)),
            "finalized_child_traces": int(sum(len(b["child_finalized"]) for b in parent_batches)),
            "delegated_tagged_child_traces": int(delegated_tagged_child),
            "parent_session_id_marked_child_traces": int(parent_session_tagged_child),
            "child_tokens_total": int(sum(sum(t["tokens"] for t in b["child_traces"]) for b in parent_batches)),
            "child_api_calls_total": int(sum(sum(t["api_calls"] for t in b["child_traces"]) for b in parent_batches)),
            "child_tool_calls_total": int(sum(sum(t["tool_calls"] for t in b["child_traces"]) for b in parent_batches)),
            "largest_batch": largest_batch,
        },
        "outliers": {"max_tokens_trace": max_tokens_trace, "max_duration_trace": max_duration_trace, "max_tool_calls_trace": max_tool_calls_trace},
        "alerts": alerts,
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
    from cron.opik_rollup_persistence import persist_hourly_rollup

    report = build_hourly_report()
    latest, dated = write_structured_outputs(report, OUT_DIR, report["window"]["to_utc"])
    report.setdefault("collection", {})["path_latest"] = str(latest)
    report.setdefault("collection", {})["path_stamped"] = str(dated)
    persist_hourly_rollup(report)
    payload = json.dumps(report, indent=2)
    latest.write_text(payload)
    dated.write_text(payload)
    print(payload)


if __name__ == "__main__":
    main()