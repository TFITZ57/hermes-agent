from __future__ import annotations

from types import SimpleNamespace

from agent.opik_tracer_compat import patch_external_opik_tracer


class _FakeTrace:
    def __init__(self, trace_id: str):
        self.id = trace_id
        self.updates: list[dict] = []

    def update(self, **kwargs):
        self.updates.append(kwargs)


class _FakeSpan:
    def __init__(self, span_id: str):
        self.id = span_id
        self.updates: list[dict] = []

    def update(self, **kwargs):
        self.updates.append(kwargs)


def _build_fake_opik_module():
    module = SimpleNamespace()
    module._traces = {}
    module._trace_starts = {}
    module._api_call_spans = {}
    module._tool_spans = {}
    module._tool_span_aliases = {}
    module._calls = []

    def _on_session_start(*, session_id: str, model: str = "", platform: str = "", **kwargs):
        module._calls.append(("on_session_start", session_id, kwargs))
        module._traces[session_id] = _FakeTrace(f"trace-{session_id}")
        module._trace_starts[session_id] = 1.0

    def _pre_api_request(*, session_id: str = "", **kwargs):
        module._calls.append(("pre_api_request", session_id, kwargs))
        if session_id in module._traces:
            module._api_call_spans[session_id] = _FakeSpan(f"api-{session_id}")

    def _pre_tool_call(*, session_id: str = "", tool_name: str = "", tool_call_id: str = "", **kwargs):
        module._calls.append(("pre_tool_call", session_id, kwargs))
        if session_id in module._traces:
            span_key = f"{session_id}:{tool_call_id}" if tool_call_id else f"{session_id}:{tool_name}"
            module._tool_spans[span_key] = _FakeSpan(f"tool-{span_key}")

    def _pre_llm_call(**kwargs):
        module._calls.append(("pre_llm_call", kwargs.get("session_id", ""), kwargs))

    def _post_llm_call(**kwargs):
        module._calls.append(("post_llm_call", kwargs.get("session_id", ""), kwargs))

    def _post_tool_call(**kwargs):
        module._calls.append(("post_tool_call", kwargs.get("session_id", ""), kwargs))

    def _post_api_request(**kwargs):
        module._calls.append(("post_api_request", kwargs.get("session_id", ""), kwargs))

    def _on_session_end(**kwargs):
        module._calls.append(("on_session_end", kwargs.get("session_id", ""), kwargs))

    module._on_session_start = _on_session_start
    module._pre_api_request = _pre_api_request
    module._pre_tool_call = _pre_tool_call
    module._pre_llm_call = _pre_llm_call
    module._post_llm_call = _post_llm_call
    module._post_tool_call = _post_tool_call
    module._post_api_request = _post_api_request
    module._on_session_end = _on_session_end
    return module


def test_pre_api_request_rebootstraps_missing_trace_and_adds_lineage_metadata():
    module = patch_external_opik_tracer(_build_fake_opik_module())

    module._pre_api_request(
        session_id="child-session",
        model="test/model",
        platform="cli",
        parent_session_id="parent-session",
        delegate_depth=1,
        continued_session=True,
    )

    assert module._calls[0][0] == "on_session_start"
    trace = module._traces["child-session"]
    span = module._api_call_spans["child-session"]

    assert trace.updates[-1]["metadata"]["parent_session_id"] == "parent-session"
    assert trace.updates[-1]["metadata"]["delegate_depth"] == 1
    assert trace.updates[-1]["metadata"]["continued_session"] is True
    assert "delegated" in trace.updates[-1]["tags"]
    assert span.updates[-1]["metadata"]["parent_session_id"] == "parent-session"
    assert span.updates[-1]["metadata"]["delegate_depth"] == 1


def test_pre_tool_call_adds_lineage_metadata_to_new_tool_span():
    module = patch_external_opik_tracer(_build_fake_opik_module())
    module._on_session_start(session_id="child-session", model="test/model", platform="cli")

    module._pre_tool_call(
        session_id="child-session",
        tool_name="web_search",
        tool_call_id="tool-1",
        parent_session_id="parent-session",
        delegate_depth=2,
    )

    span = module._tool_spans["child-session:tool-1"]
    assert span.updates[-1]["metadata"]["parent_session_id"] == "parent-session"
    assert span.updates[-1]["metadata"]["delegate_depth"] == 2
    assert "delegated" in span.updates[-1]["tags"]
