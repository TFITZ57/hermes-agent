import importlib.util
from pathlib import Path


def _load_opik_tracer_module():
    plugin_path = Path.home() / ".hermes" / "plugins" / "opik-tracer" / "__init__.py"
    spec = importlib.util.spec_from_file_location("opik_tracer_plugin", plugin_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_opik_tracer_registers_memory_lifecycle_hooks():
    module = _load_opik_tracer_module()

    registered_hooks = []

    class DummyCtx:
        def register_hook(self, name, callback):
            registered_hooks.append((name, callback))

    module.register(DummyCtx())

    hook_names = {name for name, _ in registered_hooks}
    assert "on_memory_inject" in hook_names
    assert "on_memory_recall" in hook_names
    assert "on_memory_sync" in hook_names


def test_memory_inject_bootstraps_trace_from_session_id(monkeypatch):
    module = _load_opik_tracer_module()

    class DummySpan:
        def __init__(self):
            self.ended = False

        def end(self):
            self.ended = True

    class DummyClient:
        def __init__(self):
            self.spans = []

        def span(self, **kwargs):
            self.spans.append(kwargs)
            return DummySpan()

    class DummyTrace:
        id = "trace-123"

    dummy_client = DummyClient()
    monkeypatch.setattr(module, "_get_client", lambda: dummy_client)
    monkeypatch.setattr(module, "_traces", {})
    monkeypatch.setattr(
        module,
        "_bootstrap_trace",
        lambda session_id, **kwargs: (session_id, DummyTrace()),
    )

    module._on_memory_inject(
        session_id="session-123",
        memory_chars=10,
        user_chars=20,
        external_chars=30,
        total_chars=60,
        estimated_tokens=15,
    )

    assert len(dummy_client.spans) == 1
    assert dummy_client.spans[0]["trace_id"] == "trace-123"
    assert dummy_client.spans[0]["name"] == "memory:inject"
