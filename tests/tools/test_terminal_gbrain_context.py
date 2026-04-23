import json
from types import SimpleNamespace

from tools.terminal_tool import _build_gbrain_span_context, _temporary_env_overrides, terminal_tool


def test_build_gbrain_span_context_uses_gateway_session_values(monkeypatch):
    def _fake_get(name, default=""):
        values = {
            "HERMES_SESSION_PLATFORM": "discord",
            "HERMES_SESSION_CHAT_ID": "chat-123",
            "HERMES_SESSION_THREAD_ID": "thread-456",
            "HERMES_SESSION_USER_ID": "user-789",
            "HERMES_SESSION_KEY": "session-key-1",
        }
        return values.get(name, default)

    monkeypatch.setattr("gateway.session_context.get_session_env", _fake_get)

    env_vars = _build_gbrain_span_context("task-42")
    payload = json.loads(env_vars["HERMES_GBRAIN_SPAN_CONTEXT"])

    assert payload == {
        "caller": "hermes-terminal-tool",
        "source": "terminal_tool",
        "taskId": "task-42",
        "platform": "discord",
        "chatId": "chat-123",
        "threadId": "thread-456",
        "userId": "user-789",
        "sessionKey": "session-key-1",
    }


def test_temporary_env_overrides_restore_original_values():
    env_obj = SimpleNamespace(env={"EXISTING": "keep"})

    with _temporary_env_overrides(env_obj, {"HERMES_GBRAIN_SPAN_CONTEXT": "payload", "EXISTING": "override"}):
        assert env_obj.env["HERMES_GBRAIN_SPAN_CONTEXT"] == "payload"
        assert env_obj.env["EXISTING"] == "override"

    assert env_obj.env == {"EXISTING": "keep"}


def test_terminal_tool_injects_gbrain_span_context_during_execute_and_restores_after(monkeypatch):
    seen = {}

    class _FakeEnv:
        def __init__(self):
            self.env = {"EXISTING": "keep"}

        def execute(self, command, **kwargs):
            seen["command"] = command
            seen["kwargs"] = kwargs
            seen["during"] = dict(self.env)
            return {"output": "ok", "returncode": 0}

    fake_env = _FakeEnv()

    monkeypatch.setattr(
        "tools.terminal_tool._get_env_config",
        lambda: {"env_type": "local", "cwd": ".", "timeout": 30, "docker_image": "", "singularity_image": "", "modal_image": "", "daytona_image": ""},
    )
    monkeypatch.setattr("tools.terminal_tool._start_cleanup_thread", lambda: None)
    monkeypatch.setattr("tools.terminal_tool._foreground_background_guidance", lambda command: None)
    monkeypatch.setattr("tools.terminal_tool.get_active_env", lambda task_id: fake_env)
    monkeypatch.setitem(__import__("tools.terminal_tool", fromlist=["_active_environments"])._active_environments, "task-ctx", fake_env)

    def _fake_get(name, default=""):
        values = {
            "HERMES_SESSION_PLATFORM": "discord",
            "HERMES_SESSION_CHAT_ID": "chat-123",
            "HERMES_SESSION_THREAD_ID": "thread-456",
            "HERMES_SESSION_USER_ID": "user-789",
            "HERMES_SESSION_KEY": "session-key-1",
        }
        return values.get(name, default)

    monkeypatch.setattr("gateway.session_context.get_session_env", _fake_get)

    result = json.loads(terminal_tool("gbrain --version", task_id="task-ctx", force=True))

    payload = json.loads(seen["during"]["HERMES_GBRAIN_SPAN_CONTEXT"])
    assert payload["taskId"] == "task-ctx"
    assert payload["platform"] == "discord"
    assert payload["chatId"] == "chat-123"
    assert payload["threadId"] == "thread-456"
    assert payload["userId"] == "user-789"
    assert payload["sessionKey"] == "session-key-1"
    assert fake_env.env == {"EXISTING": "keep"}
    assert result["exit_code"] == 0
