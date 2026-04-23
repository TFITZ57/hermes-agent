from unittest.mock import patch

from agent.runtime_event_telemetry import emit_command_execute, emit_skill_activate


def test_emit_command_execute_invokes_hook_with_safe_fields():
    with patch("hermes_cli.plugins.invoke_hook") as mock_invoke:
        command_id = emit_command_execute(
            raw_command="/plan",
            canonical_command="plan",
            command_kind="builtin",
            source_surface="cli",
            platform="cli",
            session_id="session-1",
            args_text="Add OAuth login",
            redirect_target="",
            active_agent_running=True,
        )

    assert command_id.startswith("cmd_")
    mock_invoke.assert_called_once()
    kwargs = mock_invoke.call_args.kwargs
    assert mock_invoke.call_args.args[0] == "on_command_execute"
    assert kwargs["command_id"] == command_id
    assert kwargs["raw_command"] == "/plan"
    assert kwargs["canonical_command"] == "plan"
    assert kwargs["command_kind"] == "builtin"
    assert kwargs["source_surface"] == "cli"
    assert kwargs["platform"] == "cli"
    assert kwargs["session_id"] == "session-1"
    assert kwargs["has_args"] is True
    assert kwargs["args_chars"] == len("Add OAuth login")
    assert kwargs["active_agent_running"] is True



def test_emit_skill_activate_invokes_hook_with_safe_fields():
    with patch("hermes_cli.plugins.invoke_hook") as mock_invoke:
        activation_id = emit_skill_activate(
            skill_name="plan",
            activation_mode="slash",
            source_surface="gateway",
            platform="discord",
            session_id="session-2",
            gateway_session_key="gateway-key",
            command_name="plan",
            skill_command="/plan",
            skill_identifier="plan",
            instruction_chars=12,
            runtime_note_chars=24,
            skill_dir="/tmp/plan",
        )

    assert activation_id.startswith("skill_")
    mock_invoke.assert_called_once()
    kwargs = mock_invoke.call_args.kwargs
    assert mock_invoke.call_args.args[0] == "on_skill_activate"
    assert kwargs["activation_id"] == activation_id
    assert kwargs["skill_name"] == "plan"
    assert kwargs["activation_mode"] == "slash"
    assert kwargs["source_surface"] == "gateway"
    assert kwargs["platform"] == "discord"
    assert kwargs["session_id"] == "session-2"
    assert kwargs["gateway_session_key"] == "gateway-key"
    assert kwargs["command_name"] == "plan"
    assert kwargs["skill_command"] == "/plan"
    assert kwargs["instruction_chars"] == 12
    assert kwargs["runtime_note_chars"] == 24
    assert kwargs["skill_dir"] == "/tmp/plan"
