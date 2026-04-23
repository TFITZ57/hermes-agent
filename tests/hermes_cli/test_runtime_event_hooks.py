from hermes_cli import plugins as plugins_mod


def test_valid_hooks_include_runtime_event_hooks():
    assert "on_command_execute" in plugins_mod.VALID_HOOKS
    assert "on_skill_activate" in plugins_mod.VALID_HOOKS
