import hermes_cli.plugins as plugins_mod


def test_valid_hooks_include_memory_lifecycle_hooks():
    assert "on_memory_inject" in plugins_mod.VALID_HOOKS
    assert "on_memory_recall" in plugins_mod.VALID_HOOKS
    assert "on_memory_sync" in plugins_mod.VALID_HOOKS
