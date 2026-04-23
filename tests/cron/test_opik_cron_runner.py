import sys
from pathlib import Path
from types import SimpleNamespace

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

import cron.opik_cron_runner as runner


def test_run_builder_sends_run_complete_and_returns_builder_stdout(monkeypatch):
    pings = []

    def fake_import_module(name):
        assert name == "fake.hourly"
        return SimpleNamespace(main=lambda: print('{"ok": true}'))

    monkeypatch.setattr(runner, "_BUILDERS", {"hourly": ("monitor-hourly", "fake.hourly")})
    monkeypatch.setattr(runner, "_load_runtime_env", lambda: None)
    monkeypatch.setattr(runner.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(runner, "_ping", lambda key, state, message=None: pings.append((key, state, message)))

    output = runner.run_builder("hourly")

    assert output == '{"ok": true}\n'
    assert pings == [("monitor-hourly", "run", None), ("monitor-hourly", "complete", None)]


def test_run_builder_sends_fail_ping_and_reraises(monkeypatch):
    pings = []

    def fail_main():
        raise RuntimeError("boom")

    monkeypatch.setattr(runner, "_BUILDERS", {"daily": ("monitor-daily", "fake.daily")})
    monkeypatch.setattr(runner, "_load_runtime_env", lambda: None)
    monkeypatch.setattr(runner.importlib, "import_module", lambda name: SimpleNamespace(main=fail_main))
    monkeypatch.setattr(runner, "_ping", lambda key, state, message=None: pings.append((key, state, message)))

    with pytest.raises(RuntimeError, match="boom"):
        runner.run_builder("daily")

    assert pings[0] == ("monitor-daily", "run", None)
    assert pings[1][0] == "monitor-daily"
    assert pings[1][1] == "fail"
    assert "RuntimeError: boom" in pings[1][2]


def test_main_prints_selected_builder_output(monkeypatch, capsys):
    monkeypatch.setattr(runner, "run_builder", lambda kind: f"payload:{kind}\n")

    runner.main(["daily"])

    assert capsys.readouterr().out == "payload:daily\n"
