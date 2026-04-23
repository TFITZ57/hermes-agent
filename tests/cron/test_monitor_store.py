from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cron.monitor_store import MonitorRunFinish, MonitorRunStart, MonitorStore


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: tuple[object, ...] | None = None):
        compact = ' '.join(sql.split()).lower()
        self.calls.append((compact, params))
        if 'update monitoring.monitor_runs' in compact:
            self.rowcount = 1
        elif 'update monitoring.issues' in compact:
            self.rowcount = 1
        else:
            self.rowcount = 0
        return self

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakePsycopg:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn
        self.calls: list[str] = []

    def connect(self, database_url: str):
        self.calls.append(database_url)
        return self._conn


class TestMonitorStore:
    def test_record_run_start_writes_config_run_and_event(self):
        conn = FakeConnection()
        psycopg = FakePsycopg(conn)
        store = MonitorStore(database_url='postgresql://example/monitoring', psycopg_module=psycopg)

        started_at = datetime(2026, 4, 22, 16, 5, tzinfo=timezone.utc)
        expected_run_at = datetime(2026, 4, 22, 16, 0, tzinfo=timezone.utc)
        run_id = store.record_run_start(
            MonitorRunStart(
                job_id='job-123',
                session_id='cron_job-123_20260422_160500',
                host='test-host.local',
                environment='production',
                expected_run_at=expected_run_at,
                started_at=started_at,
                timeout_seconds=600,
            )
        )

        assert run_id
        executed_sql = '\n'.join(sql for sql, _ in conn.cursor_obj.calls)
        assert 'insert into monitoring.monitor_configs' in executed_sql
        assert 'insert into monitoring.monitor_runs' in executed_sql
        assert 'insert into monitoring.monitor_events' in executed_sql
        assert conn.commits == 1
        assert conn.closed is True

    def test_record_run_finish_updates_run_writes_event_and_opens_delivery_issue_when_needed(self):
        conn = FakeConnection()
        psycopg = FakePsycopg(conn)
        store = MonitorStore(database_url='postgresql://example/monitoring', psycopg_module=psycopg)

        ended_at = datetime(2026, 4, 22, 16, 9, tzinfo=timezone.utc)
        store.record_run_finish(
            MonitorRunFinish(
                run_id='run-123',
                job_id='job-123',
                session_id='cron_job-123_20260422_160500',
                state='complete',
                ended_at=ended_at,
                duration_ms=240000,
                output_path='/tmp/job-123.md',
                final_response_excerpt='done',
                error_message=None,
                delivery_error='network timeout',
            )
        )

        executed_sql = '\n'.join(sql for sql, _ in conn.cursor_obj.calls)
        assert 'update monitoring.monitor_runs' in executed_sql
        assert 'insert into monitoring.monitor_events' in executed_sql
        assert 'insert into monitoring.issues' in executed_sql
        assert conn.commits == 1
        assert conn.closed is True

    def test_record_schedule_violation_writes_deduped_event_and_issue(self):
        conn = FakeConnection()
        psycopg = FakePsycopg(conn)
        store = MonitorStore(database_url='postgresql://example/monitoring', psycopg_module=psycopg)

        expected_run_at = datetime(2026, 4, 22, 15, 0, tzinfo=timezone.utc)
        store.record_schedule_violation(
            'job-123',
            expected_run_at=expected_run_at,
            event_type='missed',
            summary='Job missed its expected run window.',
        )

        executed_sql = '\n'.join(sql for sql, _ in conn.cursor_obj.calls)
        assert 'insert into monitoring.monitor_events' in executed_sql
        assert 'on conflict (event_key) do nothing' in executed_sql
        assert 'insert into monitoring.issues' in executed_sql
        assert conn.commits == 1
        assert conn.closed is True

    def test_reconcile_stale_runs_marks_running_rows_abandoned(self):
        conn = FakeConnection()
        psycopg = FakePsycopg(conn)
        store = MonitorStore(database_url='postgresql://example/monitoring', psycopg_module=psycopg)

        now = datetime(2026, 4, 22, 16, 20, tzinfo=timezone.utc)
        count = store.reconcile_stale_runs(now)

        executed_sql = '\n'.join(sql for sql, _ in conn.cursor_obj.calls)
        assert 'update monitoring.monitor_runs' in executed_sql
        assert 'current_state = ' in executed_sql
        assert count == 1
        assert conn.commits == 1
        assert conn.closed is True

    def test_store_loads_database_url_from_hermes_dotenv(self, monkeypatch, tmp_path):
        hermes_home = tmp_path / 'hermes-home'
        hermes_home.mkdir()
        (hermes_home / '.env').write_text(
            'HERMES_HQ_DATABASE_URL=postgresql://dotenv-user:secret@example.com:5432/postgres\n',
            encoding='utf-8',
        )
        monkeypatch.setenv('HERMES_HOME', str(hermes_home))
        for env_var in (
            'HERMES_HQ_DATABASE_DIRECT_CONNECTION_STRING',
            'HERMES_HQ_DATABASE_URL',
            'SUPABASE_DATABASE_URL',
            'DATABASE_URL',
        ):
            monkeypatch.delenv(env_var, raising=False)

        store = MonitorStore(database_url=None, psycopg_module=None)

        assert store.database_url == 'postgresql://dotenv-user:secret@example.com:5432/postgres'

    def test_store_is_disabled_without_database_url(self, monkeypatch):
        for env_var in (
            'HERMES_HQ_DATABASE_DIRECT_CONNECTION_STRING',
            'HERMES_HQ_DATABASE_URL',
            'SUPABASE_DATABASE_URL',
            'DATABASE_URL',
        ):
            monkeypatch.delenv(env_var, raising=False)
        store = MonitorStore(database_url='', psycopg_module=None)

        run_id = store.record_run_start(
            MonitorRunStart(
                job_id='job-123',
                session_id='cron_job-123_20260422_160500',
                host='test-host.local',
                environment='production',
                expected_run_at=None,
                started_at=datetime.now(timezone.utc),
                timeout_seconds=600,
            )
        )
        assert run_id is None
        assert store.reconcile_stale_runs(datetime.now(timezone.utc) + timedelta(minutes=10)) == 0
