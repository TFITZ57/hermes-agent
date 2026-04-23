from __future__ import annotations

import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DB_URL_ENV_VARS = (
    "HERMES_HQ_DATABASE_URL",
    "HERMES_HQ_DATABASE_DIRECT_CONNECTION_STRING",
    "SUPABASE_DATABASE_URL",
    "DATABASE_URL",
)


@dataclass
class MonitorRunStart:
    job_id: str
    session_id: str
    host: str
    environment: str
    expected_run_at: datetime | None
    started_at: datetime
    timeout_seconds: float | None


@dataclass
class MonitorRunFinish:
    run_id: str | None
    job_id: str
    session_id: str
    state: str
    ended_at: datetime
    duration_ms: int | None
    output_path: str | None
    final_response_excerpt: str | None
    error_message: str | None
    delivery_error: str | None


class MonitorStore:
    def __init__(self, database_url: str | None = None, psycopg_module: Any | None = None):
        self.database_url_candidates = self._load_database_urls(database_url)
        self.database_url = self.database_url_candidates[0] if self.database_url_candidates else None
        self._psycopg = psycopg_module

    @staticmethod
    def _env_file_candidates() -> list[Path]:
        hermes_home = Path(os.getenv("HERMES_HOME") or (Path.home() / ".hermes"))
        project_env = Path(__file__).resolve().parents[1] / ".env"
        return [hermes_home / ".env", project_env]

    @classmethod
    def _load_database_urls(cls, database_url: str | None = None) -> list[str]:
        if database_url and database_url.strip():
            return [database_url.strip()]
        candidates: list[str] = []
        seen: set[str] = set()

        def add_candidate(value: str | None) -> None:
            cleaned = (value or '').strip()
            if not cleaned or cleaned in seen:
                return
            seen.add(cleaned)
            candidates.append(cleaned)

        for env_var in _DB_URL_ENV_VARS:
            add_candidate(os.getenv(env_var))
        for env_path in cls._env_file_candidates():
            if not env_path.exists():
                continue
            try:
                lines = env_path.read_text(errors='replace').splitlines()
            except OSError:
                continue
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, raw = line.split('=', 1)
                if key.strip() not in _DB_URL_ENV_VARS:
                    continue
                add_candidate(raw.strip().strip('"').strip("'"))
        return candidates

    @staticmethod
    def default_host() -> str:
        return socket.gethostname() or "unknown-host"

    def enabled(self) -> bool:
        return bool(self.database_url)

    def _resolve_psycopg(self) -> Any | None:
        if self._psycopg is not None:
            return self._psycopg
        if not self.database_url:
            return None
        try:
            import psycopg  # type: ignore
        except ImportError:
            logger.debug("MonitorStore disabled: psycopg not installed")
            return None
        self._psycopg = psycopg
        return self._psycopg

    def _connect(self):
        psycopg_module = self._resolve_psycopg()
        if not psycopg_module or not self.database_url_candidates:
            return None
        last_error = None
        for database_url in self.database_url_candidates:
            try:
                conn = psycopg_module.connect(database_url)
            except Exception as exc:  # pragma: no cover - exercised via fake psycopg test
                last_error = exc
                logger.debug('MonitorStore connect failed for %s: %s', database_url, exc)
                continue
            self.database_url = database_url
            return conn
        if last_error is not None:
            logger.warning('MonitorStore disabled: all HQ Postgres connection attempts failed: %s', last_error)
        return None

    @staticmethod
    def _event_key(job_id: str, event_type: str, expected_run_at: datetime | None) -> str | None:
        if expected_run_at is None:
            return None
        return f"{job_id}:{event_type}:{expected_run_at.isoformat()}"

    def _ensure_default_notification_list(self, cur) -> None:
        cur.execute(
            """
            insert into monitoring.notification_lists (list_key, name, is_default)
            values (%s, %s, %s)
            on conflict (list_key) do update
            set name = excluded.name,
                is_default = excluded.is_default,
                updated_at = now()
            """,
            ("default", "Hermes Inbox", True),
        )

    def _ensure_monitor_config(self, cur, payload: MonitorRunStart) -> None:
        self._ensure_default_notification_list(cur)
        cur.execute(
            """
            insert into monitoring.monitor_configs (
                job_id,
                environment,
                timezone,
                failure_tolerance,
                schedule_tolerance,
                grace_seconds,
                consecutive_alert_threshold,
                realert_interval,
                notify_list_key,
                created_at,
                updated_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            on conflict (job_id) do update
            set environment = coalesce(monitoring.monitor_configs.environment, excluded.environment),
                timezone = coalesce(monitoring.monitor_configs.timezone, excluded.timezone),
                updated_at = now()
            """,
            (
                payload.job_id,
                payload.environment,
                "US/Eastern",
                0,
                0,
                60,
                1,
                "8 hours",
                "default",
            ),
        )

    def _record_issue_event(self, cur, issue_id: str, run_id: str | None, event_type: str, payload: dict[str, Any] | None = None) -> None:
        cur.execute(
            """
            insert into monitoring.issue_events (issue_id, run_id, event_type, payload_json)
            values (%s, %s, %s, %s::jsonb)
            """,
            (issue_id, run_id, event_type, self._json(payload or {})),
        )

    @staticmethod
    def _json(payload: dict[str, Any]) -> str:
        import json
        return json.dumps(payload, sort_keys=True, default=str)

    def _resolve_open_issue(self, cur, job_id: str, reason_type: str) -> str | None:
        cur.execute(
            """
            select issue_id
            from monitoring.issues
            where job_id = %s
              and reason_type = %s
              and resolved_at is null
            order by opened_at desc
            limit 1
            """,
            (job_id, reason_type),
        )
        row = cur.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return row.get("issue_id")
        if isinstance(row, tuple):
            return row[0]
        return getattr(row, "issue_id", None)

    def _open_or_update_issue(self, cur, *, job_id: str, reason_type: str, title: str, severity: str, run_id: str | None = None, payload: dict[str, Any] | None = None) -> str:
        issue_id = self._resolve_open_issue(cur, job_id, reason_type)
        if issue_id:
            cur.execute(
                """
                update monitoring.issues
                set state = 'open',
                    severity = %s,
                    title = %s,
                    latest_run_id = %s,
                    updated_at = now()
                where issue_id = %s
                """,
                (severity, title, run_id, issue_id),
            )
        else:
            issue_id = f"issue_{uuid.uuid4().hex[:12]}"
            cur.execute(
                """
                insert into monitoring.issues (
                    issue_id,
                    job_id,
                    reason_type,
                    state,
                    severity,
                    title,
                    opened_at,
                    latest_run_id,
                    notes,
                    created_at,
                    updated_at
                ) values (%s, %s, %s, 'open', %s, %s, now(), %s, %s, now(), now())
                """,
                (issue_id, job_id, reason_type, severity, title, run_id, None),
            )
        self._record_issue_event(cur, issue_id, run_id, "opened_or_updated", payload)
        return issue_id

    def _resolve_issues_for_job(self, cur, job_id: str, run_id: str | None, payload: dict[str, Any] | None = None) -> None:
        cur.execute(
            """
            update monitoring.issues
            set state = 'resolved',
                resolved_at = now(),
                latest_run_id = %s,
                updated_at = now()
            where job_id = %s
              and resolved_at is null
            """,
            (run_id, job_id),
        )
        if getattr(cur, "rowcount", 0):
            cur.execute(
                """
                select issue_id
                from monitoring.issues
                where job_id = %s
                  and latest_run_id = %s
                  and resolved_at is not null
                """,
                (job_id, run_id),
            )
            rows = cur.fetchall() or []
            for row in rows:
                issue_id = row.get("issue_id") if isinstance(row, dict) else row[0]
                self._record_issue_event(cur, issue_id, run_id, "resolved", payload)

    def record_run_start(self, payload: MonitorRunStart) -> str | None:
        conn = self._connect()
        if conn is None:
            return None
        try:
            with conn.cursor() as cur:
                self._ensure_monitor_config(cur, payload)
                run_id = f"run_{uuid.uuid4().hex[:12]}"
                cur.execute(
                    """
                    insert into monitoring.monitor_runs (
                        run_id,
                        job_id,
                        session_id,
                        current_state,
                        started_at,
                        expected_run_at,
                        host,
                        environment,
                        timeout_seconds,
                        created_at,
                        updated_at
                    ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    """,
                    (
                        run_id,
                        payload.job_id,
                        payload.session_id,
                        "running",
                        payload.started_at,
                        payload.expected_run_at,
                        payload.host,
                        payload.environment,
                        payload.timeout_seconds,
                    ),
                )
                cur.execute(
                    """
                    insert into monitoring.monitor_events (
                        job_id,
                        run_id,
                        event_type,
                        event_key,
                        expected_run_at,
                        occurred_at,
                        summary,
                        payload_json,
                        created_at
                    ) values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                    """,
                    (
                        payload.job_id,
                        run_id,
                        "run",
                        self._event_key(payload.job_id, "run", payload.expected_run_at),
                        payload.expected_run_at,
                        payload.started_at,
                        "Cron job started",
                        self._json({
                            "host": payload.host,
                            "environment": payload.environment,
                            "session_id": payload.session_id,
                            "timeout_seconds": payload.timeout_seconds,
                        }),
                    ),
                )
            conn.commit()
            return run_id
        except Exception:
            conn.rollback()
            logger.exception("MonitorStore failed to record run start for job %s", payload.job_id)
            return None
        finally:
            conn.close()

    def record_run_finish(self, payload: MonitorRunFinish) -> None:
        conn = self._connect()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update monitoring.monitor_runs
                    set current_state = %s,
                        ended_at = %s,
                        duration_ms = %s,
                        output_path = %s,
                        final_response_excerpt = %s,
                        error_message = %s,
                        delivery_error = %s,
                        updated_at = now()
                    where run_id = %s
                       or (run_id is null and job_id = %s and session_id = %s)
                    """,
                    (
                        payload.state,
                        payload.ended_at,
                        payload.duration_ms,
                        payload.output_path,
                        payload.final_response_excerpt,
                        payload.error_message,
                        payload.delivery_error,
                        payload.run_id,
                        payload.job_id,
                        payload.session_id,
                    ),
                )
                cur.execute(
                    """
                    insert into monitoring.monitor_events (
                        job_id,
                        run_id,
                        event_type,
                        occurred_at,
                        summary,
                        payload_json,
                        created_at
                    ) values (%s, %s, %s, %s, %s, %s::jsonb, now())
                    """,
                    (
                        payload.job_id,
                        payload.run_id,
                        payload.state,
                        payload.ended_at,
                        f"Cron job {payload.state}",
                        self._json({
                            "duration_ms": payload.duration_ms,
                            "output_path": payload.output_path,
                            "delivery_error": payload.delivery_error,
                            "error_message": payload.error_message,
                        }),
                    ),
                )
                if payload.state in {"complete", "skipped"} and not payload.delivery_error:
                    self._resolve_issues_for_job(cur, payload.job_id, payload.run_id, {"state": payload.state})
                else:
                    reason_type = {
                        "timeout": "run_timeout",
                        "abandoned": "run_abandoned",
                        "fail": "run_failure",
                        "missed": "schedule_missed",
                        "late": "schedule_late",
                    }.get(payload.state, "run_failure")
                    issue_id = self._open_or_update_issue(
                        cur,
                        job_id=payload.job_id,
                        reason_type=reason_type,
                        title=f"{payload.job_id} {payload.state}",
                        severity="warning" if payload.state == "late" else "error",
                        run_id=payload.run_id,
                        payload={"error_message": payload.error_message, "delivery_error": payload.delivery_error},
                    )
                    if payload.delivery_error:
                        self._open_or_update_issue(
                            cur,
                            job_id=payload.job_id,
                            reason_type="delivery_error",
                            title=f"{payload.job_id} delivery failed",
                            severity="error",
                            run_id=payload.run_id,
                            payload={"delivery_error": payload.delivery_error, "source_issue_id": issue_id},
                        )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("MonitorStore failed to record run finish for job %s", payload.job_id)
        finally:
            conn.close()

    def record_schedule_violation(self, job_id: str, *, expected_run_at: datetime, event_type: str, summary: str) -> None:
        conn = self._connect()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                event_key = self._event_key(job_id, event_type, expected_run_at)
                cur.execute(
                    """
                    insert into monitoring.monitor_events (
                        job_id,
                        run_id,
                        event_type,
                        event_key,
                        expected_run_at,
                        occurred_at,
                        summary,
                        payload_json,
                        created_at
                    ) values (%s, %s, %s, %s, %s, now(), %s, %s::jsonb, now())
                    on conflict (event_key) do nothing
                    """,
                    (
                        job_id,
                        None,
                        event_type,
                        event_key,
                        expected_run_at,
                        summary,
                        self._json({"expected_run_at": expected_run_at.isoformat()}),
                    ),
                )
                self._open_or_update_issue(
                    cur,
                    job_id=job_id,
                    reason_type=f"schedule_{event_type}",
                    title=f"{job_id} {event_type}",
                    severity="warning" if event_type == "late" else "error",
                    payload={"expected_run_at": expected_run_at.isoformat(), "summary": summary},
                )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("MonitorStore failed to record schedule violation for job %s", job_id)
        finally:
            conn.close()

    def record_delivery_attempt(
        self,
        job_id: str,
        run_id: str | None,
        *,
        delivery_state: str,
        error_message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        conn = self._connect()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into monitoring.alert_deliveries (
                        delivery_id,
                        issue_id,
                        job_id,
                        run_id,
                        target_id,
                        delivery_state,
                        attempted_at,
                        response_code,
                        response_body_excerpt,
                        error_message,
                        payload_json,
                        created_at
                    ) values (%s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s::jsonb, now())
                    """,
                    (
                        f"delivery_{uuid.uuid4().hex[:12]}",
                        None,
                        job_id,
                        run_id,
                        None,
                        delivery_state,
                        None,
                        None,
                        error_message,
                        self._json(payload or {}),
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("MonitorStore failed to record delivery attempt for job %s", job_id)
        finally:
            conn.close()

    def reconcile_stale_runs(self, now: datetime) -> int:
        conn = self._connect()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update monitoring.monitor_runs
                    set current_state = 'abandoned',
                        ended_at = coalesce(ended_at, %s),
                        error_message = coalesce(error_message, 'stale running run reconciled'),
                        updated_at = now()
                    where current_state = 'running'
                      and started_at + ((coalesce(timeout_seconds, 600) + 60) * interval '1 second') < %s
                    """,
                    (now, now),
                )
                updated = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
            return updated
        except Exception:
            conn.rollback()
            logger.exception("MonitorStore failed to reconcile stale runs")
            return 0
        finally:
            conn.close()


def get_monitor_store() -> MonitorStore:
    return MonitorStore()
