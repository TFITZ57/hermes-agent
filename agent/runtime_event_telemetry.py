from __future__ import annotations

import logging
import uuid
from typing import Optional

logger = logging.getLogger(__name__)


def _new_event_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def emit_command_execute(
    *,
    raw_command: str,
    canonical_command: str,
    command_kind: str,
    source_surface: str,
    platform: str = "",
    session_id: str = "",
    gateway_session_key: str = "",
    user_id: str = "",
    chat_id: str = "",
    thread_id: str = "",
    message_id: str = "",
    args_text: str = "",
    redirect_target: str = "",
    active_agent_running: bool = False,
    command_id: Optional[str] = None,
) -> str:
    command_id = command_id or _new_event_id("cmd")
    try:
        from hermes_cli.plugins import invoke_hook

        invoke_hook(
            "on_command_execute",
            command_id=command_id,
            raw_command=str(raw_command or "").strip(),
            canonical_command=str(canonical_command or "").strip(),
            command_kind=str(command_kind or "").strip(),
            source_surface=str(source_surface or "").strip(),
            platform=str(platform or "").strip(),
            session_id=str(session_id or "").strip(),
            gateway_session_key=str(gateway_session_key or "").strip(),
            user_id=str(user_id or "").strip(),
            chat_id=str(chat_id or "").strip(),
            thread_id=str(thread_id or "").strip(),
            message_id=str(message_id or "").strip(),
            has_args=bool((args_text or "").strip()),
            args_chars=len(args_text or ""),
            redirect_target=str(redirect_target or "").strip(),
            active_agent_running=bool(active_agent_running),
        )
    except Exception:
        logger.debug("command telemetry hook failed", exc_info=True)
    return command_id


def emit_skill_activate(
    *,
    skill_name: str,
    activation_mode: str,
    source_surface: str,
    platform: str = "",
    session_id: str = "",
    gateway_session_key: str = "",
    user_id: str = "",
    chat_id: str = "",
    thread_id: str = "",
    message_id: str = "",
    command_name: str = "",
    skill_command: str = "",
    skill_identifier: str = "",
    instruction_chars: int = 0,
    runtime_note_chars: int = 0,
    skill_dir: str = "",
    activation_id: Optional[str] = None,
) -> str:
    activation_id = activation_id or _new_event_id("skill")
    try:
        from hermes_cli.plugins import invoke_hook

        invoke_hook(
            "on_skill_activate",
            activation_id=activation_id,
            skill_name=str(skill_name or "").strip(),
            activation_mode=str(activation_mode or "").strip(),
            source_surface=str(source_surface or "").strip(),
            platform=str(platform or "").strip(),
            session_id=str(session_id or "").strip(),
            gateway_session_key=str(gateway_session_key or "").strip(),
            user_id=str(user_id or "").strip(),
            chat_id=str(chat_id or "").strip(),
            thread_id=str(thread_id or "").strip(),
            message_id=str(message_id or "").strip(),
            command_name=str(command_name or "").strip(),
            skill_command=str(skill_command or "").strip(),
            skill_identifier=str(skill_identifier or "").strip(),
            instruction_chars=max(0, int(instruction_chars or 0)),
            runtime_note_chars=max(0, int(runtime_note_chars or 0)),
            skill_dir=str(skill_dir or "").strip(),
        )
    except Exception:
        logger.debug("skill activation telemetry hook failed", exc_info=True)
    return activation_id
