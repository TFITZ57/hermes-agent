"""Compatibility shims for the external opik-tracer plugin.

Hermes ships the hook lifecycle, but the Opik tracer itself lives as an
external plugin under ``~/.hermes/plugins``.  This module patches that plugin
in-process so Hermes can harden tracing behaviour without mutating user plugin
files.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Optional


def _lineage_metadata(**kwargs: Any) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    parent_session_id = kwargs.get("parent_session_id")
    if parent_session_id:
        metadata["parent_session_id"] = parent_session_id
        metadata["is_delegated_session"] = True

    delegate_depth = kwargs.get("delegate_depth")
    if delegate_depth not in (None, ""):
        metadata["delegate_depth"] = delegate_depth

    if "continued_session" in kwargs:
        metadata["continued_session"] = bool(kwargs.get("continued_session"))

    session_prompt_source = kwargs.get("session_prompt_source")
    if session_prompt_source:
        metadata["session_prompt_source"] = session_prompt_source

    bootstrap_reason = kwargs.get("trace_bootstrap_reason")
    if bootstrap_reason:
        metadata["trace_bootstrap_reason"] = bootstrap_reason

    return metadata


def _lineage_tags(**kwargs: Any) -> list[str]:
    tags: list[str] = []
    if kwargs.get("parent_session_id"):
        tags.append("delegated")
    if kwargs.get("continued_session"):
        tags.append("continued")
    return tags


def patch_external_opik_tracer(module: Any) -> Any:
    """Patch the loaded external ``opik-tracer`` plugin module in-place.

    The external plugin keeps mutable process-global dictionaries keyed by
    session ID. Parallel delegated children can mutate that state from
    multiple threads, so we wrap the tracer hooks with a re-entrant lock and
    add two Hermes-specific behaviours:

    1. Lazy trace bootstrap when a trace is missing at the start of a hook.
    2. Delegation/session-lineage metadata updates for traces and spans.
    """

    if module is None or getattr(module, "_hermes_compat_patched", False):
        return module

    state_lock = threading.RLock()
    setattr(module, "_hermes_state_lock", state_lock)

    orig_on_session_start = getattr(module, "_on_session_start", None)
    orig_pre_llm_call = getattr(module, "_pre_llm_call", None)
    orig_post_llm_call = getattr(module, "_post_llm_call", None)
    orig_pre_tool_call = getattr(module, "_pre_tool_call", None)
    orig_post_tool_call = getattr(module, "_post_tool_call", None)
    orig_pre_api_request = getattr(module, "_pre_api_request", None)
    orig_post_api_request = getattr(module, "_post_api_request", None)
    orig_on_session_end = getattr(module, "_on_session_end", None)

    def _update_entity(entity: Any, *, metadata: Optional[Dict[str, Any]] = None, tags: Optional[list[str]] = None) -> None:
        if entity is None or not hasattr(entity, "update"):
            return
        update_kwargs: Dict[str, Any] = {}
        if metadata:
            update_kwargs["metadata"] = metadata
        if tags:
            update_kwargs["tags"] = tags
        if update_kwargs:
            entity.update(**update_kwargs)

    def _update_session_trace(session_id: str, **kwargs: Any) -> Any:
        trace = getattr(module, "_traces", {}).get(session_id)
        if trace is None:
            return None
        _update_entity(
            trace,
            metadata=_lineage_metadata(**kwargs),
            tags=_lineage_tags(**kwargs),
        )
        return trace

    def _ensure_trace(session_id: str = "", model: str = "", platform: str = "", **kwargs: Any) -> Any:
        if not session_id:
            return None
        trace = getattr(module, "_traces", {}).get(session_id)
        if trace is None and orig_on_session_start is not None:
            bootstrap_kwargs = dict(kwargs)
            bootstrap_kwargs.setdefault("trace_bootstrap_reason", "hook_rebootstrap")
            orig_on_session_start(
                session_id=session_id,
                model=model,
                platform=platform,
                **bootstrap_kwargs,
            )
        return _update_session_trace(session_id, **kwargs)

    def _decorate_api_span(session_id: str, **kwargs: Any) -> None:
        span = getattr(module, "_api_call_spans", {}).get(session_id)
        if span is not None:
            _update_entity(span, metadata=_lineage_metadata(**kwargs), tags=_lineage_tags(**kwargs))

    def _decorate_tool_spans(session_id: str, before_keys: set[str], **kwargs: Any) -> None:
        tool_spans = getattr(module, "_tool_spans", {})
        after_keys = set(tool_spans.keys())
        new_keys = after_keys - before_keys

        tool_call_id = kwargs.get("tool_call_id")
        if tool_call_id:
            candidate_key = f"{session_id}:{tool_call_id}"
            if candidate_key in tool_spans:
                new_keys.add(candidate_key)

        for key in new_keys:
            span = tool_spans.get(key)
            if span is not None:
                _update_entity(span, metadata=_lineage_metadata(**kwargs), tags=_lineage_tags(**kwargs))

    def _wrap_locked(callback):
        if callback is None:
            return None

        def _wrapped(*args: Any, **kwargs: Any):
            with state_lock:
                return callback(*args, **kwargs)

        _wrapped.__name__ = getattr(callback, "__name__", "wrapped_opik_hook")
        _wrapped.__doc__ = getattr(callback, "__doc__", None)
        return _wrapped

    def _wrapped_on_session_start(*, session_id: str, model: str = "", platform: str = "", **kwargs: Any):
        with state_lock:
            if orig_on_session_start is not None:
                orig_on_session_start(session_id=session_id, model=model, platform=platform, **kwargs)
            return _update_session_trace(session_id, model=model, platform=platform, **kwargs)

    def _wrapped_pre_api_request(*, session_id: str = "", model: str = "", platform: str = "", **kwargs: Any):
        with state_lock:
            _ensure_trace(session_id, model=model, platform=platform, **kwargs)
            result = orig_pre_api_request(session_id=session_id, model=model, platform=platform, **kwargs) if orig_pre_api_request is not None else None
            _decorate_api_span(session_id, model=model, platform=platform, **kwargs)
            return result

    def _wrapped_pre_tool_call(*, session_id: str = "", model: str = "", platform: str = "", **kwargs: Any):
        with state_lock:
            _ensure_trace(session_id, model=model, platform=platform, **kwargs)
            before_keys = set(getattr(module, "_tool_spans", {}).keys())
            result = orig_pre_tool_call(session_id=session_id, **kwargs) if orig_pre_tool_call is not None else None
            _decorate_tool_spans(session_id, before_keys, model=model, platform=platform, **kwargs)
            return result

    module._on_session_start = _wrapped_on_session_start
    module._pre_llm_call = _wrap_locked(orig_pre_llm_call)
    module._post_llm_call = _wrap_locked(orig_post_llm_call)
    module._pre_tool_call = _wrapped_pre_tool_call
    module._post_tool_call = _wrap_locked(orig_post_tool_call)
    module._pre_api_request = _wrapped_pre_api_request
    module._post_api_request = _wrap_locked(orig_post_api_request)
    module._on_session_end = _wrap_locked(orig_on_session_end)
    module._hermes_compat_patched = True
    return module
