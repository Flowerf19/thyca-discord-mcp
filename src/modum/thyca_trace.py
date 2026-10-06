"""Pure native trace detail to evidence correlation (no I/O).

Canonical owner of the non-I/O ``detail -> evidence`` processing moved
verbatim from ``ThycaClient.get_trace_evidence`` post-fetch. Takes the
parsed trace-detail dict plus explicit caller IDs and returns the exact
evidence shape; never touches private client state and never imports the
client (no cycle). Depends only on
:mod:`modum.thyca_errors` and
:mod:`modum.thyca_validate`. No HTTP, no retries, no policy.
"""

from __future__ import annotations

import json

from .thyca_errors import ThycaProtocolError
from .thyca_validate import (
    _ERROR_KINDS,
    _JUMP_RE,
    _TRACE_STATUSES,
    ARG_CHANNEL,
    ARG_CONTENT,
    ARG_JOB_REF,
    ARG_REASON,
    ARG_SOURCE_MID,
    ARG_STATUS,
    NO_REPLY_REASONS,
    NO_REPLY_STATUS_DECLARED,
    NO_REPLY_TOOL_NAME,
    SEND_TOOL_NAME,
    _as_discord_id,
    _sha256_hex,
    _validate_message_roles,
)


def build_trace_evidence(
    payload: dict,
    *,
    native_sid: str,
    turn_index: int,
    job_ref: str,
    source_message_id: int,
    channel_id: int,
    guild_id: int | None = None,
) -> dict:
    """Correlate recorded send and no-reply calls from one parsed trace.

    ``payload`` is the ``_parse_json_dict`` output for
    ``GET /api/traces/<sid>/<index>``. Caller IDs are explicit arguments
    (no controller object, no ``self``); only match booleans, counts,
    lengths, and hashes are returned. See
    ``ThycaClient.get_trace_evidence`` for the full proof contract.
    """
    session_ret = payload.get("session_id")
    index_ret = payload.get("turn_index")
    status_ret = payload.get("status")
    model_ret = payload.get("model")
    error_ret = payload.get("error")
    messages_ret = payload.get("messages")
    if not isinstance(session_ret, str) or not session_ret:
        raise ThycaProtocolError(reason="malformed")
    if type(index_ret) is not int or index_ret < 0:
        raise ThycaProtocolError(reason="malformed")
    if not isinstance(status_ret, str) or not status_ret:
        raise ThycaProtocolError(reason="malformed")
    if model_ret is not None and not isinstance(model_ret, str):
        raise ThycaProtocolError(reason="malformed")
    if error_ret is not None and not isinstance(error_ret, dict):
        raise ThycaProtocolError(reason="malformed")
    messages = _validate_message_roles(messages_ret)

    trace_session_match = session_ret == native_sid
    turn_index_match = index_ret == turn_index
    if status_ret in _TRACE_STATUSES:
        status_out = status_ret
    else:
        status_out = "unknown"
    if model_ret is None:
        model_present = False
        model_len: int | None = None
        model_hash: str | None = None
    else:
        model_present = True
        model_len = len(model_ret)
        model_hash = _sha256_hex(model_ret)
    if error_ret is None:
        error_present = False
        error_kind = "none"
        error_code_len: int | None = None
        error_message_len: int | None = None
        error_keys_len: int | None = None
    else:
        error_present = True
        code_raw = error_ret.get("code")
        msg_raw = error_ret.get("message")
        if isinstance(code_raw, str) and code_raw in _ERROR_KINDS:
            error_kind = code_raw
        else:
            error_kind = "unknown"
        error_code_len = len(code_raw) if isinstance(code_raw, str) else None
        error_message_len = len(msg_raw) if isinstance(msg_raw, str) else None
        error_keys_len = len(error_ret)

    request_job_ref_match = False
    request_user_count = 0
    first_user_content: str | None = None
    first_user_seen = False
    for item in messages:
        if item.get("role") != "user":
            continue
        request_user_count += 1
        content = item.get("content")
        if not first_user_seen:
            first_user_seen = True
            if isinstance(content, str):
                first_user_content = content
        if isinstance(content, str) and job_ref in content:
            request_job_ref_match = True
    # Native per-turn trace holds exactly one slice, hence at most one
    # user frame when present. More than one user frame is malformed and
    # must never yield tool proof.
    if request_user_count > 1:
        raise ThycaProtocolError(reason="malformed")
    if first_user_content is not None:
        request_input_sha256: str | None = _sha256_hex(first_user_content)
        request_input_len: int | None = len(first_user_content)
    else:
        request_input_sha256 = None
        request_input_len = None
    # first_user_content intentionally goes out of scope here; only the
    # hash/len leave this function.

    tool_results: dict[str, dict] = {}
    tool_result_counts: dict[str, int] = {}
    for item in messages:
        if item.get("role") != "tool":
            continue
        call_id = item.get("tool_call_id")
        if isinstance(call_id, str) and call_id:
            tool_result_counts[call_id] = tool_result_counts.get(call_id, 0) + 1
            if call_id not in tool_results:
                tool_results[call_id] = item

    sends: list[dict] = []
    observation_calls: list[dict] = []
    all_call_ids: list[str] = []
    other_tool_count = 0
    other_name_lens: list[int] = []
    for item in messages:
        if item.get("role") != "assistant":
            continue
        calls = item.get("tool_calls")
        if calls is None:
            continue
        if not isinstance(calls, list):
            raise ThycaProtocolError(reason="malformed")
        for call in calls:
            if not isinstance(call, dict):
                raise ThycaProtocolError(reason="malformed")
            call_id = call.get("id")
            call_name = call.get("name")
            if not isinstance(call_id, str) or not call_id:
                raise ThycaProtocolError(reason="malformed")
            if not isinstance(call_name, str) or not call_name:
                raise ThycaProtocolError(reason="malformed")
            all_call_ids.append(call_id)
            if call_name != SEND_TOOL_NAME:
                other_tool_count += 1
                other_name_lens.append(len(call_name))
                if call_name == NO_REPLY_TOOL_NAME:
                    obs_args = call.get("arguments")
                    if obs_args is None:
                        obs_args = {}
                    if not isinstance(obs_args, dict):
                        raise ThycaProtocolError(reason="malformed")
                    obs_channel = _as_discord_id(obs_args.get(ARG_CHANNEL)) == channel_id
                    obs_source = _as_discord_id(obs_args.get(ARG_SOURCE_MID)) == source_message_id
                    obs_job_arg = obs_args.get(ARG_JOB_REF)
                    obs_job = isinstance(obs_job_arg, str) and obs_job_arg == job_ref
                    obs_reason_arg = obs_args.get(ARG_REASON)
                    obs_reason_valid = (
                        isinstance(obs_reason_arg, str) and obs_reason_arg in NO_REPLY_REASONS
                    )
                    # Whitelisted enum for Router comparison against the
                    # persisted job.no_reply_declaration.reason. Never
                    # echo untrusted strings/types/reprs; None when invalid.
                    if obs_reason_valid:
                        obs_reason_out: str | None = obs_reason_arg  # type: ignore[assignment]
                    else:
                        obs_reason_out = None
                    obs_correlated = bool(
                        obs_channel and obs_source and obs_job and obs_reason_valid
                    )
                    obs_result = tool_results.get(call_id)
                    obs_result_count = tool_result_counts.get(call_id, 0)
                    if obs_result is None:
                        observation_calls.append(
                            {
                                "tool_call_id": call_id,
                                "channel_match": obs_channel,
                                "source_mid_match": obs_source,
                                "job_ref_match": obs_job,
                                "reason_valid": obs_reason_valid,
                                "reason": obs_reason_out,
                                "correlated": obs_correlated,
                                "result_present": False,
                                "result_ids_match": False,
                                "result_reason_match": False,
                                "result_status_declared": False,
                                "result_ok": False,
                            }
                        )
                        continue
                    obs_meta = obs_result.get("meta")
                    obs_is_error = isinstance(obs_meta, dict) and obs_meta.get("is_error") is True
                    obs_content = obs_result.get("content")
                    obs_parsed: dict | None = None
                    if isinstance(obs_content, str) and obs_content.strip():
                        try:
                            obs_decoded = json.loads(obs_content)
                        except (json.JSONDecodeError, ValueError):
                            obs_decoded = None
                        if isinstance(obs_decoded, dict):
                            obs_parsed = obs_decoded
                    obs_has_error = obs_parsed is not None and "error" in obs_parsed
                    if obs_parsed is None or obs_has_error or obs_is_error:
                        observation_calls.append(
                            {
                                "tool_call_id": call_id,
                                "channel_match": obs_channel,
                                "source_mid_match": obs_source,
                                "job_ref_match": obs_job,
                                "reason_valid": obs_reason_valid,
                                "reason": obs_reason_out,
                                "correlated": obs_correlated,
                                "result_present": True,
                                "result_ids_match": False,
                                "result_reason_match": False,
                                "result_status_declared": False,
                                "result_ok": False,
                            }
                        )
                        continue
                    assert obs_parsed is not None
                    obs_ch = _as_discord_id(obs_parsed.get(ARG_CHANNEL))
                    obs_mid = _as_discord_id(obs_parsed.get(ARG_SOURCE_MID))
                    obs_rjob = obs_parsed.get(ARG_JOB_REF)
                    obs_rreason = obs_parsed.get(ARG_REASON)
                    obs_rstatus = obs_parsed.get(ARG_STATUS)
                    obs_ids = bool(
                        obs_ch is not None
                        and obs_ch == channel_id
                        and obs_mid is not None
                        and obs_mid == source_message_id
                        and isinstance(obs_rjob, str)
                        and obs_rjob == job_ref
                    )
                    obs_reason_ok = bool(
                        isinstance(obs_rreason, str)
                        and obs_rreason in NO_REPLY_REASONS
                        and isinstance(obs_reason_arg, str)
                        and obs_reason_arg in NO_REPLY_REASONS
                        and obs_rreason == obs_reason_arg
                    )
                    obs_status_ok = (
                        isinstance(obs_rstatus, str) and obs_rstatus == NO_REPLY_STATUS_DECLARED
                    )
                    # Observation proof requires exactly one tool-result row
                    # for this call ID. SEND keeps first-wins for compat;
                    # observation fails closed on duplicates even when the
                    # first row is positive (contradictory rows never prove).
                    obs_singleton = obs_result_count == 1
                    obs_ok = bool(
                        obs_correlated
                        and obs_ids
                        and obs_reason_ok
                        and obs_status_ok
                        and obs_singleton
                    )
                    observation_calls.append(
                        {
                            "tool_call_id": call_id,
                            "channel_match": obs_channel,
                            "source_mid_match": obs_source,
                            "job_ref_match": obs_job,
                            "reason_valid": obs_reason_valid,
                            "reason": obs_reason_out,
                            "correlated": obs_correlated,
                            "result_present": True,
                            "result_ids_match": obs_ids,
                            "result_reason_match": obs_reason_ok,
                            "result_status_declared": obs_status_ok,
                            "result_ok": obs_ok,
                        }
                    )
                continue
            args = call.get("arguments")
            if args is None:
                args = {}
            if not isinstance(args, dict):
                raise ThycaProtocolError(reason="malformed")
            channel_match = _as_discord_id(args.get(ARG_CHANNEL)) == channel_id
            source_match = _as_discord_id(args.get(ARG_SOURCE_MID)) == source_message_id
            job_arg = args.get(ARG_JOB_REF)
            job_match = isinstance(job_arg, str) and job_arg == job_ref
            content_arg = args.get(ARG_CONTENT)
            if isinstance(content_arg, str):
                content_len: int | None = len(content_arg)
                content_hash: str | None = _sha256_hex(content_arg)
            else:
                content_len = None
                content_hash = None
            # args/content_arg intentionally go out of scope here; only
            # match booleans, len, and hash are kept.
            result_item = tool_results.get(call_id)
            if result_item is None:
                sends.append(
                    {
                        "tool_call_id": call_id,
                        "channel_match": channel_match,
                        "source_mid_match": source_match,
                        "job_ref_match": job_match,
                        "correlated": bool(channel_match and source_match and job_match),
                        "result_present": False,
                        "result_message_id": None,
                        "result_channel_match": False,
                        "jump_present": False,
                        "jump_ids_match": False,
                        "result_ok": False,
                        "content_len": content_len,
                        "content_sha256": content_hash,
                    }
                )
                continue
            meta = result_item.get("meta")
            is_error = isinstance(meta, dict) and meta.get("is_error") is True
            result_content = result_item.get("content")
            parsed: dict | None = None
            if isinstance(result_content, str) and result_content.strip():
                try:
                    decoded = json.loads(result_content)
                except (json.JSONDecodeError, ValueError):
                    decoded = None
                if isinstance(decoded, dict):
                    parsed = decoded
            has_error_key = parsed is not None and "error" in parsed
            if parsed is None or has_error_key or is_error:
                sends.append(
                    {
                        "tool_call_id": call_id,
                        "channel_match": channel_match,
                        "source_mid_match": source_match,
                        "job_ref_match": job_match,
                        "correlated": bool(channel_match and source_match and job_match),
                        "result_present": True,
                        "result_message_id": None,
                        "result_channel_match": False,
                        "jump_present": isinstance(parsed.get("jump_url") if parsed else None, str),
                        "jump_ids_match": False,
                        "result_ok": False,
                        "content_len": content_len,
                        "content_sha256": content_hash,
                    }
                )
                continue
            assert parsed is not None
            mid = _as_discord_id(parsed.get("message_id"))
            channel_parsed = _as_discord_id(parsed.get("channel_id"))
            jump_raw = parsed.get("jump_url")
            result_channel_match = channel_parsed is not None and channel_parsed == channel_id
            jump_present = isinstance(jump_raw, str) and bool(jump_raw)
            jump_match = False
            if isinstance(jump_raw, str):
                match = _JUMP_RE.fullmatch(jump_raw.strip())
                if match is not None and mid is not None:
                    try:
                        guild_part = int(match.group(1))
                        channel_part = int(match.group(2))
                        mid_part = int(match.group(3))
                    except ValueError:
                        guild_part = channel_part = mid_part = 0
                    if (
                        guild_part > 0
                        and channel_part == channel_id
                        and mid_part == mid
                        and (guild_id is None or guild_part == guild_id)
                    ):
                        jump_match = True
            result_ok = bool(
                channel_match
                and source_match
                and job_match
                and mid is not None
                and result_channel_match
                and jump_match
            )
            sends.append(
                {
                    "tool_call_id": call_id,
                    "channel_match": channel_match,
                    "source_mid_match": source_match,
                    "job_ref_match": job_match,
                    "correlated": bool(channel_match and source_match and job_match),
                    "result_present": True,
                    "result_message_id": mid,
                    "result_channel_match": result_channel_match,
                    "jump_present": jump_present,
                    "jump_ids_match": jump_match,
                    "result_ok": result_ok,
                    "content_len": content_len,
                    "content_sha256": content_hash,
                }
            )

    correlated = [s for s in sends if s["correlated"]]
    all_confirmed = bool(
        trace_session_match
        and turn_index_match
        and request_user_count == 1
        and request_job_ref_match
        and correlated
        and all(s["result_ok"] for s in correlated)
    )
    correlated_obs = [o for o in observation_calls if o["correlated"]]
    unique_call_ids = len(set(all_call_ids)) == len(all_call_ids)
    all_obs_confirmed = bool(
        trace_session_match
        and turn_index_match
        and request_user_count == 1
        and request_job_ref_match
        and correlated_obs
        and all(o["result_ok"] for o in correlated_obs)
        and unique_call_ids
    )
    return {
        "native_sid": native_sid,
        "requested_turn_index": turn_index,
        "returned_turn_index": index_ret,
        "trace_session_match": trace_session_match,
        "turn_index_match": turn_index_match,
        "status": status_out,
        "status_len": len(status_ret),
        "model_present": model_present,
        "model_len": model_len,
        "model_sha256": model_hash,
        "error_present": error_present,
        "error_kind": error_kind,
        "error_code_len": error_code_len,
        "error_message_len": error_message_len,
        "error_keys_len": error_keys_len,
        "request_job_ref_match": request_job_ref_match,
        "request_job_ref_len": len(job_ref),
        "request_job_ref_sha256": _sha256_hex(job_ref),
        "request_user_count": request_user_count,
        "request_input_sha256": request_input_sha256,
        "request_input_len": request_input_len,
        "send_call_count": len(sends),
        "correlated_send_count": len(correlated),
        "legacy_uncorrelated_count": len(sends) - len(correlated),
        "other_tool_call_count": other_tool_count,
        "other_tool_name_lens": other_name_lens,
        "sends": sends,
        "all_sends_confirmed": all_confirmed,
        "observation_calls": observation_calls,
        "observation_call_count": len(observation_calls),
        "all_observations_confirmed": all_obs_confirmed,
        "proof": "trace_detail",
    }
