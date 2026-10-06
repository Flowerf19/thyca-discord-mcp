import json

from audit import append_audit


def test_append_audit_writes_jsonl(tmp_path):
    path = tmp_path / "audit.jsonl"
    record = append_audit(
        path,
        session_id="discord-main",
        actor="mcp",
        tool="send_message",
        args={"channel_id": 1, "content": "hi"},
        ok=True,
    )
    assert record["tool"] == "send_message"
    assert record["ok"] is True
    assert "ts" in record
    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 1
    assert json.loads(lines[0])["args"]["content"] == "hi"


def test_append_audit_redacts_secrets(tmp_path):
    path = tmp_path / "audit.jsonl"
    record = append_audit(
        path,
        session_id="s",
        actor="mcp",
        tool="login",
        args={"token": "abc123", "nested": {"password": "x"}, "ok_field": "keep"},
        ok=False,
        error="LoginFailure",
    )
    assert record["args"]["token"] == "***"
    assert record["args"]["nested"]["password"] == "***"
    assert record["args"]["ok_field"] == "keep"
    assert record["error"] == "LoginFailure"
