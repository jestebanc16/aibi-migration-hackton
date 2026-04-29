"""Serving endpoint chat wrapper (parameter retry logic)."""

from __future__ import annotations

from unittest.mock import MagicMock

from aibi_migrator.dbx_client.workspace import WorkspaceResources


def _resp_with_content(text: str) -> MagicMock:
    msg = MagicMock()
    msg.content = text
    ch = MagicMock()
    ch.message = msg
    ch.text = None
    resp = MagicMock()
    resp.choices = [ch]
    return resp


def test_query_serving_endpoint_chat_retries_without_temperature() -> None:
    client = MagicMock()
    client.serving_endpoints.query.side_effect = [
        Exception("Model does not allow temperature parameter"),
        _resp_with_content('{"ok": true}'),
    ]
    wr = WorkspaceResources(client=client)

    text, err = wr.query_serving_endpoint_chat(
        "claude-opus",
        system_prompt="s",
        user_prompt="u",
        temperature=0.3,
        max_tokens=500,
    )
    assert err is None
    assert text == '{"ok": true}'
    assert client.serving_endpoints.query.call_count == 2
    second = client.serving_endpoints.query.call_args_list[1].kwargs
    assert "temperature" not in second
    assert second.get("max_tokens") == 500


def test_query_serving_endpoint_chat_non_parameter_error_no_retry() -> None:
    client = MagicMock()
    client.serving_endpoints.query.side_effect = Exception("403 forbidden")
    wr = WorkspaceResources(client=client)

    text, err = wr.query_serving_endpoint_chat(
        "x",
        system_prompt="s",
        user_prompt="u",
        temperature=0.1,
        max_tokens=100,
    )
    assert text is None
    assert err is not None
    assert "403" in err
    assert client.serving_endpoints.query.call_count == 1


def test_query_serving_endpoint_chat_omits_max_tokens_on_rejection() -> None:
    client = MagicMock()
    client.serving_endpoints.query.side_effect = [
        Exception("max_tokens is not supported for this model"),
        _resp_with_content("done"),
    ]
    wr = WorkspaceResources(client=client)

    text, err = wr.query_serving_endpoint_chat(
        "ep",
        system_prompt="s",
        user_prompt="u",
        temperature=None,
        max_tokens=4096,
    )
    assert err is None
    assert text == "done"
    assert client.serving_endpoints.query.call_count == 2
    second = client.serving_endpoints.query.call_args_list[1].kwargs
    assert "max_tokens" not in second
