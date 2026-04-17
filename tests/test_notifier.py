import pytest
import httpx
from unittest.mock import patch, MagicMock
from src.notifier import Notifier


def test_send_disabled_does_not_call_http():
    n = Notifier(webhook_url="https://example.com", enabled=False)
    with patch("httpx.post") as mock_post:
        n.send("test message")
        mock_post.assert_not_called()


def test_send_enabled_posts_correct_payload():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.post", return_value=mock_response) as mock_post:
        n.send("同步完成：成功 5 天")
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        payload = call_kwargs["json"]
        assert payload["msgtype"] == "text"
        assert "同步完成" in payload["text"]["content"]
        assert "[zer0share]" in payload["text"]["content"]


def test_send_request_error_does_not_raise():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    with patch("httpx.post", side_effect=httpx.RequestError("network error", request=MagicMock())):
        n.send("告警消息")  # 不应抛出异常


def test_send_http_error_does_not_raise():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "400 Bad Request", request=MagicMock(), response=MagicMock(status_code=400)
    )
    with patch("httpx.post", return_value=mock_response):
        n.send("告警消息")  # 不应抛出异常
