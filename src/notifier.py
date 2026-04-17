import httpx
from loguru import logger


class Notifier:
    def __init__(self, webhook_url: str, enabled: bool):
        self._url = webhook_url
        self._enabled = enabled

    def send(self, message: str) -> None:
        if not self._enabled:
            return
        payload = {
            "msgtype": "text",
            "text": {"content": f"[zer0share] {message}"}
        }
        try:
            resp = httpx.post(self._url, json=payload, timeout=10)
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"企业微信推送失败: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"企业微信返回错误: {e.response.status_code}")
