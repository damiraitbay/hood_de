"""Единый HTTP-клиент для запросов к Hood API."""

import os
import re
import time

import requests

from .config import ApiConfig


def send_request(xml_body: str, config: ApiConfig | None = None) -> str:
    """
    Отправляет XML-запрос к Hood API и возвращает ответ как строку.
    Для устойчивости на больших партиях использует configurable timeout + retries.
    """
    if os.environ.get("HOOD_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        masked = re.sub(r'password="[^"]*"', 'password="***"', xml_body)
        print("--- Запрос (password=***) ---\n", masked, "\n---", flush=True)

    cfg = config or ApiConfig.from_env()
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "User-Agent": "HoodApiClient/1.0",
    }

    connect_timeout = float(os.environ.get("HOOD_API_CONNECT_TIMEOUT_SECONDS", "30"))
    read_timeout = float(os.environ.get("HOOD_API_TIMEOUT_SECONDS", "300"))
    max_retries = int(os.environ.get("HOOD_API_MAX_RETRIES", "3"))
    base_backoff = float(os.environ.get("HOOD_API_RETRY_BACKOFF_SECONDS", "2"))

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                cfg.base_url,
                data=xml_body.encode("utf-8"),
                headers=headers,
                timeout=(connect_timeout, read_timeout),
            )
            response.raise_for_status()
            return response.text
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else 0
            # Retry only transient statuses.
            if status_code not in (429, 500, 502, 503, 504):
                raise
            last_exc = exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc

        if attempt >= max_retries:
            break
        time.sleep(base_backoff * (2 ** (attempt - 1)))

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Hood API request failed without exception details")
