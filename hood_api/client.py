"""Единый HTTP-клиент для запросов к Hood API."""

import os
import re
import requests

from .config import ApiConfig


def send_request(xml_body: str, config: ApiConfig | None = None) -> str:
    """
    Отправляет XML-запрос к Hood API и возвращает ответ как строку.
    Hood API Doc 2.0.1: Content-Type должен быть text/xml; charset=UTF-8.
    User-Agent задан, чтобы избежать 403.
    """
    if os.environ.get("HOOD_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        masked = re.sub(r'password="[^"]*"', 'password="***"', xml_body)
        print("--- Запрос (password=***) ---\n", masked, "\n---", flush=True)
    cfg = config or ApiConfig.from_env()
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "User-Agent": "HoodApiClient/1.0",
    }
    response = requests.post(
        cfg.base_url,
        data=xml_body.encode("utf-8"),
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    return response.text
