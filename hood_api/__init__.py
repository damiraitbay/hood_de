"""Пакет Hood API: конфиг, отправка запросов, сборка XML и парсинг ответов."""

from .config import ApiConfig
from .client import send_request

__all__ = [
    "ApiConfig",
    "send_request",
]
