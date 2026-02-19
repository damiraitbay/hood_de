"""Единые настройки для Hood API. Берутся из переменных окружения."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiConfig:
    """Настройки подключения к Hood API."""
    base_url: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "ApiConfig":
        base_url = os.environ.get("HOOD_API_URL", "https://www.hood.de/api.htm").strip()
        user = os.environ.get("HOOD_API_USER", "").strip()
        password = os.environ.get("HOOD_API_PASSWORD", "").strip()
        if not user or not password:
            raise ValueError("HOOD_API_USER and HOOD_API_PASSWORD must be set")

        return cls(
            base_url=base_url,
            user=user,
            password=password,
        )
