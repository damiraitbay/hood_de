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
    def from_env(cls, account: str | None = None) -> "ApiConfig":
        base_url = os.environ.get("HOOD_API_URL", "https://www.hood.de/api.htm").strip()
        mode = (account or "").strip().lower()

        if mode in ("xlmoebel", "xl"):
            user = os.environ.get("HOOD_API_XLUSER", "").strip()
            password = os.environ.get("HOOD_API_XLPASSWORD", "").strip()
            if not user or not password:
                raise ValueError("HOOD_API_XLUSER and HOOD_API_XLPASSWORD must be set")
        elif mode in ("jvmoebel", "jv"):
            user = os.environ.get("HOOD_API_JVUSER", "").strip()
            # Keep both names for compatibility with existing env naming.
            password = os.environ.get("HOOD_API_JSPASSWORD", "").strip() or os.environ.get("HOOD_API_JVPASSWORD", "").strip()
            if not user or not password:
                raise ValueError("HOOD_API_JVUSER and HOOD_API_JSPASSWORD must be set")
        else:
            user = os.environ.get("HOOD_API_USER", "").strip()
            password = os.environ.get("HOOD_API_PASSWORD", "").strip()
            if not user or not password:
                raise ValueError("HOOD_API_USER and HOOD_API_PASSWORD must be set")

        return cls(
            base_url=base_url,
            user=user,
            password=password,
        )
