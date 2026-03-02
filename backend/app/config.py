import os
from dataclasses import dataclass
from pathlib import Path


# Корень backend (папка, где лежат app/, data/, docker/)
BACKEND_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_FOLDER = BACKEND_ROOT / "logs"
DOTENV_PATH = BACKEND_ROOT / ".env"


def _load_dotenv_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        os.environ.setdefault(key, value)


_load_dotenv_file(DOTENV_PATH)


def _resolve_path(value: str, allow_empty: bool = False) -> str:
    raw = (value or "").strip()
    if not raw:
        return "" if allow_empty else str(BACKEND_ROOT)

    path = Path(raw)
    if path.is_absolute():
        return str(path.resolve())

    candidates = [
        (BACKEND_ROOT / path).resolve(),
        (BACKEND_ROOT.parent / path).resolve(),
        (Path.cwd() / path).resolve(),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    return str(candidates[0])


@dataclass
class Settings:
    """
    Простой конфиг без pydantic.
    Значения можно переопределять через переменные окружения.
    """

    JSON_FOLDER: str = _resolve_path(os.getenv("JSON_FOLDER", ""), allow_empty=True)
    LOG_FOLDER: str = _resolve_path(os.getenv("LOG_FOLDER", str(DEFAULT_LOG_FOLDER)))
    PRICE_SHEET_PATH: str = _resolve_path(os.getenv("PRICE_SHEET_PATH", ""), allow_empty=True)
    HTML_DESCRIPTIONS_FOLDER: str = _resolve_path(
        os.getenv("HTML_DESCRIPTIONS_FOLDER", ""),
        allow_empty=True,
    )
    CSV_FOLDER: str = _resolve_path(os.getenv("CSV_FOLDER", "backend/csv"))
    FACEBOOK_FEED_TOKEN: str = os.getenv("FACEBOOK_FEED_TOKEN", "")
    FACEBOOK_DEFAULT_BRAND: str = os.getenv("FACEBOOK_DEFAULT_BRAND", "")
    FACEBOOK_DEFAULT_CURRENCY: str = os.getenv("FACEBOOK_DEFAULT_CURRENCY", "EUR")
    FACEBOOK_PRODUCT_LINK_BASE: str = os.getenv("FACEBOOK_PRODUCT_LINK_BASE", "")

    DEBUG: bool = os.getenv("DEBUG", "0") in ("1", "true", "True")
    MAX_PARALLEL_UPLOADS: int = int(os.getenv("MAX_PARALLEL_UPLOADS", "5"))


settings = Settings()


def normalize_account_name(account: str | None) -> str | None:
    mode = (account or "").strip().lower()
    if not mode:
        return None
    if mode in ("xlmoebel", "xl"):
        return "xlmoebel"
    if mode in ("jvmoebel", "jv"):
        return "jvmoebel"
    raise ValueError("account must be one of: xlmoebel, jvmoebel")


def get_json_folder_for_account(account: str | None) -> str:
    mode = normalize_account_name(account)
    if mode == "xlmoebel":
        return _resolve_path(
            os.getenv("JSON_FOLDER_XL", "/var/lib/productbaseapi/data/XL/XL_LISTER/XL_NEW/JSON")
        )
    if mode == "jvmoebel":
        return _resolve_path(
            os.getenv("JSON_FOLDER_JV", "/var/lib/productbaseapi/data/JV/JV_LISTER/JV_NEW/JSON")
        )
    return settings.JSON_FOLDER


def get_html_folder_for_account(account: str | None) -> str:
    mode = normalize_account_name(account)
    if mode == "xlmoebel":
        return _resolve_path(
            os.getenv("HTML_DESCRIPTIONS_FOLDER_XL", "/var/lib/productbaseapi/data/XL/XL_PRODUCT/XL_NEW/HTML")
        )
    if mode == "jvmoebel":
        return _resolve_path(
            os.getenv("HTML_DESCRIPTIONS_FOLDER_JV", "/var/lib/productbaseapi/data/JV/JV_PRODUCT/JV_NEW/HTML")
        )
    return settings.HTML_DESCRIPTIONS_FOLDER


def get_price_sheet_for_account(account: str | None) -> str:
    mode = normalize_account_name(account)
    if mode == "xlmoebel":
        return _resolve_path(os.getenv("PRICE_SHEET_PATH_XL", ""), allow_empty=True)
    if mode == "jvmoebel":
        return _resolve_path(os.getenv("PRICE_SHEET_PATH_JV", ""), allow_empty=True)
    return settings.PRICE_SHEET_PATH


def get_csv_folder_for_account(account: str | None) -> str:
    mode = normalize_account_name(account)
    if mode == "xlmoebel":
        return _resolve_path(
            os.getenv("CSV_FOLDER_XL", "/var/lib/productbaseapi/data/XL/XL_LISTER/XL_NEW/csv")
        )
    if mode == "jvmoebel":
        return _resolve_path(
            os.getenv("CSV_FOLDER_JV", "/var/lib/productbaseapi/data/JV/JV_LISTER/JV_NEW/csv")
        )
    return settings.CSV_FOLDER

