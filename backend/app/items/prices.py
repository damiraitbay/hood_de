import csv
from pathlib import Path

from app.config import settings
from app.logger import get_logger

logger = get_logger("prices")


def load_prices():
    sheet_path_raw = settings.PRICE_SHEET_PATH.strip()
    if not sheet_path_raw:
        raise ValueError("PRICE_SHEET_PATH is not configured")
    sheet_path = Path(sheet_path_raw)
    if not sheet_path.exists() or not sheet_path.is_file():
        raise FileNotFoundError(f"Price sheet not found: {sheet_path}")

    prices = {}
    with sheet_path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            prices[row["EAN"]] = float(row["Price"])
    return prices
