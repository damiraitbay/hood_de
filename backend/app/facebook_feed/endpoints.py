import csv
import io
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.config import get_csv_folder_for_account, normalize_account_name, settings

router = APIRouter()

FACEBOOK_HEADERS = [
    "id",
    "title",
    "description",
    "availability",
    "condition",
    "price",
    "link",
    "image_link",
    "brand",
    "google_product_category",
    "fb_product_category",
    "quantity_to_sell_on_facebook",
    "sale_price",
    "sale_price_effective_date",
    "item_group_id",
    "gender",
    "color",
    "size",
    "age_group",
    "material",
    "pattern",
    "shipping",
    "shipping_weight",
    "video[0].url",
    "video[0].tag[0]",
    "gtin",
    "product_tags[0]",
    "product_tags[1]",
    "style[0]",
]

CURRENCY_MAP = {
    "7": "EUR",
}


def _account_mode(account: str | None) -> str | None:
    try:
        return normalize_account_name(account)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def _read_text_with_fallback(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Cannot read CSV file {path.name}: {exc}")
    raise HTTPException(status_code=400, detail=f"Unsupported encoding in CSV file: {path.name}")


def _repair_source_text(text: str) -> str:
    fixed = text.replace("\r\n", "\n").replace("\r", "\n")
    # Some exports arrive concatenated with ">" instead of a newline between rows.
    fixed = re.sub(r"(?<=[A-Za-z0-9/])>\s*\"(?=[^\"]+\";\")", ">\n\"", fixed)
    return fixed


def _normalize_key(value: Any) -> str:
    return str(value or "").strip().strip('"').lower()


def _normalize_value(value: Any) -> str:
    return str(value or "").strip().strip('"')


def _first_non_empty(normalized: Dict[str, str], keys: Iterable[str]) -> str:
    for key in keys:
        value = normalized.get(key)
        if value:
            return value
    return ""


def _extract_gtin_like(normalized: Dict[str, str]) -> str:
    # Support common source header variants: ean, ean_code, gtin, barcode, etc.
    raw = _first_non_empty(
        normalized,
        (
            "ean",
            "ean_code",
            "ean code",
            "ean13",
            "ean-13",
            "gtin",
            "gtin13",
            "gtin14",
            "barcode",
            "bar code",
            "upc",
        ),
    )
    if not raw:
        return ""
    digits = re.sub(r"\D+", "", raw)
    # Typical accepted GTIN lengths.
    if len(digits) in (8, 12, 13, 14):
        return digits
    return ""


def _to_decimal(value: Any) -> float:
    raw = _normalize_value(value)
    if not raw:
        return 0.0
    raw = raw.replace(" ", "")
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _to_int(value: Any, default: int = 0) -> int:
    raw = _normalize_value(value)
    if not raw:
        return default
    try:
        return int(float(raw.replace(",", ".")))
    except ValueError:
        return default


def _resolve_currency(raw_currency: Any) -> str:
    currency = _normalize_value(raw_currency).upper()
    if not currency:
        return settings.FACEBOOK_DEFAULT_CURRENCY
    if currency.isalpha() and len(currency) == 3:
        return currency
    return CURRENCY_MAP.get(currency, settings.FACEBOOK_DEFAULT_CURRENCY)


def _build_product_link(product_id: str) -> str:
    base = (settings.FACEBOOK_PRODUCT_LINK_BASE or "").strip()
    if not base:
        return ""
    return f"{base.rstrip('/')}/{quote(product_id)}"


def _split_image_urls(raw_value: Any) -> List[str]:
    raw = _normalize_value(raw_value)
    if not raw:
        return []
    parts = re.split(r"[|,\s]+", raw)
    result: List[str] = []
    seen: set[str] = set()
    for part in parts:
        url = part.strip()
        if not url or not re.match(r"^https?://", url, flags=re.IGNORECASE):
            continue
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def _compact_text(raw_value: Any, fallback: str = "") -> str:
    text = _normalize_value(raw_value) or fallback
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 5000:
        return text[:5000]
    return text


def _normalize_row(row: Dict[str, Any], fallback_id: str) -> Dict[str, str]:
    normalized = {_normalize_key(k): _normalize_value(v) for k, v in row.items() if k is not None}

    title = normalized.get("artikelbeschreibung") or normalized.get("title") or normalized.get("name")
    if not title:
        title = f"Product {fallback_id}"
    title = _compact_text(title, fallback=f"Product {fallback_id}")

    quantity = _to_int(normalized.get("menge"), default=1)
    availability = "in stock" if quantity > 0 else "out of stock"

    buy_now_price = _to_decimal(normalized.get("sofortkaufenpreis"))
    start_price = _to_decimal(normalized.get("startpreis"))
    amount = buy_now_price if buy_now_price > 0 else start_price
    sale_amount = start_price if buy_now_price > 0 and start_price > buy_now_price else 0.0

    currency = _resolve_currency(normalized.get("currency"))
    price = f"{amount:.2f} {currency}"
    sale_price = f"{sale_amount:.2f} {currency}" if sale_amount > 0 else ""

    gtin = _extract_gtin_like(normalized)

    # Facebook feed requires stable product ids; prefer GTIN/EAN when available.
    product_id = (
        gtin
        or normalized.get("id")
        or normalized.get("itemnumber")
        or normalized.get("item_number")
        or fallback_id
    )

    image_candidates = (
        _split_image_urls(normalized.get("pictureurl"))
        + _split_image_urls(normalized.get("galleryurl"))
        + _split_image_urls(normalized.get("image_link"))
    )
    image_link = image_candidates[0] if image_candidates else ""

    description = _compact_text(normalized.get("description"), fallback=title)
    brand = normalized.get("marke") or settings.FACEBOOK_DEFAULT_BRAND
    item_group_id = normalized.get("variantgroup") or normalized.get("item_group_id") or ""
    category_id = normalized.get("categoryid") or ""
    color = normalized.get("farbe") or ""
    size = normalized.get("groesse") or normalized.get("größe") or normalized.get("size") or ""
    material = normalized.get("material") or ""
    pattern = normalized.get("muster") or ""
    style = normalized.get("stil") or normalized.get("style") or ""
    shipping_weight = normalized.get("shippingweight") or normalized.get("gewicht") or ""
    quantity_to_sell = str(quantity) if quantity > 0 else ""

    return {
        "id": product_id,
        "title": title,
        "description": description,
        "availability": availability,
        "condition": "new",
        "price": price,
        "link": _build_product_link(product_id),
        "image_link": image_link,
        "brand": brand,
        "google_product_category": "",
        "fb_product_category": "",
        "quantity_to_sell_on_facebook": quantity_to_sell,
        "sale_price": sale_price,
        "sale_price_effective_date": "",
        "item_group_id": item_group_id,
        "gender": "",
        "color": color,
        "size": size,
        "age_group": "",
        "material": material,
        "pattern": pattern,
        "shipping": "",
        "shipping_weight": shipping_weight,
        "video[0].url": "",
        "video[0].tag[0]": "",
        "gtin": gtin or "",
        "product_tags[0]": category_id,
        "product_tags[1]": "",
        "style[0]": style,
    }


def _parse_csv_file(path: Path) -> List[Dict[str, str]]:
    source_text = _repair_source_text(_read_text_with_fallback(path))
    reader = csv.DictReader(io.StringIO(source_text), delimiter=";", quotechar='"')

    if not reader.fieldnames:
        return []

    rows: List[Dict[str, str]] = []
    for index, row in enumerate(reader, start=1):
        if not row:
            continue
        if not any(_normalize_value(v) for v in row.values()):
            continue
        fallback_id = f"{path.stem}-{index}"
        rows.append(_normalize_row(row, fallback_id=fallback_id))
    return rows


def _resolve_csv_files(csv_folder: Path, source_file: str | None = None) -> List[Path]:
    if source_file:
        file_name = Path(source_file).name
        if file_name != source_file or not file_name.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="source_file must be a plain .csv file name")
        file_path = csv_folder / file_name
        if not file_path.exists() or not file_path.is_file():
            raise HTTPException(status_code=404, detail=f"CSV file not found: {file_name}")
        return [file_path]

    files = sorted(csv_folder.glob("*.csv"))
    if not files:
        raise HTTPException(status_code=404, detail=f"No CSV files found in folder: {csv_folder}")
    return files


def _build_feed_csv(rows: Iterable[Dict[str, str]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=FACEBOOK_HEADERS, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in FACEBOOK_HEADERS})
    return buffer.getvalue()


@router.get("/catalog.csv")
def facebook_catalog_feed(
    account: str | None = Query(default=None),
    token: str | None = Query(default=None),
    source_file: str | None = Query(default=None),
) -> Response:
    account_mode = _account_mode(account)

    expected_token = (settings.FACEBOOK_FEED_TOKEN or "").strip()
    if expected_token and token != expected_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    csv_folder = Path(get_csv_folder_for_account(account_mode))
    if not csv_folder.exists() or not csv_folder.is_dir():
        raise HTTPException(status_code=404, detail=f"CSV folder not found: {csv_folder}")

    files = _resolve_csv_files(csv_folder=csv_folder, source_file=source_file)

    all_rows: List[Dict[str, str]] = []
    for file in files:
        all_rows.extend(_parse_csv_file(file))

    if not all_rows:
        raise HTTPException(status_code=404, detail="No valid rows found in CSV files")

    feed = _build_feed_csv(all_rows)
    headers = {
        "Content-Disposition": "inline; filename=facebook_catalog.csv",
        "Cache-Control": "no-store",
    }
    return Response(content=feed, media_type="text/csv; charset=utf-8", headers=headers)
