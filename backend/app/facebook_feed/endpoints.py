import csv
import io
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
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
    "additional_image_link",
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


def _extract_gtin_from_item_specifics(raw: str) -> str:
    if not raw:
        return ""

    # Example source fragment:
    # <Name><![CDATA[EAN]]></Name><Value><![CDATA[4069424130232]]></Value>
    patterns = [
        r"<Name><!\[CDATA\[EAN\]\]></Name>\s*<Value><!\[CDATA\[(\d{8}|\d{12}|\d{13}|\d{14})\]\]></Value>",
        r"<Name>\s*EAN\s*</Name>\s*<Value>\s*(\d{8}|\d{12}|\d{13}|\d{14})\s*</Value>",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return match.group(1)
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


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _process_uvp(price: float) -> float:
    # Keep UVP brackets identical to hood_api/builders.py.
    if price > 5000:
        value = price * 1.10
    elif 2500 <= price <= 4999:
        value = price * 1.18
    elif 1000 <= price <= 2499:
        value = price * 1.25
    else:
        value = price * 1.35
    return round(value, 2)


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
    parts = re.split(r"[|,;\s]+", raw)
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


def _dedupe_urls(urls: Iterable[str]) -> List[str]:
    result: List[str] = []
    seen: set[str] = set()
    for url in urls:
        key = (url or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _compact_text(raw_value: Any, fallback: str = "") -> str:
    text = _normalize_value(raw_value) or fallback
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 5000:
        return text[:5000]
    return text


def _clean_source_description(raw_value: Any) -> str:
    text = _normalize_value(raw_value)
    if not text:
        return ""
    text = re.sub(r"%0d%0a|%0a|%0d", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<-?\s*stammbeschreibung\s*->", "", text, flags=re.IGNORECASE)
    return _compact_text(text)


def _parse_item_specifics(raw_value: str) -> List[Tuple[str, str]]:
    if not raw_value:
        return []

    result: List[Tuple[str, str]] = []
    blocks = re.findall(r"<NameValueList>(.*?)</NameValueList>", raw_value, flags=re.IGNORECASE | re.DOTALL)
    for block in blocks:
        name_match = re.search(
            r"<Name><!\[CDATA\[(.*?)\]\]></Name>|<Name>\s*(.*?)\s*</Name>",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not name_match:
            continue
        name = _compact_text(name_match.group(1) or name_match.group(2))
        if not name:
            continue

        values = re.findall(
            r"<Value><!\[CDATA\[(.*?)\]\]></Value>|<Value>\s*(.*?)\s*</Value>",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        cleaned_values: List[str] = []
        seen: set[str] = set()
        for left, right in values:
            value = _compact_text(left or right)
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            cleaned_values.append(value)

        if not cleaned_values:
            continue

        result.append((name, ", ".join(cleaned_values)))

    return result


def _build_specs_index(specs: List[Tuple[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name, value in specs:
        key = name.lower().strip()
        if key and value and key not in out:
            out[key] = value
    return out


def _spec_value(specs_index: Dict[str, str], keys: Iterable[str]) -> str:
    for key in keys:
        value = specs_index.get(str(key).lower().strip())
        if value:
            return value
    return ""


def _normalize_gender(raw_value: str) -> str:
    text = str(raw_value or "").strip().lower()
    if not text:
        return ""
    if any(token in text for token in ("female", "frau", "women", "damen", "weiblich")):
        return "female"
    if any(token in text for token in ("male", "mann", "men", "herren")):
        return "male"
    if any(token in text for token in ("unisex", "erwachsene", "adult")):
        return "unisex"
    return ""


def _normalize_age_group(raw_value: str) -> str:
    text = str(raw_value or "").strip().lower()
    if not text:
        return ""
    if any(token in text for token in ("adult", "erwachsene")):
        return "adult"
    if any(token in text for token in ("all ages", "alle")):
        return "all ages"
    if any(token in text for token in ("infant", "baby")):
        return "infant"
    if "newborn" in text:
        return "newborn"
    if "toddler" in text:
        return "toddler"
    if any(token in text for token in ("kids", "kinder")):
        return "kids"
    if any(token in text for token in ("teen", "jugend")):
        return "teen"
    return ""


def _build_description_from_specs(normalized: Dict[str, str], title: str, specs: List[Tuple[str, str]]) -> str:
    base_description = _clean_source_description(normalized.get("description", ""))
    if not specs:
        return _compact_text(base_description, fallback=title)

    by_name = {name.lower(): (name, value) for name, value in specs}
    preferred = [
        "marke",
        "produktart",
        "farbe",
        "material",
        "zimmer",
        "stil",
        "breite",
        "länge",
        "höhe",
        "ean",
    ]

    selected: List[Tuple[str, str]] = []
    used: set[str] = set()
    for key in preferred:
        if key in by_name:
            selected.append(by_name[key])
            used.add(key)

    for name, value in specs:
        key = name.lower()
        if key in used:
            continue
        selected.append((name, value))
        used.add(key)
        if len(selected) >= 8:
            break

    parts: List[str] = []
    if base_description and base_description.lower() != title.lower():
        parts.append(base_description)
    for name, value in selected[:8]:
        parts.append(f"{name}: {value}")

    return _compact_text(" | ".join(parts), fallback=title)


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
    uvp_amount = _process_uvp(amount) if amount > 0 else 0.0
    price_amount = uvp_amount if uvp_amount > amount else amount
    sale_amount = amount if uvp_amount > amount else 0.0

    currency = _resolve_currency(normalized.get("currency"))
    price = f"{price_amount:.2f} {currency}"
    sale_price = f"{sale_amount:.2f} {currency}" if sale_amount > 0 else ""

    gtin = _extract_gtin_like(normalized)
    if not gtin:
        gtin = _extract_gtin_from_item_specifics(
            normalized.get("customitemspecifics", "") or normalized.get("translateddescription", "")
        )
    raw_specs = normalized.get("customitemspecifics", "") or normalized.get("translateddescription", "")
    specs = _parse_item_specifics(raw_specs)
    specs_index = _build_specs_index(specs)

    # Facebook feed requires stable product ids; prefer GTIN/EAN when available.
    product_id = (
        gtin
        or normalized.get("id")
        or normalized.get("itemnumber")
        or normalized.get("item_number")
        or fallback_id
    )

    image_candidates = _dedupe_urls(
        _split_image_urls(normalized.get("pictureurl"))
        + _split_image_urls(normalized.get("pictureurls"))
        + _split_image_urls(normalized.get("galleryurl"))
        + _split_image_urls(normalized.get("image_link"))
    )
    image_link = image_candidates[0] if image_candidates else ""
    additional_image_link = ",".join(image_candidates[1:]) if len(image_candidates) > 1 else ""

    description = _build_description_from_specs(normalized, title=title, specs=specs)
    brand = (
        normalized.get("marke")
        or normalized.get("brand")
        or _spec_value(specs_index, ("marke", "brand"))
        or settings.FACEBOOK_DEFAULT_BRAND
    )
    item_group_id = ""
    category_id = normalized.get("categoryid") or ""
    color = normalized.get("farbe") or normalized.get("color") or _spec_value(specs_index, ("farbe", "color"))
    size = (
        normalized.get("groesse")
        or normalized.get("größe")
        or normalized.get("size")
        or _spec_value(specs_index, ("groesse", "größe", "size", "liegeflaeche", "liegefläche"))
    )
    material = normalized.get("material") or _spec_value(specs_index, ("material",))
    pattern = normalized.get("muster") or normalized.get("pattern") or _spec_value(specs_index, ("muster", "pattern"))
    style = normalized.get("stil") or normalized.get("style") or _spec_value(specs_index, ("stil", "style"))
    shipping_weight = (
        normalized.get("shippingweight")
        or normalized.get("gewicht")
        or _spec_value(specs_index, ("gewicht", "versandgewicht", "weight", "shipping weight"))
    )
    gender = _normalize_gender(
        normalized.get("gender") or _spec_value(specs_index, ("geschlecht", "gender", "abteilung"))
    )
    age_group = _normalize_age_group(
        normalized.get("age_group") or _spec_value(specs_index, ("altersgruppe", "age group", "abteilung"))
    )
    google_product_category = (
        normalized.get("google_product_category")
        or normalized.get("googleproductcategory")
        or _spec_value(specs_index, ("produktart", "produkttyp", "kategorie"))
    )
    fb_product_category = (
        normalized.get("fb_product_category")
        or normalized.get("facebook_product_category")
        or _spec_value(specs_index, ("produktart", "kategorie"))
    )
    video_url = normalized.get("video[0].url") or normalized.get("videourl") or normalized.get("video_url") or ""
    video_tag = normalized.get("video[0].tag[0]") or normalized.get("videotag") or ""
    second_tag = normalized.get("category2id") or normalized.get("shopcat2") or normalized.get("kollektion") or ""
    shipping_cost = _to_decimal(normalized.get("ship_shippinghandlingcosts") or normalized.get("ship_shippingrate"))
    shipping = ""
    if shipping_cost > 0:
        shipping = f"DE:::{shipping_cost:.2f} {currency}"
    elif _is_truthy(normalized.get("ship_sellerpays")):
        shipping = f"DE:::0.00 {currency}"
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
        "additional_image_link": additional_image_link,
        "brand": brand,
        "google_product_category": google_product_category,
        "fb_product_category": fb_product_category,
        "quantity_to_sell_on_facebook": quantity_to_sell,
        "sale_price": sale_price,
        "sale_price_effective_date": "",
        "item_group_id": item_group_id,
        "gender": gender,
        "color": color,
        "size": size,
        "age_group": age_group,
        "material": material,
        "pattern": pattern,
        "shipping": shipping,
        "shipping_weight": shipping_weight,
        "video[0].url": video_url,
        "video[0].tag[0]": video_tag,
        "gtin": gtin or "",
        "product_tags[0]": category_id,
        "product_tags[1]": second_tag,
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
