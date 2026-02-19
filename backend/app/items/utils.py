from typing import Any, Dict


# Ровно тот же список категорий, что и в твоём скрипте
CATEGORIES = [
    {"category_id": "2412", "category_name": "Sonstige"},
    {"category_id": "22210", "category_name": "Sonstiges"},
    {"category_id": "29625", "category_name": "Nachhaltiges Gärtnern"},
    {"category_id": "20802", "category_name": "Sonstige"},
    {"category_id": "29618", "category_name": "Nachhaltige Kosmetik"},
    {"category_id": "3921", "category_name": "Sammlungen & Pakete"},
    {"category_id": "3940", "category_name": "Sonstige"},
    {"category_id": "4231", "category_name": "Sonstige"},
    {"category_id": "4722", "category_name": "Sonstiges"},
    {"category_id": "5305", "category_name": "Notebooks"},
    {"category_id": "5378", "category_name": "PC-Systeme"},
    {"category_id": "5209", "category_name": "Monitore"},
    {"category_id": "5187", "category_name": "Computer-Klassiker"},
    {"category_id": "29622", "category_name": "Refurbished Laptops"},
    {"category_id": "5484", "category_name": "Sonstige"},
    {"category_id": "6489", "category_name": "Digitalkameras"},
    {"category_id": "22041", "category_name": "Digitale Camcorder"},
    {"category_id": "22048", "category_name": "Sonstige"},
    {"category_id": "14389", "category_name": "Sonstige"},
    {"category_id": "6943", "category_name": "Handys, Smartphones ohne Vertrag"},
]

PROPERTY_EXCLUDE_EXACT = {
    "__source_file__",
    "ID",
    "id",
    "reference_id",
    "item_name",
    "Name",
    "TITLE",
    "title",
    "Artikelbeschreibung",
    "Description",
    "DESC",
    "description",
    "TranslatedDescription",
    "Currency",
    "Startpreis",
    "Price",
    "price",
    "Menge",
    "Anzahl der Einheiten",
    "Quantity",
    "quantity",
    "qty",
    "SofortkaufenPreis",
    "CategoryID",
    "category_id",
    "Category2ID",
    "ConditionID",
    "Typ",
    "Dauer",
    "SiteID",
    "Country",
    "Location",
    "ZIP",
    "Region",
    "ShippingOption",
    "ShipToLocations",
    "EAN",
    "ean",
    "GTIN / EAN",
    "GTIN / EAN:",
    "item_number",
    "ItemNumber",
    "MPN",
    "Zustand",
    "Zustand:",
    "Hersteller Nr.",
    "Hersteller Nr.:",
    "Herstellernummer",
    "PictureURL",
    "GalleryURL",
    "pictureurls",
    "UUID",
    "Fabric",
}

PROPERTY_EXCLUDE_PREFIXES = (
    "Pay_",
    "Ship_",
    "Use",
    "Gallery",
    "Reserve",
    "SecOffer",
    "Auff",
    "Discount",
    "QuantityRelationship",
    "PaymentProfile",
    "ReturnPolicy",
    "ShippingProfile",
    "ShippingDiscount",
    "InternationalShippingDiscount",
    "Motors",
    "Packstation",
    "Ebay",
    "Hazard",
    "Safety",
    "EconomicOperator",
    "CrossSelling",
    "TitleBar",
    "QuickCheckout",
    "NowAndNew",
    "BusinessSeller",
    "RestrictedToBusiness",
    "ListAvailable",
    "TransferCurr",
    "DispatchTime",
    "Widerruf",
    "SubAccount",
    "Var",
    "ProPack",
    "Repair",
    "DisableUse",
    "ItemBorder",
    "Checkout",
)

PROPERTY_NAME_ALIASES = {
    "L\u0413\u00A4nge": "L?nge",
    "L\u0420\u201C\u0412\u00A4nge": "L?nge",
    "Länge:": "Länge",
    "H\u0413\u00B6he": "H?he",
    "H\u0420\u201C\u0412\u00B6he": "H?he",
    "Höhe:": "Höhe",
    "Herstellergarantie:": "Herstellergarantie",
    "Zimmer:": "Zimmer",
    "Abteilung:": "Abteilung",
    "Breite:": "Breite",
    "Muster:": "Muster",
    "Herstellungsjahr:": "Herstellungsjahr",
    "Verpackung:": "Verpackung",
    "Marke:": "Marke",
    "Farbe:": "Farbe",
    "Produktart:": "Produktart",
}


def closest_category(item_name: str) -> str:
    """
    Берём ближайшую категорию из твоего списка,
    как в примере скрипта.
    """
    item_lower = item_name.lower()
    for c in CATEGORIES:
        words = c["category_name"].split()
        if any(word.lower() in item_lower for word in words):
            return c["category_id"]
    # fallback — первая категория из списка
    return CATEGORIES[0]["category_id"]


def normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводит запись товара из JSON к единому формату для работы с Hood API.

    ВАЖНО: при необходимости скорректируй поля под реальную структуру своих JSON‑файлов.
    Сейчас предполагается примерно такой набор ключей:
      - ID          — внутренний ID товара
      - EAN         — штрихкод
      - Name / TITLE / title — название
      - Description / DESC   — описание
      - Price / price        — цена
      - Quantity / qty       — количество
      - CategoryID / category_id — категория
    """
    internal_id = raw.get("ID") or raw.get("id")
    reference_id = raw.get("reference_id") or (f"ART{internal_id}" if internal_id is not None else None)

    # ????????
    item_name = (
        raw.get("Artikelbeschreibung")
        or raw.get("Name")
        or raw.get("TITLE")
        or raw.get("title")
        or raw.get("item_name")
        or (f"Item {internal_id}" if internal_id is not None else "")
    )

    # Описание: избегаем плейсхолдера "<-StammBeschreibung->"
    desc_raw = (
        raw.get("Description")
        or raw.get("DESC")
        or raw.get("description")
        or ""
    )
    if not desc_raw or "<-StammBeschreibung->" in str(desc_raw):
        desc_raw = str(item_name)
    description = str(desc_raw).strip()
    # Подстрахуемся по длине описания (требование Hood: описание не слишком короткое)
    if len(description) < 80:
        description = (
            description
            + "\n\nAusführliche Produktbeschreibung folgt. "
              "Alle wichtigen Details entnehmen Sie bitte den Artikelbildern und technischen Daten."
        )

    # Цена: как в твоём скрипте — напрямую из Startpreis,
    # без дополнительного форматирования/передёргивания.
    price = raw.get("Startpreis", "0.00")
    price_str = str(price)

    # Количество: сначала Menge / "Anzahl der Einheiten", потом прочие поля
    quantity = (
        raw.get("Menge")
        or raw.get("Anzahl der Einheiten")
        or raw.get("Quantity")
        or raw.get("quantity")
        or raw.get("qty")
        or 1
    )
    try:
        quantity_int = int(quantity)
    except (TypeError, ValueError):
        quantity_int = 1

    original_category = str(raw.get("CategoryID", ""))

    # Как в примере: жёстко "new" и "shopProduct"
    condition = "new"
    item_mode = "shopProduct"

    # ???????????: ?????????? pictureurls (??????) ??? PictureURL (????)
    images = []
    if isinstance(raw.get("pictureurls"), list):
        images = [str(u) for u in raw["pictureurls"] if u]
    elif raw.get("PictureURL"):
        images = [str(raw["PictureURL"])]

    # CategoryID: если исходный не из твоего списка — подбираем ближайший
    valid_ids = {c["category_id"] for c in CATEGORIES}
    if not original_category or original_category not in valid_ids:
        category = closest_category(str(item_name))
    else:
        category = original_category

    def first_present(*keys: str) -> Any:
        for key in keys:
            if key in raw and raw.get(key) not in (None, ""):
                return raw.get(key)
        return None

    ean = first_present("EAN", "ean", "GTIN / EAN", "GTIN / EAN:")
    mpn = (
        first_present("Herstellernummer", "Hersteller Nr.", "Hersteller Nr.:", "MPN")
        or (f"JVM{ean}" if ean else "")
    )
    item_number = first_present("item_number", "ItemNumber") or (str(ean) if ean else "")
    zustand = first_present("Zustand", "Zustand:")
    if not zustand and str(raw.get("ConditionID", "")).strip() == "1000":
        zustand = "Neu"

    # productProperties: отправляем все характеристики конкретного товара,
    # кроме служебных/технических полей.
    def is_property_key(key: str) -> bool:
        if not key or key in PROPERTY_EXCLUDE_EXACT:
            return False
        if key.startswith("__"):
            return False
        return not any(key.startswith(prefix) for prefix in PROPERTY_EXCLUDE_PREFIXES)

    def normalize_property_name(key: str) -> str:
        key = str(key).strip()
        key = PROPERTY_NAME_ALIASES.get(key, key)
        if key.endswith(":"):
            key = key[:-1].strip()
        return key

    def normalize_property_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return ", ".join(str(v).strip() for v in value if str(v).strip())
        if isinstance(value, str):
            return str(value).strip()
        return ""

    property_key_pairs = []
    for raw_key, raw_value in raw.items():
        raw_key = str(raw_key)
        if not is_property_key(raw_key):
            continue
        value = normalize_property_value(raw_value)
        if not value:
            continue
        prop_name = normalize_property_name(raw_key)
        if not prop_name:
            continue
        property_key_pairs.append((prop_name, value))
    product_properties = []
    seen_names = set()
    for prop_name, value in property_key_pairs:
        if prop_name in seen_names:
            continue
        if not value:
            continue
        product_properties.append({"name": prop_name, "value": value})
        seen_names.add(prop_name)

    return {
        "reference_id": reference_id or "",
        "item_name": str(item_name),
        "description": str(description),
        "price": price_str,
        "quantity": quantity_int,
        "category_id": str(category),
        "condition": str(condition),
        "item_mode": str(item_mode),
        "ean": ean,
        "mpn": str(mpn).strip(),
        "item_number": str(item_number).strip(),
        "product_properties": product_properties,
        "image_urls": images,
        "raw": raw,
    }
