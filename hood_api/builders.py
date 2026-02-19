"""
РЎР±РѕСЂРєР° XML-Р·Р°РїСЂРѕСЃРѕРІ РґР»СЏ РІСЃРµС… СЌРЅРґРїРѕРёРЅС‚РѕРІ Hood API.
РћРґРёРЅ РёСЃС‚РѕС‡РЅРёРє РїСЂР°РІРґС‹: СЃС‚СЂСѓРєС‚СѓСЂР° Р·Р°РїСЂРѕСЃР° Р±РµР· Р»РёС€РЅРёС… РїРѕР»РµР№.
"""

import hashlib
import html
from xml.etree import ElementTree as ET
from typing import Any, Dict, List, Optional

from .config import ApiConfig

DEFAULT_ITEM_MANUFACTURER = "JV moebel"

DEFAULT_PRODUCT_CONTACT_MANUFACTURER = {
    "name": "AEA GmbH & Co. KG",
    "street": "Am Flugplatz 28",
    "zip": "88483",
    "city": "Burgrieden",
    "country2DigitCode": "DE",
    "state": "Baden-WГјrttemberg",
    "phone": "07392-9378440",
    "email": "info@jvmoebel.de",
    "comment": (
        "Eingetragen beim Amtsgericht Ulm, HRA 726335\n"
        "USt-ID: DE327113973\n"
        "WEEE-Reg.-Nr. DE 46974041"
    ),
}

DEFAULT_PRODUCT_CONTACT_RESPONSIBLE_PERSON = {
    "name": "Eugen Krisling",
    "street": "Am Flugplatz 28",
    "zip": "88483",
    "city": "Burgrieden",
    "country2DigitCode": "DE",
    "state": "Baden-WГјrttemberg",
    "phone": "+49 7392 9378425",
    "email": "info@jvmoebel.de",
    "comment": "Verantwortlich fГјr eigene Inhalte der AEA GmbH & Co. KG gem. В§ 55 RStV",
}

DEFAULT_SAFETY_INSTRUCTIONS = [
    "Nicht fГјr Kinder unter 3 Jahren geeignet.",
    "Benutzung unter unmittelbarer Aufsicht von Erwachsenen.",
    (
        "Um mГ¶gliche Verletzungen durch Verheddern zu verhindern, ist dieses "
        "Spielzeug zu entfernen, sobald das Kind zu krabbeln beginnt."
    ),
    "Nur fuer den Hausgebrauch.",
]


def _escape_text(text: str) -> str:
    """Р­РєСЂР°РЅРёСЂРѕРІР°РЅРёРµ РґР»СЏ XML (API Hood РјРѕР¶РµС‚ РЅРµ РѕР±СЂР°Р±Р°С‚С‹РІР°С‚СЊ CDATA)."""
    if not text:
        return ""
    return html.escape(str(text).strip(), quote=True)


def _password_hash(password: str) -> str:
    """MD5-С…СЌС€ РїР°СЂРѕР»СЏ РІ РІРёРґРµ hex (РєР°Рє РІ СЃС‚Р°СЂС‹С… СЃРєСЂРёРїС‚Р°С…). API Hood РѕР¶РёРґР°РµС‚ РёРјРµРЅРЅРѕ hex."""
    if not password:
        return ""
    if password.startswith("hash("):
        return password[5:-1]  # СѓР±СЂР°С‚СЊ РѕР±С‘СЂС‚РєСѓ hash(...)
    return hashlib.md5(password.encode()).hexdigest()


def _api_head(config: ApiConfig, function: str) -> str:
    ph = _password_hash(config.password)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<api type="public" version="2.0.1" user="{config.user}" password="{ph}">
    <function>{function}</function>
    <accountName>{config.user}</accountName>
    <accountPass>{ph}</accountPass>"""


def _elem(parent: ET.Element, tag: str, text: str | None = None) -> None:
    """Р”РѕР±Р°РІР»СЏРµС‚ РґРѕС‡РµСЂРЅРёР№ СЌР»РµРјРµРЅС‚ СЃ С‚РµРєСЃС‚РѕРј (ElementTree СЃР°Рј СЌРєСЂР°РЅРёСЂСѓРµС‚)."""
    child = ET.SubElement(parent, tag)
    if text is not None and text != "":
        child.text = str(text).strip()


def _safe_cdata(s: str) -> str:
    """РўРµРєСЃС‚ РґР»СЏ CDATA: СЂР°Р·Р±РёС‚СЊ ]]> С‡С‚РѕР±С‹ РЅРµ Р»РѕРјР°С‚СЊ СЃРµРєС†РёСЋ."""
    if not s:
        return ""
    return str(s).strip().replace("]]>", "]]]]><![CDATA[>")


def _build_default_product_contact_information_xml() -> str:
    manufacturer = DEFAULT_PRODUCT_CONTACT_MANUFACTURER
    responsible = DEFAULT_PRODUCT_CONTACT_RESPONSIBLE_PERSON
    return (
        "<productContactInformation>"
        "<manufacturer>"
        f"<name><![CDATA[{_safe_cdata(manufacturer['name'])}]]></name>"
        f"<street>{_escape_text(manufacturer['street'])}</street>"
        f"<zip>{_escape_text(manufacturer['zip'])}</zip>"
        f"<city>{_escape_text(manufacturer['city'])}</city>"
        f"<country2DigitCode>{_escape_text(manufacturer['country2DigitCode'])}</country2DigitCode>"
        f"<state>{_escape_text(manufacturer['state'])}</state>"
        f"<phone>{_escape_text(manufacturer['phone'])}</phone>"
        f"<email>{_escape_text(manufacturer['email'])}</email>"
        f"<comment><![CDATA[{_safe_cdata(manufacturer['comment'])}]]></comment>"
        "</manufacturer>"
        "<responsiblePerson>"
        f"<name>{_escape_text(responsible['name'])}</name>"
        f"<street>{_escape_text(responsible['street'])}</street>"
        f"<zip>{_escape_text(responsible['zip'])}</zip>"
        f"<city>{_escape_text(responsible['city'])}</city>"
        f"<country2DigitCode>{_escape_text(responsible['country2DigitCode'])}</country2DigitCode>"
        f"<state>{_escape_text(responsible['state'])}</state>"
        f"<phone>{_escape_text(responsible['phone'])}</phone>"
        f"<email>{_escape_text(responsible['email'])}</email>"
        f"<comment><![CDATA[{_safe_cdata(responsible['comment'])}]]></comment>"
        "</responsiblePerson>"
        "</productContactInformation>"
    )


def _build_default_safety_instructions_xml() -> str:
    instructions = "".join(
        f"<safetyInstruction>{_escape_text(text)}</safetyInstruction>"
        for text in DEFAULT_SAFETY_INSTRUCTIONS
    )
    return f"<safetyInstructions>{instructions}</safetyInstructions>"


def _build_item_insert_or_validate(
    reference_id: str,
    title: str,
    description: str,
    price: str,
    quantity: int,
    category_id: str,
    condition: str,
    item_mode: str,
    pay_options: List[str],
    ship_methods: List[Dict[str, Any]],
    image_urls: List[str],
    product_properties: Optional[List[Dict[str, Any]]],
    ean: Optional[str],
    mpn: Optional[str],
    item_number: Optional[str],
    config: ApiConfig,
    function_name: str,
    item_number_unique_flag: int = 1,
) -> str:
    """РћР±С‰Р°СЏ СЃР±РѕСЂРєР° XML РґР»СЏ itemInsert Рё itemValidate (Hood API Doc 2.0.1: С‚Р° Р¶Рµ СЃС‚СЂСѓРєС‚СѓСЂР°)."""
    ph = _password_hash(config.password)
    pay_opts = pay_options or ["paypal"]
    ship_list = ship_methods or [{"name": "DHLPacket", "country": "nat", "value": "5.99"}]
    title_ok = (title or "").strip()
    desc_ok = (description or "").strip()
    price_ok = str(price or "").strip()
    qty_ok = str(int(quantity)) if quantity is not None else "1"
    cat_ok = str(category_id or "").strip()
    cond_ok = (condition or "new").strip()
    mode_ok = (item_mode or "classic").strip()

    pay_xml = "".join(f"<option>{o}</option>" for o in pay_opts)
    ship_xml = "".join(
        f'<shipmethod name="{m.get("name", "DHLPacket")}_{m.get("country", "nat")}"><value>{m.get("value", "0")}</value></shipmethod>'
        for m in ship_list
    )
    images_xml = "".join(f"<imageURL>{html.escape((u or '').strip())}</imageURL>" for u in (image_urls or []))
    ean_ok = (ean or "").strip()
    mpn_ok = (mpn or "").strip()
    item_number_ok = (item_number or "").strip()

    properties_xml_parts = []
    for prop in (product_properties or []):
        prop_name = str(prop.get("name", "")).strip()
        prop_value = prop.get("value")
        if not prop_name or prop_value is None:
            continue
        if isinstance(prop_value, list):
            prop_value = ", ".join(str(v).strip() for v in prop_value if str(v).strip())
        else:
            prop_value = str(prop_value).strip()
        if not prop_value:
            continue
        properties_xml_parts.append(
            f"<nameValueList><name><![CDATA[{_safe_cdata(prop_name)}]]></name><value><![CDATA[{_safe_cdata(prop_value)}]]></value></nameValueList>"
        )
    product_properties_xml = "".join(properties_xml_parts)

    # Hood API: РґР»СЏ itemInsert РїРѕР»Рµ РЅР°Р·РІР°РЅРёСЏ С‚РѕРІР°СЂР° РЅР°Р·С‹РІР°РµС‚СЃСЏ itemName (РќР• title)
    # РЎС‚СЂСѓРєС‚СѓСЂР° РїРѕР»СЏ С‚РѕРІР°СЂР° РєР°Рє РІ СЂР°Р±РѕС‡РµРј СЃРєСЂРёРїС‚Рµ: Рё startPrice, Рё price.
    item_lines = [
        f"<referenceID>{html.escape(str(reference_id))}</referenceID>",
        f"<itemName>{_escape_text(title_ok)}</itemName>",
        f"<description><![CDATA[{_safe_cdata(desc_ok)}]]></description>",
        f"<startPrice>{price_ok}</startPrice>",
        f"<price>{price_ok}</price>",
        f"<quantity>{qty_ok}</quantity>",
        f"<categoryID>{html.escape(cat_ok)}</categoryID>",
        f"<condition>{html.escape(cond_ok)}</condition>",
        f"<itemMode>{html.escape(mode_ok)}</itemMode>",
        f"<payOptions>{pay_xml}</payOptions>",
        f"<shipMethods>{ship_xml}</shipMethods>",
    ]
    if item_number_ok:
        item_lines.append(f"<itemNumber>{_escape_text(item_number_ok)}</itemNumber>")
    if mpn_ok:
        item_lines.append(f"<mpn>{_escape_text(mpn_ok)}</mpn>")
    if ean_ok:
        item_lines.append(f"<ean>{_escape_text(ean_ok)}</ean>")
    item_lines.append(f"<manufacturer>{_escape_text(DEFAULT_ITEM_MANUFACTURER)}</manufacturer>")
    item_lines.append(_build_default_product_contact_information_xml())
    item_lines.append(_build_default_safety_instructions_xml())
    if images_xml:
        item_lines.append(f"<images>{images_xml}</images>")
    if product_properties_xml:
        item_lines.append(f"<productProperties>{product_properties_xml}</productProperties>")
    item_lines.append(f"<itemNumberUniqueFlag>{item_number_unique_flag}</itemNumberUniqueFlag>")
    item_body = "\n        ".join(item_lines)

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<api type="public" version="2.0.1" user="{html.escape(config.user)}" password="{ph}">\n  '
        f"<function>{function_name}</function>\n  "
        f"<items>\n    <item>\n        {item_body}\n    </item>\n  </items>\n  "
        f"<accountName>{html.escape(config.user)}</accountName>\n  "
        f"<accountPass>{ph}</accountPass>\n"
        "</api>"
    )


def build_item_insert(
    reference_id: str,
    title: str,
    description: str,
    price: str,
    quantity: int,
    category_id: str,
    condition: str,
    item_mode: str,
    pay_options: List[str],
    ship_methods: List[Dict[str, Any]],
    image_urls: List[str],
    product_properties: Optional[List[Dict[str, Any]]] = None,
    ean: Optional[str] = None,
    mpn: Optional[str] = None,
    item_number: Optional[str] = None,
    config: ApiConfig | None = None,
    item_number_unique_flag: int = 1,
) -> str:
    """itemInsert: РґРѕР±Р°РІР»РµРЅРёРµ РѕРґРЅРѕРіРѕ С‚РѕРІР°СЂР° (Hood API Doc 2.2)."""
    config = config or ApiConfig.from_env()
    return _build_item_insert_or_validate(
        reference_id, title, description, price, quantity, category_id,
        condition, item_mode, pay_options, ship_methods, image_urls,
        product_properties, ean, mpn, item_number,
        config, "itemInsert", item_number_unique_flag,
    )


def build_item_validate(
    reference_id: str,
    title: str,
    description: str,
    price: str,
    quantity: int,
    category_id: str,
    condition: str,
    item_mode: str,
    pay_options: List[str],
    ship_methods: List[Dict[str, Any]],
    image_urls: List[str],
    product_properties: Optional[List[Dict[str, Any]]] = None,
    ean: Optional[str] = None,
    mpn: Optional[str] = None,
    item_number: Optional[str] = None,
    config: ApiConfig | None = None,
) -> str:
    """itemValidate: РїСЂРѕРІРµСЂРєР° XML Р±РµР· РґРѕР±Р°РІР»РµРЅРёСЏ С‚РѕРІР°СЂР°, РІРѕР·РІСЂР°С‰Р°РµС‚ СЃС‚РѕРёРјРѕСЃС‚СЊ (Hood API Doc 2.1)."""
    config = config or ApiConfig.from_env()
    return _build_item_insert_or_validate(
        reference_id, title, description, price, quantity, category_id,
        condition, item_mode, pay_options, ship_methods, image_urls,
        product_properties, ean, mpn, item_number,
        config, "itemValidate", 1,
    )


def build_item_detail(item_id: str, config: ApiConfig | None = None) -> str:
    """itemDetail: function, accountName, accountPass, items/item/itemID (Р±РµР· РЅРёС… API РІРѕР·РІСЂР°С‰Р°РµС‚ globalError)."""
    config = config or ApiConfig.from_env()
    ph = _password_hash(config.password)
    api = ET.Element("api", type="public", version="2.0.1", user=config.user, password=ph)
    ET.SubElement(api, "function").text = "itemDetail"
    ET.SubElement(api, "accountName").text = config.user
    ET.SubElement(api, "accountPass").text = ph
    items_el = ET.SubElement(api, "items")
    item_el = ET.SubElement(items_el, "item")
    ET.SubElement(item_el, "itemID").text = str(item_id).strip()
    out = ET.tostring(api, encoding="unicode", method="xml", default_namespace="")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + out


def build_item_list(item_status: str, start_at: int, group_size: int,
                    start_date: Optional[str] = None, end_date: Optional[str] = None,
                    config: ApiConfig = None) -> str:
    """itemList: СЃРїРёСЃРѕРє С‚РѕРІР°СЂРѕРІ."""
    config = config or ApiConfig.from_env()
    date_xml = ""
    if start_date and end_date:
        date_xml = f"""
    <dateRange>
        <startDate>{start_date}</startDate>
        <endDate>{end_date}</endDate>
    </dateRange>"""
    return f"""{_api_head(config, "itemList")}
    <itemStatus>{item_status}</itemStatus>
    <startAt>{start_at}</startAt>
    <groupSize>{group_size}</groupSize>{date_xml}
</api>"""


def build_item_status(item_id: str, detail_level: str = "image", config: ApiConfig = None) -> str:
    """itemStatus: СЃС‚Р°С‚СѓСЃ С‚РѕРІР°СЂР°."""
    config = config or ApiConfig.from_env()
    return f"""{_api_head(config, "itemStatus")}
    <detailLevel>{detail_level}</detailLevel>
    <items>
        <item><itemID>{item_id}</itemID></item>
    </items>
</api>"""


def build_item_delete(items: List[Dict[str, Any]], config: ApiConfig | None = None) -> str:
    """
    itemDelete: удаление товаров по itemNumber.
    items: список словарей с ключом itemNumber/item_number.
    """
    config = config or ApiConfig.from_env()
    parts = []
    for it in items:
        item_number = it.get("itemNumber") or it.get("item_number")
        if not item_number:
            continue
        parts.append(f"<item><itemNumber>{_escape_text(str(item_number))}</itemNumber></item>")
    items_xml = "\n        ".join(parts) if parts else ""
    return f"""{_api_head(config, "itemDelete")}
    <items>
        {items_xml}
    </items>
</api>"""


def build_item_update(items: List[Dict[str, Any]], config: ApiConfig | None = None) -> str:
    """itemUpdate: РѕР±РЅРѕРІР»РµРЅРёРµ РґРѕ 5 С‚РѕРІР°СЂРѕРІ. РљР°Р¶РґС‹Р№ item: itemID + РѕРїС†РёРѕРЅР°Р»СЊРЅРѕ title, description, price, quantity, categoryID, condition, itemMode, pay_options, ship_methods, images."""
    config = config or ApiConfig.from_env()
    parts = []
    for it in items:
        item_id = it.get("itemID", "")
        pay_opts = it.get("pay_options", ["paypal"])
        ship = it.get("ship_methods", {"DHLsmallPacket_net": "5.99"})
        imgs = it.get("images", [])
        pay_xml = "".join(f"<option>{o}</option>" for o in pay_opts)
        ship_xml = "".join(f'<shipmethod name="{k}"><value>{v}</value></shipmethod>' for k, v in ship.items())
        img_xml = "".join(f"<imageURL>{u}</imageURL>" for u in imgs)
        lines = [f"<itemID>{item_id}</itemID>"]
        if it.get("title") is not None:
            lines.append(f"<title>{_escape_text(str(it['title']))}</title>")
        if it.get("description") is not None:
            lines.append(f"<description><![CDATA[{_safe_cdata(str(it['description']))}]]></description>")
        if it.get("price") is not None:
            lines.append(f"<price>{it['price']}</price>")
        if it.get("quantity") is not None:
            lines.append(f"<quantity>{it['quantity']}</quantity>")
        if it.get("categoryID") is not None:
            lines.append(f"<categoryID>{it['categoryID']}</categoryID>")
        if it.get("condition") is not None:
            lines.append(f"<condition>{it['condition']}</condition>")
        if it.get("itemMode") is not None:
            lines.append(f"<itemMode>{it['itemMode']}</itemMode>")
        if pay_xml:
            lines.append(f"<payOptions>{pay_xml}</payOptions>")
        if ship_xml:
            lines.append(f"<shipMethods>{ship_xml}</shipMethods>")
        if img_xml:
            lines.append(f"<images>{img_xml}</images>")
        parts.append("\n            ".join(["<item>", *lines, "</item>"]))
    items_xml = "\n        ".join(parts)
    return f"""{_api_head(config, "itemUpdate")}
    <items>
        {items_xml}
    </items>
</api>"""


def build_order_list(start_date: str, end_date: str, list_mode: str = "details",
                     order_id: Optional[str] = None, config: ApiConfig = None) -> str:
    """orderList: СЃРїРёСЃРѕРє Р·Р°РєР°Р·РѕРІ."""
    config = config or ApiConfig.from_env()
    extra = f"\n    <listMode>{list_mode}</listMode>" if list_mode else ""
    if order_id:
        extra += f"\n    <orderID>{order_id}</orderID>"
    return f"""{_api_head(config, "orderList")}
    <dateRange>
        <type>orderDate</type>
        <startDate>{start_date}</startDate>
        <endDate>{end_date}</endDate>
    </dateRange>{extra}
</api>"""


def build_update_order_status(orders: List[Dict[str, Any]], config: ApiConfig | None = None) -> str:
    """updateOrderStatus: orderID, statusAction; РѕРїС†РёРѕРЅР°Р»СЊРЅРѕ trackingCode, carrier, messageText."""
    config = config or ApiConfig.from_env()
    parts = []
    for o in orders:
        block = f"<orderID>{o['orderID']}</orderID>\n        <statusAction>{o['statusAction']}</statusAction>"
        if o.get("trackingCode"):
            block += f"\n        <trackingCode>{o['trackingCode']}</trackingCode>"
        if o.get("carrier"):
            block += f"\n        <carrier>{o['carrier']}</carrier>"
        if o.get("messageText"):
            block += f"\n        <messageText><![CDATA[{o['messageText']}]]></messageText>"
        parts.append(f"<order>\n        {block}\n    </order>")
    return f"""{_api_head(config, "updateOrderStatus")}
    <orders>
    {"".join(parts)}
    </orders>
</api>"""


def build_rate_buyer(orders: List[Dict[str, Any]], config: ApiConfig | None = None) -> str:
    """rateBuyer: orderID, rating (positive/neutral/negative), ratingText."""
    config = config or ApiConfig.from_env()
    parts = []
    for o in orders:
        parts.append(f"""    <order>
        <orderID>{o['orderID']}</orderID>
        <rating>{o['rating']}</rating>
        <ratingText><![CDATA[{o.get('ratingText', '')}]]></ratingText>
    </order>""")
    return f"""{_api_head(config, "rateBuyer")}
    <orders>
{"".join(parts)}
    </orders>
</api>"""


def build_categories_browse(category_id: str = "0", config: ApiConfig = None) -> str:
    """categoriesBrowse: РєР°С‚РµРіРѕСЂРёРё Hood (0 = РєРѕСЂРµРЅСЊ)."""
    config = config or ApiConfig.from_env()
    return f"""{_api_head(config, "categoriesBrowse")}
    <categoryID>{category_id}</categoryID>
</api>"""


def build_shop_categories_list(config: ApiConfig = None) -> str:
    """shopCategoriesList."""
    config = config or ApiConfig.from_env()
    return f"""{_api_head(config, "shopCategoriesList")}
</api>"""


def build_shop_categories_insert(parent_id: str, category_name: str, config: ApiConfig = None) -> str:
    """shopCategoriesInsert."""
    config = config or ApiConfig.from_env()
    return f"""{_api_head(config, "shopCategoriesInsert")}
    <parentID>{parent_id}</parentID>
    <prodCatName><![CDATA[{category_name}]]></prodCatName>
</api>"""


def build_shop_categories_update(category_id: str, category_name: str, config: ApiConfig = None) -> str:
    """shopCategoriesUpdate."""
    config = config or ApiConfig.from_env()
    return f"""{_api_head(config, "shopCategoriesUpdate")}
    <prodCatID>{category_id}</prodCatID>
    <prodCatName><![CDATA[{category_name}]]></prodCatName>
</api>"""


def build_shop_categories_delete(category_id: str, config: ApiConfig = None) -> str:
    """shopCategoriesDelete."""
    config = config or ApiConfig.from_env()
    return f"""{_api_head(config, "shopCategoriesDelete")}
    <prodCatID>{category_id}</prodCatID>
</api>"""
