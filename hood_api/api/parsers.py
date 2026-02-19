"""
Парсеры XML-ответов Hood API.
Преобразуют сырой XML в структурированные данные (dict/list) для использования на сайте.
"""

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional


def _text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return (el.text or "").strip()


def _find_text(root: ET.Element, tag: str, default: str = "") -> str:
    child = root.find(tag)
    return _text(child) if child is not None else default


def parse_generic_response(xml_str: str) -> Dict[str, Any]:
    """
    Базовый парсер: статус, сообщение, ошибки.
    Возвращает dict: status, message, errors (список), success.
    """
    root = ET.fromstring(xml_str)
    status = _find_text(root, "status", "unknown")
    message = _find_text(root, "message")
    errors: List[str] = []
    for err in root.findall(".//error"):
        msg = _text(err) or (err.get("message") or "")
        if msg:
            errors.append(msg)
    return {
        "status": status,
        "message": message or None,
        "errors": errors,
        "success": status.lower() in ("success", "ok", "1"),
    }


def parse_item_insert_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ itemInsert и itemValidate: referenceID, status, itemID, cost, message (Hood API Doc)."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    item = root.find(".//item")
    if item is not None:
        data["item_id"] = _find_text(item, "itemID")
        data["reference_id"] = _find_text(item, "referenceID")
        data["item_status"] = _find_text(item, "status")
        data["item_message"] = _find_text(item, "message") or None
        cost_el = item.find("cost")
        if cost_el is not None and cost_el.text:
            try:
                data["cost"] = float(cost_el.text.strip())
            except ValueError:
                data["cost"] = None
        else:
            data["cost"] = None
        if data.get("item_status") == "success":
            data["success"] = True
        elif data.get("item_status") == "failed" and data.get("item_message"):
            data["errors"] = data["errors"] or [data["item_message"]]
    return data


def parse_item_delete_response(xml_str: str) -> Dict[str, Any]:
    """
    Парсит ответ itemDelete.
    У Hood часто нет глобального <status>, поэтому учитываем статусы в <items>/<item>.
    """
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)

    item_results: List[Dict[str, Any]] = []
    item_statuses: List[str] = []
    for item in root.findall(".//item"):
        item_number = _find_text(item, "itemNumber")
        item_id = _find_text(item, "itemID")
        item_status = _find_text(item, "status")
        item_message = _find_text(item, "message")
        if item_status:
            item_statuses.append(item_status.lower())
        if item_number or item_id or item_status or item_message:
            item_results.append(
                {
                    "item_number": item_number or None,
                    "item_id": item_id or None,
                    "status": item_status or None,
                    "message": item_message or None,
                }
            )

    if item_results:
        data["items"] = item_results

    if "success" in item_statuses and "failed" not in item_statuses:
        data["success"] = True
    elif "failed" in item_statuses:
        data["success"] = False
    elif not data["errors"]:
        # Если явных ошибок нет, а API не прислал глобальный статус, считаем успехом.
        data["success"] = True

    return data


def _item_element_to_dict(item: ET.Element) -> Dict[str, Any]:
    """Один элемент <item> в словарь."""
    out: Dict[str, Any] = {}
    simple = (
        "itemID", "referenceID", "title", "description", "startPrice", "price",
        "quantity", "categoryID", "condition", "itemMode", "itemStatus",
        "itemNumber", "itemNumberUniqueFlag",
    )
    for tag in simple:
        val = _find_text(item, tag)
        if val:
            if tag in ("startPrice", "price", "quantity") and val:
                try:
                    out[tag] = float(val) if "." in val else int(val)
                except ValueError:
                    out[tag] = val
            else:
                out[tag] = val
    if "title" not in out:
        item_name = _find_text(item, "itemName")
        if item_name:
            out["title"] = item_name
    images_el = item.find("images")
    if images_el is not None:
        urls = [_text(img) for img in images_el.findall("imageURL") if _text(img)]
        if urls:
            out["images"] = urls
    ship_el = item.find("shipMethods")
    if ship_el is not None:
        methods = []
        for m in ship_el.findall("shipmethod"):
            name = m.get("name", "")
            val = _text(m) or _find_text(m, "value")
            if name or val:
                methods.append({"name": name, "value": val})
        if methods:
            out["shipMethods"] = methods
    props_el = item.find("productProperties")
    if props_el is not None:
        props: List[Dict[str, str]] = []
        for pair in props_el.findall("nameValueList"):
            name = _find_text(pair, "name")
            value = _find_text(pair, "value")
            if not name and not value:
                continue
            props.append({"name": name, "value": value})
        if props:
            out["productProperties"] = props
    return out


def parse_item_detail_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ itemDetail: данные по одному товару."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    items_data: List[Dict[str, Any]] = []
    for item in root.findall(".//item"):
        items_data.append(_item_element_to_dict(item))
    data["items"] = items_data
    return data


def parse_item_list_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ itemList: список товаров, totalRecords, startAt, groupSize."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    data["total_records"] = int(_find_text(root, "totalRecords") or "0")
    data["start_at"] = int(_find_text(root, "startAt") or "0")
    data["group_size"] = int(_find_text(root, "groupSize") or "0")
    items_data: List[Dict[str, Any]] = []
    for item in root.findall(".//item"):
        items_data.append(_item_element_to_dict(item))
    data["items"] = items_data
    if not data["errors"] and (len(items_data) > 0 or data["total_records"] > 0):
        data["success"] = True
    return data


def parse_item_status_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ itemStatus."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    items_data: List[Dict[str, Any]] = []
    for item in root.findall(".//item"):
        items_data.append(_item_element_to_dict(item))
    data["items"] = items_data
    return data


def _order_element_to_dict(order_el: ET.Element) -> Dict[str, Any]:
    """Один <order> в словарь."""
    out: Dict[str, Any] = {}
    details = order_el.find("orderDetails")
    if details is not None:
        out["order_id"] = _find_text(details, "orderID")
        out["order_date"] = _find_text(details, "orderDate")
        out["total_price"] = _find_text(details, "totalPrice")
        out["total_quantity"] = _find_text(details, "totalQuantity")
        out["buyer_status"] = _find_text(details, "buyerStatus")
        out["seller_status"] = _find_text(details, "sellerStatus")
        out["shipping_cost"] = _find_text(details, "shippingCost")
        out["shipping_method"] = _find_text(details, "shippingMethod")
    buyer = order_el.find("buyer")
    if buyer is not None:
        out["buyer"] = {
            "account_name": _find_text(buyer, "accountName"),
            "email": _find_text(buyer, "email"),
            "first_name": _find_text(buyer, "firstName"),
            "last_name": _find_text(buyer, "lastName"),
            "address": _find_text(buyer, "address"),
            "city": _find_text(buyer, "city"),
            "zip_code": _find_text(buyer, "zipCode"),
            "country": _find_text(buyer, "country"),
        }
    items_el = order_el.find("orderItems") or order_el.find("items")
    if items_el is not None:
        out["items"] = []
        for it in items_el.findall("item"):
            out["items"].append({
                "item_id": _find_text(it, "itemID"),
                "product_name": _find_text(it, "productName"),
                "quantity": _find_text(it, "quantity"),
                "price": _find_text(it, "price"),
            })
    return out


def parse_order_list_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ orderList."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    orders: List[Dict[str, Any]] = []
    for order_el in root.findall(".//order"):
        orders.append(_order_element_to_dict(order_el))
    data["orders"] = orders
    return data


def parse_update_order_status_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ updateOrderStatus."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    results: List[Dict[str, Any]] = []
    for order in root.findall(".//order"):
        results.append({
            "order_id": _find_text(order, "orderID"),
            "status_action": _find_text(order, "statusAction"),
            "tracking_code": _find_text(order, "trackingCode"),
            "carrier": _find_text(order, "carrier"),
        })
    data["orders"] = results
    return data


def parse_rate_buyer_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ rateBuyer."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    results: List[Dict[str, Any]] = []
    for order in root.findall(".//order"):
        results.append({
            "order_id": _find_text(order, "orderID"),
            "rating": _find_text(order, "rating"),
        })
    data["orders"] = results
    return data


def parse_categories_browse_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ categoriesBrowse."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    categories: List[Dict[str, Any]] = []
    for c in root.findall(".//category"):
        categories.append({
            "category_id": _find_text(c, "categoryID"),
            "category_name": _find_text(c, "categoryName"),
            "parent_id": _find_text(c, "parentID"),
        })
    data["categories"] = categories
    return data


def parse_shop_categories_list_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ shopCategoriesList."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    categories: List[Dict[str, Any]] = []
    for c in root.findall(".//shopCategory") or root.findall(".//category"):
        categories.append({
            "category_id": _find_text(c, "prodCatID") or _find_text(c, "categoryID"),
            "category_name": _find_text(c, "prodCatName") or _find_text(c, "categoryName"),
            "parent_id": _find_text(c, "parentID"),
        })
    data["categories"] = categories
    return data


def parse_shop_category_mutation_response(xml_str: str) -> Dict[str, Any]:
    """Парсит ответ shopCategoriesInsert/Update/Delete."""
    data = parse_generic_response(xml_str)
    root = ET.fromstring(xml_str)
    data["category_id"] = _find_text(root, "prodCatID")
    data["category_name"] = _find_text(root, "prodCatName")
    return data
