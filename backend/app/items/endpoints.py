import asyncio
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException, Query

from app.config import settings
from app.logger import get_logger
from hood_api.config import ApiConfig
from hood_api.client import send_request
from hood_api.builders import (
    build_item_delete,
    build_item_insert,
    build_item_list,
    build_item_update,
    build_item_validate,
)
from hood_api.api.parsers import (
    parse_generic_response,
    parse_item_delete_response,
    parse_item_insert_response,
    parse_item_list_response,
)
from app.items.prices import load_prices
from app.items.storage import (
    list_json_source_files,
    load_all_items,
    load_items_from_source_file,
)
from app.items.utils import normalize_item

router = APIRouter()
logger = get_logger("items")

# Файл, куда будем складывать товары, не загрузившиеся в Hood
FAILED_ITEMS_PATH = Path(settings.LOG_FOLDER).resolve() / "failed_items.json"


def _resolve_description_for_api(norm: Dict[str, Any]) -> str:
    """
    Для API отправляем HTML-описание по EAN, если найден файл <EAN>.html/.htm.
    Если файла нет или чтение не удалось, отправляем обычный description.
    """
    fallback = str(norm.get("description") or "")
    reference_id = str(norm.get("reference_id") or "")
    raw_ean = str(norm.get("ean") or "").strip()
    ean = raw_ean[:-2] if raw_ean.endswith(".0") else raw_ean
    ean = re.sub(r"\s+", "", ean)
    if not ean:
        logger.info(
            "HTML description source: fallback description (reason=no_ean, reference_id=%s)",
            reference_id or "?",
        )
        return fallback

    candidate_dirs = []
    if settings.HTML_DESCRIPTIONS_FOLDER.strip():
        candidate_dirs.append(Path(settings.HTML_DESCRIPTIONS_FOLDER))
    if not candidate_dirs:
        logger.info(
            "HTML description source: fallback description (reason=html_path_not_configured, reference_id=%s, ean=%s)",
            reference_id or "?",
            ean,
        )
        return fallback

    for html_dir in candidate_dirs:
        if not html_dir.exists() or not html_dir.is_dir():
            continue
        for ext in (".html", ".htm"):
            html_file = html_dir / f"{ean}{ext}"
            if not html_file.exists():
                continue
            try:
                html_text = html_file.read_text(encoding="utf-8").strip()
            except UnicodeDecodeError:
                html_text = html_file.read_text(encoding="utf-8-sig", errors="ignore").strip()
            except OSError:
                continue
            if html_text:
                logger.info(
                    "HTML description source: file=%s (reference_id=%s, ean=%s)",
                    html_file,
                    reference_id or "?",
                    ean,
                )
                return html_text

    logger.info(
        "HTML description source: fallback description (reason=file_not_found, reference_id=%s, ean=%s)",
        reference_id or "?",
        ean,
    )
    return fallback


@router.get("/json")
def items_from_json(
    offset: int = 0,
    limit: int = Query(20, ge=1, le=200),
    normalized: bool = False,
    source_file: str | None = Query(default=None),
) -> Dict[str, Any]:
    """
    Вывод товаров из наших локальных JSON (с пагинацией).
    По умолчанию возвращает сырые записи, если normalized=true — возвращает normalize_item().
    """
    try:
        all_items = load_items_from_source_file(source_file) if source_file else load_all_items()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")

    total = len(all_items)
    chunk = all_items[offset : offset + limit]
    items = [normalize_item(x) for x in chunk] if normalized else chunk
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "source_file": source_file,
        "items": items,
    }


@router.get("/json/files")
def json_files() -> Dict[str, Any]:
    try:
        files = list_json_source_files()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"files": files}


@router.get("/lookup/{reference_id}")
def lookup_in_hood(reference_id: str) -> Dict[str, Any]:
    """
    ???? ????? ? Hood ?? referenceID (????????, ART84153326) ? ?????????? itemID ? ?????? ????.
    """
    cfg = ApiConfig.from_env()
    xml_body = build_item_list(
        item_status="running",
        start_at=1,
        group_size=500,
        start_date=None,
        end_date=None,
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    data = parse_item_list_response(response_xml)
    items = data.get("items", [])
    found = [it for it in items if it.get("referenceID") == reference_id]
    if not found:
        raise HTTPException(status_code=404, detail="Item with this reference_id not found in Hood")
    # если несколько — вернём все, но чаще всего будет один
    return {"reference_id": reference_id, "items": found}


def _find_raw_item_by_id(item_id: str, source_file: str | None = None) -> Dict[str, Any] | None:
    source_items = load_items_from_source_file(source_file) if source_file else load_all_items()
    for it in source_items:
        if str(it.get("ID", "")) == str(item_id):
            return it
    return None


def _upload_one_by_id(item_id: str, source_file: str | None = None) -> Dict[str, Any]:
    try:
        raw = _find_raw_item_by_id(item_id, source_file=source_file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")
    if not raw:
        raise HTTPException(status_code=404, detail="Item not found in JSON by ID")

    cfg = ApiConfig.from_env()
    norm = normalize_item(raw)
    api_description = _resolve_description_for_api(norm)

    xml_body = build_item_insert(
        reference_id=norm["reference_id"],
        title=norm["item_name"],
        description=api_description,
        price=norm["price"],
        quantity=norm["quantity"],
        category_id=norm["category_id"],
        condition=norm["condition"],
        item_mode=norm["item_mode"],
        pay_options=["paypal"],
        ship_methods=[{"name": "DHLPacket", "country": "nat", "value": "5.99"}],
        image_urls=norm.get("image_urls") or [],
        product_properties=norm.get("product_properties") or [],
        ean=norm.get("ean"),
        mpn=norm.get("mpn"),
        item_number=norm.get("item_number"),
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    resp = parse_item_insert_response(response_xml)
    resp["reference_id"] = norm["reference_id"]
    resp["item_id_local"] = str(item_id)

    msg = (resp.get("item_message") or "") + " " + " ".join(resp.get("errors") or [])
    if "Sie haben bereits einen identischen Artikel" in msg:
        resp["success"] = True

    return resp


@router.post("/validate_one/{item_id}")
def validate_one(item_id: str, source_file: str | None = Query(default=None)) -> Dict[str, Any]:
    """
    Проверка структуры одного товара по ID из JSON.
    """
    try:
        raw = _find_raw_item_by_id(item_id, source_file=source_file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")
    if not raw:
        raise HTTPException(status_code=404, detail="Item not found in JSON by ID")
    cfg = ApiConfig.from_env()
    norm = normalize_item(raw)
    api_description = _resolve_description_for_api(norm)
    xml_body = build_item_validate(
        reference_id=norm["reference_id"],
        title=norm["item_name"],
        description=api_description,
        price=norm["price"],
        quantity=norm["quantity"],
        category_id=norm["category_id"],
        condition=norm["condition"],
        item_mode=norm["item_mode"],
        pay_options=["paypal"],
        ship_methods=[{"name": "DHLPacket", "country": "nat", "value": "5.99"}],
        image_urls=norm.get("image_urls") or [],
        product_properties=norm.get("product_properties") or [],
        ean=norm.get("ean"),
        mpn=norm.get("mpn"),
        item_number=norm.get("item_number"),
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    resp = parse_item_insert_response(response_xml)
    resp["reference_id"] = norm["reference_id"]
    resp["item_id_local"] = item_id
    return resp


@router.post("/upload_one/{item_id}")
def upload_one(item_id: str, source_file: str | None = Query(default=None)) -> Dict[str, Any]:
    """
    Отправка одного товара по ID из JSON.
    Если Hood вернул 'Sie haben bereits einen identischen Artikel' — считаем успехом и удаляем из JSON.
    """
    return _upload_one_by_id(item_id=item_id, source_file=source_file)


@router.post("/upload_one")
def upload_many(
    item_ids: List[str] = Body(..., embed=True),
    source_file: str | None = Query(default=None),
) -> List[Dict[str, Any]]:
    normalized_ids: List[str] = []
    seen: set[str] = set()
    for raw in item_ids:
        val = str(raw or "").strip()
        if not val or val in seen:
            continue
        normalized_ids.append(val)
        seen.add(val)

    if not normalized_ids:
        raise HTTPException(status_code=400, detail="item_ids is empty")

    results: List[Dict[str, Any]] = []
    for item_id in normalized_ids:
        try:
            resp = _upload_one_by_id(item_id=item_id, source_file=source_file)
        except HTTPException as exc:
            results.append(
                {
                    "item_id_local": item_id,
                    "success": False,
                    "status": exc.status_code,
                    "error": exc.detail,
                }
            )
            continue
        results.append(resp)

    return results


@router.get("/status")
def items_status() -> Dict[str, Any]:
    """
    Список товаров, которые НЕ удалось загрузить в Hood при последнем /items/upload.
    Читаем их из failed_items.json.
    """
    if not FAILED_ITEMS_PATH.exists():
        return {"failed_items": []}

    try:
        data = json.loads(FAILED_ITEMS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Не удалось прочитать failed_items.json: {exc}")

    # Ожидается список нормализованных товаров
    if not isinstance(data, list):
        data = []
    return {"failed_items": data}


@router.post("/validate")
def items_validate() -> List[Dict[str, Any]]:
    """
    Проверка структуры товаров: itemValidate для всех товаров сервера.
    """
    cfg = ApiConfig.from_env()
    server_items = load_all_items()

    results: List[Dict[str, Any]] = []
    for raw in server_items:
        norm = normalize_item(raw)
        api_description = _resolve_description_for_api(norm)
        xml_body = build_item_validate(
            reference_id=norm["reference_id"],
            title=norm["item_name"],
            description=api_description,
            price=norm["price"],
            quantity=norm["quantity"],
            category_id=norm["category_id"],
            condition=norm["condition"],
            item_mode=norm["item_mode"],
            pay_options=["paypal"],
            ship_methods=[{"name": "DHLPacket", "country": "nat", "value": "5.99"}],
            image_urls=norm.get("image_urls") or [],
            product_properties=norm.get("product_properties") or [],
            ean=norm.get("ean"),
            mpn=norm.get("mpn"),
            item_number=norm.get("item_number"),
            config=cfg,
        )
        try:
            response_xml = send_request(xml_body, config=cfg)
        except Exception as exc:
            results.append(
                {
                    "reference_id": norm["reference_id"],
                    "error": str(exc),
                }
            )
            continue
        resp = parse_item_insert_response(response_xml)
        resp["reference_id"] = norm["reference_id"]
        results.append(resp)
    return results


@router.post("/upload")
async def items_upload(
    limit: int = 1,
    source_file: str | None = Query(default=None),
) -> List[Dict[str, Any]]:
    """
    Асинхронная загрузка ВСЕХ товаров из JSON в Hood.
    Если при загрузке товара произошла ошибка, сохраняем его в failed_items.json.
    """
    cfg = ApiConfig.from_env()
    try:
        server_items = load_items_from_source_file(source_file) if source_file else load_all_items()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")
    all_norms: List[Dict[str, Any]] = [normalize_item(raw) for raw in server_items]

    # Пока по умолчанию грузим только первый товар (limit=1).
    # Для массовой загрузки можно будет просто вызвать /items/upload?limit=1000.
    if limit <= 0:
        to_upload: List[Dict[str, Any]] = all_norms
        logger.info(f"Start upload {len(to_upload)} items to Hood (all items)")
    else:
        to_upload = all_norms[:limit]
        logger.info(f"Start upload {len(to_upload)} items to Hood (limit={limit})")

    semaphore = asyncio.Semaphore(getattr(settings, "MAX_PARALLEL_UPLOADS", 5))
    results: List[Dict[str, Any]] = []
    processed_count = 0
    total_count = len(to_upload)

    async def worker(norm: Dict[str, Any]) -> None:
        nonlocal processed_count
        async with semaphore:
            api_description = _resolve_description_for_api(norm)
            xml_body = build_item_insert(
                reference_id=norm["reference_id"],
                title=norm["item_name"],
                description=api_description,
                price=norm["price"],
                quantity=norm["quantity"],
                category_id=norm["category_id"],
                condition=norm["condition"],
                item_mode=norm["item_mode"],
                pay_options=["paypal"],
                ship_methods=[{"name": "DHLPacket", "country": "nat", "value": "5.99"}],
                image_urls=norm.get("image_urls") or [],
                product_properties=norm.get("product_properties") or [],
                ean=norm.get("ean"),
                mpn=norm.get("mpn"),
                item_number=norm.get("item_number"),
                config=cfg,
            )
            try:
                response_xml = await asyncio.to_thread(send_request, xml_body, cfg)
                resp = parse_item_insert_response(response_xml)
                resp["reference_id"] = norm["reference_id"]

                # Специальный случай: товар уже существует в Hood
                msg = (resp.get("item_message") or "") + " " + " ".join(resp.get("errors") or [])
                if "Sie haben bereits einen identischen Artikel" in msg:
                    logger.info(
                        f"≡ {norm['reference_id']} уже есть в Hood (identischer Artikel); "
                        f"itemID={resp.get('item_id', '?')} — удаляем из локального JSON"
                    )
                    # Считаем как успех и удаляем из исходного JSON
                    resp["success"] = True
                elif resp.get("success"):
                    logger.info(
                        f"✓ {norm['reference_id']} загружен успешно; "
                        f"itemID={resp.get('item_id', '?')}"
                    )
                else:
                    logger.warning(f"✗ {norm['reference_id']} не загружен: {resp.get('item_message', 'unknown error')}")
            except Exception as exc:
                resp = {
                    "reference_id": norm["reference_id"],
                    "success": False,
                    "error": str(exc),
                }
                logger.error(f"✗ Ошибка загрузки товара {norm['reference_id']}: {exc}")

            results.append(resp)
            processed_count += 1
            
            # Логируем прогресс каждые 10 товаров или на каждом 10-м, 20-м, 30-м и т.д.
            if processed_count % 10 == 0 or processed_count == total_count:
                logger.info(f"Прогресс: {processed_count}/{total_count} товаров обработано ({processed_count * 100 // total_count}%)")

    tasks = [worker(it) for it in to_upload]
    if tasks:
        await asyncio.gather(*tasks)

    # Собираем все товары, которые не удалось загрузить, и сохраняем
    # ?????? ? ??????? ?????? Hood (status, errors, item_message, reference_id ? ?.?.)
    failed_items: List[Dict[str, Any]] = [r for r in results if not r.get("success")]

    FAILED_ITEMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    FAILED_ITEMS_PATH.write_text(json.dumps(failed_items, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        f"Загрузка завершена. Успешно: {len(results) - len(failed_items)}, "
        f"с ошибками: {len(failed_items)}. Файл с ошибочными товарами: {FAILED_ITEMS_PATH}"
    )

    return results


@router.delete("/delete/by-item-number/{item_number}")
def delete_item_by_item_number(item_number: str) -> Dict[str, Any]:
    cfg = ApiConfig.from_env()
    xml_delete = build_item_delete(items=[{"itemNumber": item_number}], config=cfg)
    try:
        delete_resp_xml = send_request(xml_delete, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    resp = parse_item_delete_response(delete_resp_xml)
    resp["item_number"] = item_number
    return resp


@router.post("/delete/by-item-number")
def delete_items_by_item_number(item_numbers: List[str] = Body(..., embed=True)) -> Dict[str, Any]:
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in item_numbers:
        val = str(raw or "").strip()
        if not val or val in seen:
            continue
        normalized.append(val)
        seen.add(val)

    if not normalized:
        raise HTTPException(status_code=400, detail="item_numbers is empty")

    cfg = ApiConfig.from_env()
    xml_delete = build_item_delete(
        items=[{"itemNumber": item_number} for item_number in normalized],
        config=cfg,
    )
    try:
        delete_resp_xml = send_request(xml_delete, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    resp = parse_item_delete_response(delete_resp_xml)
    resp["item_numbers"] = normalized
    resp["requested"] = len(normalized)
    return resp


@router.post("/update_prices")
def update_prices() -> Dict[str, Any]:
    """
    Массовое обновление цен по EAN из CSV (PRICE_SHEET_PATH).
    Для всех товаров сервера ищем EAN в прайс‑листе и вызываем itemUpdate по itemID.
    """
    cfg = ApiConfig.from_env()
    prices = load_prices()  # EAN -> ????? ????
    server_items = load_all_items()

    # Получаем карту referenceID -> itemID из Hood
    xml_body = build_item_list(
        item_status="running",
        start_at=1,
        group_size=500,
        start_date=None,
        end_date=None,
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    hood_data = parse_item_list_response(response_xml)
    hood_items = hood_data.get("items", [])
    ref_to_item_id: Dict[str, str] = {
        it.get("referenceID", ""): it.get("itemID")
        for it in hood_items
        if it.get("referenceID") and it.get("itemID")
    }

    updates: List[Dict[str, Any]] = []
    for raw in server_items:
        norm = normalize_item(raw)
        ean = norm.get("ean")
        ref = norm["reference_id"]
        if not ean or ean not in prices or ref not in ref_to_item_id:
            continue
        new_price = prices[ean]
        updates.append(
            {
                "itemID": ref_to_item_id[ref],
                "startPrice": float(new_price),
            }
        )

    if not updates:
        return {"updated": 0, "details": [], "message": "Нет товаров для обновления цен"}

    # itemUpdate принимает до 5 товаров за раз — бьём на чанки
    chunks = [updates[i : i + 5] for i in range(0, len(updates), 5)]
    all_responses: List[Dict[str, Any]] = []

    for chunk in chunks:
        xml_update = build_item_update(items=chunk, config=cfg)
        try:
            resp_xml = send_request(xml_update, config=cfg)
        except Exception as exc:
            all_responses.append(
                {
                    "success": False,
                    "error": str(exc),
                    "items": [u["itemID"] for u in chunk],
                }
            )
            continue
        parsed = parse_generic_response(resp_xml)
        parsed["items"] = [u["itemID"] for u in chunk]
        all_responses.append(parsed)

    return {
        "updated": len(updates),
        "details": all_responses,
    }
