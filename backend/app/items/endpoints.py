import asyncio
import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query

from app.config import (
    get_html_folder_for_account,
    get_json_folder_for_account,
    get_price_sheet_for_account,
    normalize_account_name,
    settings,
)
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
    parse_item_update_response,
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

# Р¤Р°Р№Р», РєСѓРґР° Р±СѓРґРµРј СЃРєР»Р°РґС‹РІР°С‚СЊ С‚РѕРІР°СЂС‹, РЅРµ Р·Р°РіСЂСѓР·РёРІС€РёРµСЃСЏ РІ Hood
FAILED_ITEMS_PATH = Path(settings.LOG_FOLDER).resolve() / "failed_items.json"
UPDATE_JOBS: Dict[str, Dict[str, Any]] = {}
UPDATE_JOBS_LOCK = threading.Lock()
UPLOAD_JOBS: Dict[str, Dict[str, Any]] = {}
UPLOAD_JOBS_LOCK = threading.Lock()


def _account_mode(account: str | None) -> str | None:
    try:
        return normalize_account_name(account)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def _load_all_hood_items(cfg: ApiConfig, item_status: str = "running", group_size: int = 500) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    start_at = 1

    while True:
        xml_body = build_item_list(
            item_status=item_status,
            start_at=start_at,
            group_size=group_size,
            start_date=None,
            end_date=None,
            config=cfg,
        )
        try:
            response_xml = send_request(xml_body, config=cfg)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc))

        page = parse_item_list_response(response_xml)
        if page.get("errors"):
            raise HTTPException(
                status_code=502,
                detail={"message": page.get("message"), "errors": page.get("errors")},
            )

        page_items = page.get("items", [])
        items.extend(page_items)

        total_records = int(page.get("total_records") or 0)
        if not page_items:
            break
        if total_records and len(items) >= total_records:
            break
        if len(page_items) < group_size:
            break

        start_at += group_size

    return items


def _resolve_description_for_api(norm: Dict[str, Any], html_folder: str | None = None) -> str:
    """
    Р”Р»СЏ API РѕС‚РїСЂР°РІР»СЏРµРј HTML-РѕРїРёСЃР°РЅРёРµ РїРѕ EAN, РµСЃР»Рё РЅР°Р№РґРµРЅ С„Р°Р№Р» <EAN>.html/.htm.
    Р•СЃР»Рё С„Р°Р№Р»Р° РЅРµС‚ РёР»Рё С‡С‚РµРЅРёРµ РЅРµ СѓРґР°Р»РѕСЃСЊ, РѕС‚РїСЂР°РІР»СЏРµРј РѕР±С‹С‡РЅС‹Р№ description.
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
    if (html_folder or "").strip():
        candidate_dirs.append(Path(html_folder))
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
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    """
    Р’С‹РІРѕРґ С‚РѕРІР°СЂРѕРІ РёР· РЅР°С€РёС… Р»РѕРєР°Р»СЊРЅС‹С… JSON (СЃ РїР°РіРёРЅР°С†РёРµР№).
    РџРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РІРѕР·РІСЂР°С‰Р°РµС‚ СЃС‹СЂС‹Рµ Р·Р°РїРёСЃРё, РµСЃР»Рё normalized=true вЂ” РІРѕР·РІСЂР°С‰Р°РµС‚ normalize_item().
    """
    account_mode = _account_mode(account)
    json_folder = get_json_folder_for_account(account_mode)
    try:
        all_items = (
            load_items_from_source_file(source_file, json_folder=json_folder)
            if source_file
            else load_all_items(json_folder=json_folder)
        )
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
        "account": account_mode,
        "items": items,
    }


@router.get("/json/files")
def json_files(account: str | None = Query(default=None)) -> Dict[str, Any]:
    account_mode = _account_mode(account)
    json_folder = get_json_folder_for_account(account_mode)
    try:
        files = list_json_source_files(json_folder=json_folder)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"account": account_mode, "files": files}


@router.get("/lookup/{reference_id}")
def lookup_in_hood(reference_id: str, account: str | None = Query(default=None)) -> Dict[str, Any]:
    """
    ???? ????? ? Hood ?? referenceID (????????, ART84153326) ? ?????????? itemID ? ?????? ????.
    """
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
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
    # РµСЃР»Рё РЅРµСЃРєРѕР»СЊРєРѕ вЂ” РІРµСЂРЅС‘Рј РІСЃРµ, РЅРѕ С‡Р°С‰Рµ РІСЃРµРіРѕ Р±СѓРґРµС‚ РѕРґРёРЅ
    return {"reference_id": reference_id, "account": account_mode, "items": found}


def _find_raw_item_by_id(
    item_id: str,
    source_file: str | None = None,
    json_folder: str | None = None,
) -> Dict[str, Any] | None:
    source_items = (
        load_items_from_source_file(source_file, json_folder=json_folder)
        if source_file
        else load_all_items(json_folder=json_folder)
    )
    for it in source_items:
        if str(it.get("ID", "")) == str(item_id):
            return it
    return None


def _build_item_payload_from_norm(norm: Dict[str, Any], api_description: str) -> Dict[str, Any]:
    return {
        "reference_id": norm["reference_id"],
        "title": norm["item_name"],
        "itemName": norm["item_name"],
        "description": api_description,
        "price": norm["price"],
        "quantity": norm["quantity"],
        "categoryID": norm["category_id"],
        "condition": norm["condition"],
        "itemMode": norm["item_mode"],
        "pay_options": ["paypal"],
        "ship_methods": [{"name": "DHLPacket", "country": "nat", "value": "5.99"}],
        "image_urls": norm.get("image_urls") or [],
        "product_properties": norm.get("product_properties") or [],
        "ean": norm.get("ean"),
        "mpn": norm.get("mpn"),
        "item_number": norm.get("item_number"),
        "country": norm.get("country"),
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _set_update_job(job_id: str, patch: Dict[str, Any]) -> None:
    with UPDATE_JOBS_LOCK:
        current = UPDATE_JOBS.get(job_id, {})
        current.update(patch)
        UPDATE_JOBS[job_id] = current


def _set_upload_job(job_id: str, patch: Dict[str, Any]) -> None:
    with UPLOAD_JOBS_LOCK:
        current = UPLOAD_JOBS.get(job_id, {})
        current.update(patch)
        UPLOAD_JOBS[job_id] = current


def _run_items_update(
    limit: int,
    source_file: str | None,
    account: str | None,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)

    source_items = (
        load_items_from_source_file(source_file, json_folder=json_folder)
        if source_file
        else load_all_items(json_folder=json_folder)
    )

    norms: List[Dict[str, Any]] = [normalize_item(raw) for raw in source_items]
    if limit > 0:
        norms = norms[:limit]

    update_payloads: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for norm in norms:
        item_number = str(norm.get("item_number") or norm.get("ean") or "").strip()
        if not item_number:
            skipped.append(
                {
                    "reference_id": norm["reference_id"],
                    "success": False,
                    "error": "itemNumber/ean is empty",
                }
            )
            continue

        api_description = _resolve_description_for_api(norm, html_folder=html_folder)
        payload = _build_item_payload_from_norm(norm, api_description)
        payload["item_number"] = item_number
        update_payloads.append(payload)

    chunks = [update_payloads[i : i + 5] for i in range(0, len(update_payloads), 5)]
    details: List[Dict[str, Any]] = []
    updated = 0
    failed = len(skipped)
    total_chunks = len(chunks)
    total_items = len(update_payloads)

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "prepared",
                "requested": len(norms),
                "prepared": total_items,
                "skipped": len(skipped),
                "total_chunks": total_chunks,
                "processed_chunks": 0,
                "processed_items": 0,
                "updated": 0,
                "failed": failed,
            }
        )

    for idx, chunk in enumerate(chunks, start=1):
        chunk_ids = [str(x.get("item_number") or x.get("ean") or "") for x in chunk]
        xml_update = build_item_update(items=chunk, config=cfg)
        try:
            resp_xml = send_request(xml_update, config=cfg)
            parsed = parse_item_update_response(resp_xml)
        except Exception as exc:
            parsed = {"success": False, "status": "error", "message": str(exc), "errors": [str(exc)]}

        parsed["item_numbers"] = chunk_ids
        details.append(parsed)
        if parsed.get("success"):
            updated += len(chunk_ids)
        else:
            failed += len(chunk_ids)

        if progress_cb is not None:
            progress_cb(
                {
                    "phase": "updating",
                    "total_chunks": total_chunks,
                    "processed_chunks": idx,
                    "total_items": total_items,
                    "processed_items": min(idx * 5, total_items),
                    "updated": updated,
                    "failed": failed,
                    "last_chunk_item_numbers": chunk_ids,
                    "last_chunk_success": bool(parsed.get("success")),
                    "last_chunk_status": parsed.get("status"),
                    "last_chunk_message": parsed.get("message"),
                    "last_chunk_errors": parsed.get("errors") or [],
                }
            )

    result = {
        "requested": len(norms),
        "prepared": len(update_payloads),
        "updated": updated,
        "failed": failed,
        "account": account_mode,
        "source_file": source_file,
        "details": details,
        "skipped": skipped,
    }
    if progress_cb is not None:
        progress_cb({"phase": "completed", "result_summary": {"updated": updated, "failed": failed}})
    return result


def _upload_one_by_id(item_id: str, source_file: str | None = None, account: str | None = None) -> Dict[str, Any]:
    account_mode = _account_mode(account)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)
    try:
        raw = _find_raw_item_by_id(item_id, source_file=source_file, json_folder=json_folder)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")
    if not raw:
        raise HTTPException(status_code=404, detail="Item not found in JSON by ID")

    cfg = ApiConfig.from_env(account=account_mode)
    norm = normalize_item(raw)
    api_description = _resolve_description_for_api(norm, html_folder=html_folder)

    payload = _build_item_payload_from_norm(norm, api_description)
    xml_body = build_item_insert(
        reference_id=payload["reference_id"],
        title=payload["title"],
        description=payload["description"],
        price=payload["price"],
        quantity=payload["quantity"],
        category_id=payload["categoryID"],
        condition=payload["condition"],
        item_mode=payload["itemMode"],
        pay_options=payload["pay_options"],
        ship_methods=payload["ship_methods"],
        image_urls=payload["image_urls"],
        product_properties=payload["product_properties"],
        ean=payload["ean"],
        mpn=payload["mpn"],
        item_number=payload["item_number"],
        country=payload["country"],
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    resp = parse_item_insert_response(response_xml)
    resp["reference_id"] = norm["reference_id"]
    resp["item_id_local"] = str(item_id)
    resp["account"] = account_mode

    msg = (resp.get("item_message") or "") + " " + " ".join(resp.get("errors") or [])
    if "Sie haben bereits einen identischen Artikel" in msg:
        resp["success"] = True

    return resp


@router.post("/validate_one/{item_id}")
def validate_one(
    item_id: str,
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    """
    РџСЂРѕРІРµСЂРєР° СЃС‚СЂСѓРєС‚СѓСЂС‹ РѕРґРЅРѕРіРѕ С‚РѕРІР°СЂР° РїРѕ ID РёР· JSON.
    """
    account_mode = _account_mode(account)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)
    try:
        raw = _find_raw_item_by_id(item_id, source_file=source_file, json_folder=json_folder)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")
    if not raw:
        raise HTTPException(status_code=404, detail="Item not found in JSON by ID")
    cfg = ApiConfig.from_env(account=account_mode)
    norm = normalize_item(raw)
    api_description = _resolve_description_for_api(norm, html_folder=html_folder)
    payload = _build_item_payload_from_norm(norm, api_description)
    xml_body = build_item_validate(
        reference_id=payload["reference_id"],
        title=payload["title"],
        description=payload["description"],
        price=payload["price"],
        quantity=payload["quantity"],
        category_id=payload["categoryID"],
        condition=payload["condition"],
        item_mode=payload["itemMode"],
        pay_options=payload["pay_options"],
        ship_methods=payload["ship_methods"],
        image_urls=payload["image_urls"],
        product_properties=payload["product_properties"],
        ean=payload["ean"],
        mpn=payload["mpn"],
        item_number=payload["item_number"],
        country=payload["country"],
        config=cfg,
    )
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    resp = parse_item_insert_response(response_xml)
    resp["reference_id"] = norm["reference_id"]
    resp["item_id_local"] = item_id
    resp["account"] = account_mode
    return resp


@router.post("/upload_one/{item_id}")
def upload_one(
    item_id: str,
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    """
    РћС‚РїСЂР°РІРєР° РѕРґРЅРѕРіРѕ С‚РѕРІР°СЂР° РїРѕ ID РёР· JSON.
    Р•СЃР»Рё Hood РІРµСЂРЅСѓР» 'Sie haben bereits einen identischen Artikel' вЂ” СЃС‡РёС‚Р°РµРј СѓСЃРїРµС…РѕРј Рё СѓРґР°Р»СЏРµРј РёР· JSON.
    """
    return _upload_one_by_id(item_id=item_id, source_file=source_file, account=account)


@router.post("/upload_one")
def upload_many(
    item_ids: List[str] = Body(..., embed=True),
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
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
            resp = _upload_one_by_id(item_id=item_id, source_file=source_file, account=account)
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


@router.post("/update_one")
def update_many(
    item_ids: List[str] = Body(..., embed=True),
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
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

    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)

    try:
        source_items = (
            load_items_from_source_file(source_file, json_folder=json_folder)
            if source_file
            else load_all_items(json_folder=json_folder)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")

    raw_by_id: Dict[str, Dict[str, Any]] = {
        str(it.get("ID", "")).strip(): it
        for it in source_items
        if str(it.get("ID", "")).strip()
    }

    update_payloads: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for item_id in normalized_ids:
        raw = raw_by_id.get(item_id)
        if not raw:
            skipped.append({"item_id_local": item_id, "success": False, "error": "Item not found in JSON by ID"})
            continue

        norm = normalize_item(raw)
        api_description = _resolve_description_for_api(norm, html_folder=html_folder)
        item_number = str(norm.get("item_number") or norm.get("ean") or "").strip()
        if not item_number:
            skipped.append(
                {
                    "item_id_local": item_id,
                    "reference_id": norm["reference_id"],
                    "success": False,
                    "error": "itemNumber/ean is empty",
                }
            )
            continue

        payload = _build_item_payload_from_norm(norm, api_description)
        payload["item_number"] = item_number
        update_payloads.append(payload)

    if not update_payloads:
        return {
            "requested": len(normalized_ids),
            "prepared": 0,
            "updated": 0,
            "failed": len(skipped),
            "account": account_mode,
            "details": [],
            "skipped": skipped,
        }

    chunks = [update_payloads[i : i + 5] for i in range(0, len(update_payloads), 5)]
    details: List[Dict[str, Any]] = []
    updated = 0
    failed = len(skipped)

    for chunk in chunks:
        xml_update = build_item_update(items=chunk, config=cfg)
        chunk_ids = [str(x.get("item_number") or x.get("ean") or "") for x in chunk]
        try:
            resp_xml = send_request(xml_update, config=cfg)
            parsed = parse_item_update_response(resp_xml)
        except Exception as exc:
            parsed = {"success": False, "status": "error", "message": str(exc), "errors": [str(exc)]}

        parsed["item_numbers"] = chunk_ids
        details.append(parsed)
        if parsed.get("success"):
            updated += len(chunk_ids)
        else:
            failed += len(chunk_ids)

    return {
        "requested": len(normalized_ids),
        "prepared": len(update_payloads),
        "updated": updated,
        "failed": failed,
        "account": account_mode,
        "details": details,
        "skipped": skipped,
    }


@router.post("/update")
def items_update(
    limit: int = 1,
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    """
    Массовое обновление товаров из JSON в Hood через itemUpdate.
    limit=0 — обновить все товары из выбранного source_file (или из всей папки JSON).
    """
    try:
        return _run_items_update(limit=limit, source_file=source_file, account=account)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")


def _run_items_update_job(job_id: str, limit: int, source_file: str | None, account: str | None) -> None:
    _set_update_job(
        job_id,
        {
            "status": "running",
            "started_at": _utc_now_iso(),
        },
    )
    def progress_cb(progress: Dict[str, Any]) -> None:
        _set_update_job(job_id, {"progress": progress, "last_update_at": _utc_now_iso()})

    try:
        result = _run_items_update(limit=limit, source_file=source_file, account=account, progress_cb=progress_cb)
    except Exception as exc:
        _set_update_job(
            job_id,
            {
                "status": "failed",
                "finished_at": _utc_now_iso(),
                "error": str(exc),
                "progress": {"phase": "failed"},
            },
        )
        return

    _set_update_job(
        job_id,
        {
            "status": "completed",
            "finished_at": _utc_now_iso(),
            "result": result,
            "progress": {
                "phase": "completed",
                "total_items": result.get("prepared", 0),
                "processed_items": result.get("prepared", 0),
                "updated": result.get("updated", 0),
                "failed": result.get("failed", 0),
            },
        },
    )


@router.post("/update_async")
def items_update_async(
    background_tasks: BackgroundTasks,
    limit: int = 1,
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    # Validate account early to fail fast on bad input.
    _account_mode(account)

    job_id = uuid4().hex
    _set_update_job(
        job_id,
        {
            "job_id": job_id,
            "status": "queued",
            "created_at": _utc_now_iso(),
            "limit": limit,
            "source_file": source_file,
            "account": account,
        },
    )
    background_tasks.add_task(_run_items_update_job, job_id, limit, source_file, account)
    return {
        "job_id": job_id,
        "status": "queued",
        "status_url": f"/api/items/update_async/{job_id}",
    }


@router.get("/update_async/{job_id}")
def items_update_async_status(job_id: str) -> Dict[str, Any]:
    with UPDATE_JOBS_LOCK:
        job = UPDATE_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


def _run_items_upload_job(job_id: str, limit: int, source_file: str | None, account: str | None) -> None:
    _set_upload_job(
        job_id,
        {
            "status": "running",
            "started_at": _utc_now_iso(),
        },
    )

    def progress_cb(progress: Dict[str, Any]) -> None:
        _set_upload_job(job_id, {"progress": progress, "last_update_at": _utc_now_iso()})

    try:
        result = asyncio.run(_run_items_upload(limit=limit, source_file=source_file, account=account, progress_cb=progress_cb))
    except Exception as exc:
        _set_upload_job(
            job_id,
            {
                "status": "failed",
                "finished_at": _utc_now_iso(),
                "error": str(exc),
                "progress": {"phase": "failed"},
            },
        )
        return

    _set_upload_job(
        job_id,
        {
            "status": "completed",
            "finished_at": _utc_now_iso(),
            "result": result,
            "progress": {
                "phase": "completed",
                "total_items": len(result),
                "processed_items": len(result),
                "success": sum(1 for r in result if r.get("success")),
                "failed": sum(1 for r in result if not r.get("success")),
            },
        },
    )


@router.post("/upload_async")
def items_upload_async(
    background_tasks: BackgroundTasks,
    limit: int = 1,
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    _account_mode(account)
    job_id = uuid4().hex
    _set_upload_job(
        job_id,
        {
            "job_id": job_id,
            "status": "queued",
            "created_at": _utc_now_iso(),
            "limit": limit,
            "source_file": source_file,
            "account": account,
        },
    )
    background_tasks.add_task(_run_items_upload_job, job_id, limit, source_file, account)
    return {
        "job_id": job_id,
        "status": "queued",
        "status_url": f"/api/items/upload_async/{job_id}",
    }


@router.get("/upload_async/{job_id}")
def items_upload_async_status(job_id: str) -> Dict[str, Any]:
    with UPLOAD_JOBS_LOCK:
        job = UPLOAD_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.get("/status")
def items_status() -> Dict[str, Any]:
    """
    РЎРїРёСЃРѕРє С‚РѕРІР°СЂРѕРІ, РєРѕС‚РѕСЂС‹Рµ РќР• СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РІ Hood РїСЂРё РїРѕСЃР»РµРґРЅРµРј /items/upload.
    Р§РёС‚Р°РµРј РёС… РёР· failed_items.json.
    """
    if not FAILED_ITEMS_PATH.exists():
        return {"failed_items": []}

    try:
        data = json.loads(FAILED_ITEMS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"РќРµ СѓРґР°Р»РѕСЃСЊ РїСЂРѕС‡РёС‚Р°С‚СЊ failed_items.json: {exc}")

    # РћР¶РёРґР°РµС‚СЃСЏ СЃРїРёСЃРѕРє РЅРѕСЂРјР°Р»РёР·РѕРІР°РЅРЅС‹С… С‚РѕРІР°СЂРѕРІ
    if not isinstance(data, list):
        data = []
    return {"failed_items": data}


@router.post("/validate")
def items_validate(account: str | None = Query(default=None)) -> List[Dict[str, Any]]:
    """
    РџСЂРѕРІРµСЂРєР° СЃС‚СЂСѓРєС‚СѓСЂС‹ С‚РѕРІР°СЂРѕРІ: itemValidate РґР»СЏ РІСЃРµС… С‚РѕРІР°СЂРѕРІ СЃРµСЂРІРµСЂР°.
    """
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)
    server_items = load_all_items(json_folder=json_folder)

    results: List[Dict[str, Any]] = []
    for raw in server_items:
        norm = normalize_item(raw)
        api_description = _resolve_description_for_api(norm, html_folder=html_folder)
        payload = _build_item_payload_from_norm(norm, api_description)
        xml_body = build_item_validate(
            reference_id=payload["reference_id"],
            title=payload["title"],
            description=payload["description"],
            price=payload["price"],
            quantity=payload["quantity"],
            category_id=payload["categoryID"],
            condition=payload["condition"],
            item_mode=payload["itemMode"],
            pay_options=payload["pay_options"],
            ship_methods=payload["ship_methods"],
            image_urls=payload["image_urls"],
            product_properties=payload["product_properties"],
            ean=payload["ean"],
            mpn=payload["mpn"],
            item_number=payload["item_number"],
            country=payload["country"],
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
        resp["account"] = account_mode
        results.append(resp)
    return results


async def _run_items_upload(
    limit: int = 1,
    source_file: str | None = None,
    account: str | None = None,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> List[Dict[str, Any]]:
    """
    РђСЃРёРЅС…СЂРѕРЅРЅР°СЏ Р·Р°РіСЂСѓР·РєР° Р’РЎР•РҐ С‚РѕРІР°СЂРѕРІ РёР· JSON РІ Hood.
    Р•СЃР»Рё РїСЂРё Р·Р°РіСЂСѓР·РєРµ С‚РѕРІР°СЂР° РїСЂРѕРёР·РѕС€Р»Р° РѕС€РёР±РєР°, СЃРѕС…СЂР°РЅСЏРµРј РµРіРѕ РІ failed_items.json.
    """
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)
    try:
        server_items = (
            load_items_from_source_file(source_file, json_folder=json_folder)
            if source_file
            else load_all_items(json_folder=json_folder)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"JSON file not found: {source_file}")
    all_norms: List[Dict[str, Any]] = [normalize_item(raw) for raw in server_items]

    # РџРѕРєР° РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РіСЂСѓР·РёРј С‚РѕР»СЊРєРѕ РїРµСЂРІС‹Р№ С‚РѕРІР°СЂ (limit=1).
    # Р”Р»СЏ РјР°СЃСЃРѕРІРѕР№ Р·Р°РіСЂСѓР·РєРё РјРѕР¶РЅРѕ Р±СѓРґРµС‚ РїСЂРѕСЃС‚Рѕ РІС‹Р·РІР°С‚СЊ /items/upload?limit=1000.
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
    success_count = 0
    failed_count = 0

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "prepared",
                "total_items": total_count,
                "processed_items": 0,
                "success": 0,
                "failed": 0,
            }
        )

    async def worker(norm: Dict[str, Any]) -> None:
        nonlocal processed_count, success_count, failed_count
        async with semaphore:
            api_description = _resolve_description_for_api(norm, html_folder=html_folder)
            payload = _build_item_payload_from_norm(norm, api_description)
            xml_body = build_item_insert(
                reference_id=payload["reference_id"],
                title=payload["title"],
                description=payload["description"],
                price=payload["price"],
                quantity=payload["quantity"],
                category_id=payload["categoryID"],
                condition=payload["condition"],
                item_mode=payload["itemMode"],
                pay_options=payload["pay_options"],
                ship_methods=payload["ship_methods"],
                image_urls=payload["image_urls"],
                product_properties=payload["product_properties"],
                ean=payload["ean"],
                mpn=payload["mpn"],
                item_number=payload["item_number"],
                country=payload["country"],
                config=cfg,
            )
            try:
                response_xml = await asyncio.to_thread(send_request, xml_body, cfg)
                resp = parse_item_insert_response(response_xml)
                resp["reference_id"] = norm["reference_id"]
                resp["account"] = account_mode

                # РЎРїРµС†РёР°Р»СЊРЅС‹Р№ СЃР»СѓС‡Р°Р№: С‚РѕРІР°СЂ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚ РІ Hood
                msg = (resp.get("item_message") or "") + " " + " ".join(resp.get("errors") or [])
                if "Sie haben bereits einen identischen Artikel" in msg:
                    logger.info(
                        f"в‰Ў {norm['reference_id']} СѓР¶Рµ РµСЃС‚СЊ РІ Hood (identischer Artikel); "
                        f"itemID={resp.get('item_id', '?')} вЂ” СѓРґР°Р»СЏРµРј РёР· Р»РѕРєР°Р»СЊРЅРѕРіРѕ JSON"
                    )
                    # РЎС‡РёС‚Р°РµРј РєР°Рє СѓСЃРїРµС… Рё СѓРґР°Р»СЏРµРј РёР· РёСЃС…РѕРґРЅРѕРіРѕ JSON
                    resp["success"] = True
                elif resp.get("success"):
                    logger.info(
                        f"вњ“ {norm['reference_id']} Р·Р°РіСЂСѓР¶РµРЅ СѓСЃРїРµС€РЅРѕ; "
                        f"itemID={resp.get('item_id', '?')}"
                    )
                else:
                    logger.warning(f"вњ— {norm['reference_id']} РЅРµ Р·Р°РіСЂСѓР¶РµРЅ: {resp.get('item_message', 'unknown error')}")
            except Exception as exc:
                resp = {
                    "reference_id": norm["reference_id"],
                    "account": account_mode,
                    "success": False,
                    "error": str(exc),
                }
                logger.error(f"вњ— РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё С‚РѕРІР°СЂР° {norm['reference_id']}: {exc}")

            results.append(resp)
            processed_count += 1
            if resp.get("success"):
                success_count += 1
            else:
                failed_count += 1

            if progress_cb is not None:
                progress_cb(
                    {
                        "phase": "uploading",
                        "total_items": total_count,
                        "processed_items": processed_count,
                        "success": success_count,
                        "failed": failed_count,
                        "last_reference_id": norm["reference_id"],
                        "last_success": bool(resp.get("success")),
                        "last_error": resp.get("error") or resp.get("item_message"),
                    }
                )
            
            # Р›РѕРіРёСЂСѓРµРј РїСЂРѕРіСЂРµСЃСЃ РєР°Р¶РґС‹Рµ 10 С‚РѕРІР°СЂРѕРІ РёР»Рё РЅР° РєР°Р¶РґРѕРј 10-Рј, 20-Рј, 30-Рј Рё С‚.Рґ.
            if processed_count % 10 == 0 or processed_count == total_count:
                logger.info(f"РџСЂРѕРіСЂРµСЃСЃ: {processed_count}/{total_count} С‚РѕРІР°СЂРѕРІ РѕР±СЂР°Р±РѕС‚Р°РЅРѕ ({processed_count * 100 // total_count}%)")

    tasks = [worker(it) for it in to_upload]
    if tasks:
        await asyncio.gather(*tasks)

    # РЎРѕР±РёСЂР°РµРј РІСЃРµ С‚РѕРІР°СЂС‹, РєРѕС‚РѕСЂС‹Рµ РЅРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ, Рё СЃРѕС…СЂР°РЅСЏРµРј
    # ?????? ? ??????? ?????? Hood (status, errors, item_message, reference_id ? ?.?.)
    failed_items: List[Dict[str, Any]] = [r for r in results if not r.get("success")]

    FAILED_ITEMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    FAILED_ITEMS_PATH.write_text(json.dumps(failed_items, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(
        f"Р—Р°РіСЂСѓР·РєР° Р·Р°РІРµСЂС€РµРЅР°. РЈСЃРїРµС€РЅРѕ: {len(results) - len(failed_items)}, "
        f"СЃ РѕС€РёР±РєР°РјРё: {len(failed_items)}. Р¤Р°Р№Р» СЃ РѕС€РёР±РѕС‡РЅС‹РјРё С‚РѕРІР°СЂР°РјРё: {FAILED_ITEMS_PATH}"
    )
    if progress_cb is not None:
        progress_cb(
            {
                "phase": "completed",
                "total_items": total_count,
                "processed_items": total_count,
                "success": success_count,
                "failed": failed_count,
            }
        )
    return results


@router.post("/upload")
async def items_upload(
    limit: int = 1,
    source_file: str | None = Query(default=None),
    account: str | None = Query(default=None),
) -> List[Dict[str, Any]]:
    return await _run_items_upload(limit=limit, source_file=source_file, account=account)


@router.delete("/delete/by-item-number/{item_number}")
def delete_item_by_item_number(item_number: str, account: str | None = Query(default=None)) -> Dict[str, Any]:
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    xml_delete = build_item_delete(items=[{"itemNumber": item_number}], config=cfg)
    try:
        delete_resp_xml = send_request(xml_delete, config=cfg)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    resp = parse_item_delete_response(delete_resp_xml)
    resp["item_number"] = item_number
    resp["account"] = account_mode
    return resp


@router.post("/delete/by-item-number")
def delete_items_by_item_number(
    item_numbers: List[str] = Body(..., embed=True),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
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

    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
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
    resp["account"] = account_mode
    return resp


@router.delete("/delete/all")
def delete_all_items_from_hood(
    item_status: str = Query(default="running"),
    delete_batch_size: int = Query(default=200, ge=1, le=500),
    account: str | None = Query(default=None),
) -> Dict[str, Any]:
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    hood_items = _load_all_hood_items(cfg=cfg, item_status=item_status, group_size=500)
    logger.info(
        "Delete all start: item_status=%s, found_in_item_list=%s, delete_batch_size=%s",
        item_status,
        len(hood_items),
        delete_batch_size,
    )

    item_ids: List[str] = []
    seen: set[str] = set()
    missing_item_id = 0

    for item in hood_items:
        item_id = str(item.get("itemID") or "").strip()
        if not item_id:
            missing_item_id += 1
            continue
        if item_id in seen:
            continue
        seen.add(item_id)
        item_ids.append(item_id)

    if not item_ids:
        logger.info(
            "Delete all done: item_status=%s, requested=0, deleted=0, failed=0, missing_item_id=%s",
            item_status,
            missing_item_id,
        )
        return {
            "success": True,
            "item_status": item_status,
            "account": account_mode,
            "found_in_item_list": len(hood_items),
            "missing_item_id": missing_item_id,
            "requested": 0,
            "deleted": 0,
            "failed": 0,
            "details": [],
        }

    responses: List[Dict[str, Any]] = []
    deleted = 0
    failed = 0
    total_batches = (len(item_ids) + delete_batch_size - 1) // delete_batch_size

    for i in range(0, len(item_ids), delete_batch_size):
        chunk = item_ids[i : i + delete_batch_size]
        batch_num = (i // delete_batch_size) + 1
        xml_delete = build_item_delete(
            items=[{"itemID": item_id} for item_id in chunk],
            config=cfg,
        )
        try:
            delete_resp_xml = send_request(xml_delete, config=cfg)
        except Exception as exc:
            failed += len(chunk)
            logger.error(
                "Delete all batch failed: batch=%s/%s, requested=%s, error=%s",
                batch_num,
                total_batches,
                len(chunk),
                exc,
            )
            responses.append(
                {
                    "success": False,
                    "error": str(exc),
                    "requested_item_ids": chunk,
                }
            )
            continue

        resp = parse_item_delete_response(delete_resp_xml)
        responses.append(resp)

        item_results = resp.get("items", [])
        if item_results:
            batch_deleted = sum(1 for x in item_results if str(x.get("status") or "").lower() == "success")
            batch_failed = sum(1 for x in item_results if str(x.get("status") or "").lower() == "failed")
            deleted += batch_deleted
            failed += batch_failed
            unresolved = max(len(chunk) - batch_deleted - batch_failed, 0)
            failed += unresolved
            logger.info(
                "Delete all batch done: batch=%s/%s, requested=%s, deleted=%s, failed=%s, unresolved=%s",
                batch_num,
                total_batches,
                len(chunk),
                batch_deleted,
                batch_failed,
                unresolved,
            )
        elif resp.get("success"):
            deleted += len(chunk)
            logger.info(
                "Delete all batch done: batch=%s/%s, requested=%s, deleted=%s, failed=0",
                batch_num,
                total_batches,
                len(chunk),
                len(chunk),
            )
        else:
            failed += len(chunk)
            logger.warning(
                "Delete all batch done: batch=%s/%s, requested=%s, deleted=0, failed=%s",
                batch_num,
                total_batches,
                len(chunk),
                len(chunk),
            )

    logger.info(
        "Delete all done: item_status=%s, requested=%s, deleted=%s, failed=%s, missing_item_id=%s",
        item_status,
        len(item_ids),
        deleted,
        failed,
        missing_item_id,
    )
    return {
        "success": failed == 0,
        "item_status": item_status,
        "account": account_mode,
        "found_in_item_list": len(hood_items),
        "missing_item_id": missing_item_id,
        "requested": len(item_ids),
        "deleted": deleted,
        "failed": failed,
        "details": responses,
    }


@router.post("/update_prices")
def update_prices(account: str | None = Query(default=None)) -> Dict[str, Any]:
    """
    РњР°СЃСЃРѕРІРѕРµ РѕР±РЅРѕРІР»РµРЅРёРµ С†РµРЅ РїРѕ EAN РёР· CSV (PRICE_SHEET_PATH).
    Р”Р»СЏ РІСЃРµС… С‚РѕРІР°СЂРѕРІ СЃРµСЂРІРµСЂР° РёС‰РµРј EAN РІ РїСЂР°Р№СЃвЂ‘Р»РёСЃС‚Рµ Рё РІС‹Р·С‹РІР°РµРј itemUpdate РїРѕ itemID.
    """
    account_mode = _account_mode(account)
    cfg = ApiConfig.from_env(account=account_mode)
    price_sheet_path = get_price_sheet_for_account(account_mode)
    json_folder = get_json_folder_for_account(account_mode)
    html_folder = get_html_folder_for_account(account_mode)
    prices = load_prices(price_sheet_path=price_sheet_path)  # EAN -> price
    server_items = load_all_items(json_folder=json_folder)

    updates: List[Dict[str, Any]] = []
    for raw in server_items:
        norm = normalize_item(raw)
        ean = norm.get("ean")
        if not ean or ean not in prices:
            continue
        new_price = prices[ean]
        api_description = _resolve_description_for_api(norm, html_folder=html_folder)
        payload = _build_item_payload_from_norm(norm, api_description)
        payload["item_number"] = str(norm.get("item_number") or ean)
        payload["price"] = str(new_price)
        updates.append(payload)

    if not updates:
        return {"updated": 0, "details": [], "message": "РќРµС‚ С‚РѕРІР°СЂРѕРІ РґР»СЏ РѕР±РЅРѕРІР»РµРЅРёСЏ С†РµРЅ"}

    # itemUpdate РїСЂРёРЅРёРјР°РµС‚ РґРѕ 5 С‚РѕРІР°СЂРѕРІ Р·Р° СЂР°Р· вЂ” Р±СЊС‘Рј РЅР° С‡Р°РЅРєРё
    chunks = [updates[i : i + 5] for i in range(0, len(updates), 5)]
    all_responses: List[Dict[str, Any]] = []

    for chunk in chunks:
        xml_body = build_item_update(chunk, config=cfg)
        try:
            response_xml = send_request(xml_body, config=cfg)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc))
        parsed = parse_item_update_response(response_xml)
        all_responses.append(parsed)

    return {
        "updated": len(updates),
        "details": all_responses,
    }





