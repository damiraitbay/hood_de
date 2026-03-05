import json
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Tuple

from app.config import settings
from app.items.storage import load_all_items, load_items_from_source_file
from app.items.utils import normalize_item
from app.logger import get_logger
from hood_api.api.parsers import parse_item_detail_response
from hood_api.builders import build_item_detail_by_item_number
from hood_api.client import send_request
from hood_api.config import ApiConfig

logger = get_logger("items")
_NOT_FOUND_MARKER = "artikel nicht gefunden"


def get_server_items(json_folder: str | None = None) -> List[Dict[str, Any]]:
    return load_all_items(json_folder=json_folder)


def _normalize_item_number(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    compact = "".join(raw.split())
    if compact.endswith(".0") and compact[:-2].isdigit():
        return compact[:-2]
    return compact


def _exists_in_hood_by_item_detail(item_number: str, cfg: ApiConfig) -> Tuple[bool, str | None]:
    xml_body = build_item_detail_by_item_number(item_number=item_number, config=cfg)
    try:
        response_xml = send_request(xml_body, config=cfg)
    except Exception as exc:
        return False, f"request_error: {exc}"

    try:
        data = parse_item_detail_response(response_xml)
    except Exception as exc:
        snippet = str(response_xml or "")[:200].replace("\n", " ").strip()
        return False, f"parse_error: {exc}; snippet={snippet}"

    errors = [str(err or "").strip() for err in data.get("errors") or [] if str(err or "").strip()]
    if errors:
        full_err = " ".join(errors).lower()
        if _NOT_FOUND_MARKER in full_err:
            return False, None
        return False, "; ".join(errors)

    items = data.get("items") or []
    if items:
        return True, None

    message = str(data.get("message") or "").strip().lower()
    if _NOT_FOUND_MARKER in message:
        return False, None
    if message:
        return False, message

    return False, "itemDetail returned no items"


def split_uploaded_items(
    account: str | None = None,
    json_folder: str | None = None,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], str]:
    cfg = ApiConfig.from_env(account=account)
    workers = max(1, min(int(os.environ.get("HOOD_UPLOADED_SPLIT_WORKERS", "8")), 32))
    local_items = load_all_items(json_folder=json_folder)
    uploaded: List[Dict[str, Any]] = []
    not_uploaded: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    def _factory_from_raw(raw_item: Dict[str, Any]) -> str:
        return str(raw_item.get("__source_name__") or "").strip()

    def _raw_with_factory(raw_item: Dict[str, Any], item_number: str | None = None) -> Dict[str, Any]:
        item = dict(raw_item)
        factory = _factory_from_raw(raw_item)
        if factory:
            item["factory"] = factory
        if item_number:
            item["checked_item_number"] = item_number
        return item

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "checking_items",
                "total_items": len(local_items),
                "processed_items": 0,
                "uploaded": 0,
                "not_uploaded": 0,
                "warnings_count": len(warnings),
                "workers": workers,
            }
        )

    futures_map: Dict[Any, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for raw in local_items:
            norm = normalize_item(raw)
            item_number = _normalize_item_number(norm.get("item_number") or norm.get("ean"))
            if not item_number:
                warnings.append(
                    {
                        "reason": "missing_item_number",
                        "local_id": str(raw.get("ID") or raw.get("id") or "").strip() or None,
                        "factory": _factory_from_raw(raw) or None,
                    }
                )
                not_uploaded.append(_raw_with_factory(raw))
                continue
            future = executor.submit(_exists_in_hood_by_item_detail, item_number, cfg)
            futures_map[future] = {"item_number": item_number, "raw": raw}

        processed = 0
        for future in as_completed(futures_map):
            ctx = futures_map[future]
            item_number = str(ctx["item_number"])
            raw = ctx["raw"]
            try:
                exists, err = future.result()
            except Exception as exc:
                exists, err = False, f"worker_error: {exc}"

            if err:
                warnings.append(
                    {
                        "item_number": item_number,
                        "reason": err,
                        "factory": _factory_from_raw(raw) or None,
                    }
                )

            if exists:
                uploaded.append(_raw_with_factory(raw, item_number=item_number))
            else:
                not_uploaded.append(_raw_with_factory(raw, item_number=item_number))

            processed += 1
            if progress_cb is not None and (processed % 100 == 0 or processed == len(futures_map)):
                progress_cb(
                    {
                        "phase": "checking_items",
                        "total_items": len(local_items),
                        "processed_items": processed,
                        "uploaded": len(uploaded),
                        "not_uploaded": len(not_uploaded),
                        "warnings_count": len(warnings),
                        "workers": workers,
                    }
                )

    account_suffix = str((account or "default")).strip().lower() or "default"
    not_uploaded_path = os.path.join(settings.LOG_FOLDER, f"not_in_hood_{account_suffix}.json")
    os.makedirs(os.path.dirname(not_uploaded_path), exist_ok=True)
    with open(not_uploaded_path, "w", encoding="utf-8") as f:
        json.dump(not_uploaded, f, ensure_ascii=False, indent=2)

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "saving_not_uploaded_json",
                "not_uploaded_file": not_uploaded_path,
                "uploaded": len(uploaded),
                "not_uploaded": len(not_uploaded),
                "warnings_count": len(warnings),
            }
        )

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "completed",
                "total_items": len(local_items),
                "processed_items": len(local_items),
                "uploaded": len(uploaded),
                "not_uploaded": len(not_uploaded),
                "warnings_count": len(warnings),
                "not_uploaded_file": not_uploaded_path,
            }
        )

    return uploaded, not_uploaded, warnings, not_uploaded_path


def check_selected_source_files(
    source_files: List[str],
    account: str | None = None,
    json_folder: str | None = None,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> Dict[str, Any]:
    cfg = ApiConfig.from_env(account=account)
    workers = max(1, min(int(os.environ.get("HOOD_CHECK_SELECTED_FILES_WORKERS", "8")), 32))

    normalized_files: List[str] = []
    seen: set[str] = set()
    for raw in source_files:
        name = str(raw or "").strip()
        if not name or name in seen:
            continue
        normalized_files.append(name)
        seen.add(name)
    if not normalized_files:
        raise ValueError("source_files is empty")

    files_total = len(normalized_files)
    files_done = 0
    total_uploaded = 0
    total_not_uploaded = 0
    total_missing_number = 0
    total_processed_items = 0
    files: List[Dict[str, Any]] = []

    for source_file in normalized_files:
        file_items = load_items_from_source_file(source_file, json_folder=json_folder)
        file_uploaded = 0
        file_not_uploaded = 0
        file_missing_number = 0
        missing_items: List[Dict[str, Any]] = []
        futures_map: Dict[Any, Dict[str, Any]] = {}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for raw in file_items:
                norm = normalize_item(raw)
                item_number = _normalize_item_number(norm.get("item_number") or norm.get("ean"))
                if not item_number:
                    file_missing_number += 1
                    missing_items.append(
                        {
                            "id": str(raw.get("ID") or raw.get("id") or "").strip() or None,
                            "item_number": None,
                            "factory": str(raw.get("__source_name__") or source_file),
                            "reason": "missing_item_number",
                        }
                    )
                    continue
                future = executor.submit(_exists_in_hood_by_item_detail, item_number, cfg)
                futures_map[future] = {"item_number": item_number, "raw": raw}

            for future in as_completed(futures_map):
                ctx = futures_map[future]
                raw = ctx["raw"]
                item_number = str(ctx["item_number"])
                try:
                    exists, err = future.result()
                except Exception as exc:
                    exists, err = False, f"worker_error: {exc}"
                if exists:
                    file_uploaded += 1
                else:
                    file_not_uploaded += 1
                    missing_items.append(
                        {
                            "id": str(raw.get("ID") or raw.get("id") or "").strip() or None,
                            "item_number": item_number,
                            "factory": str(raw.get("__source_name__") or source_file),
                            "reason": err or "not_found_in_hood",
                        }
                    )

        file_checkable = len(file_items) - file_missing_number
        files_done += 1
        total_uploaded += file_uploaded
        total_not_uploaded += file_not_uploaded
        total_missing_number += file_missing_number
        total_processed_items += len(file_items)

        files.append(
            {
                "source_file": source_file,
                "total_items": len(file_items),
                "checkable_items": file_checkable,
                "uploaded_items": file_uploaded,
                "not_uploaded_items": file_not_uploaded,
                "missing_item_number": file_missing_number,
                "has_any_in_hood": file_uploaded > 0,
                "fully_uploaded": file_checkable > 0 and file_not_uploaded == 0,
                "missing_items": missing_items,
            }
        )

        if progress_cb is not None:
            progress_cb(
                {
                    "phase": "checking_files",
                    "files_total": files_total,
                    "files_done": files_done,
                    "processed_items": total_processed_items,
                    "uploaded_items": total_uploaded,
                    "not_uploaded_items": total_not_uploaded,
                    "missing_item_number": total_missing_number,
                }
            )

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "completed",
                "files_total": files_total,
                "files_done": files_done,
                "processed_items": total_processed_items,
                "uploaded_items": total_uploaded,
                "not_uploaded_items": total_not_uploaded,
                "missing_item_number": total_missing_number,
            }
        )

    return {
        "files_total": files_total,
        "files_done": files_done,
        "processed_items": total_processed_items,
        "uploaded_items": total_uploaded,
        "not_uploaded_items": total_not_uploaded,
        "missing_item_number": total_missing_number,
        "files": files,
    }

