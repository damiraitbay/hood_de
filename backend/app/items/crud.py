import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Set, Tuple

from app.config import settings
from app.items.storage import load_all_items
from app.items.utils import normalize_item
from app.logger import get_logger
from hood_api.api.parsers import parse_item_detail_response
from hood_api.builders import build_item_detail
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
    xml_body = build_item_detail(item_id=item_number, config=cfg)
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
) -> Tuple[List[str], List[Dict[str, Any]], List[Dict[str, Any]], str]:
    cfg = ApiConfig.from_env(account=account)
    workers = max(1, min(int(os.environ.get("HOOD_UPLOADED_SPLIT_WORKERS", "8")), 32))
    local_items = load_all_items(json_folder=json_folder)
    uploaded: List[str] = []
    not_uploaded: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    uploaded_set: Set[str] = set()

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
            item_number = _normalize_item_number(norm.get("ean") or norm.get("item_number"))
            if not item_number:
                warnings.append(
                    {
                        "reason": "missing_item_number",
                        "local_id": str(raw.get("ID") or raw.get("id") or "").strip() or None,
                    }
                )
                not_uploaded.append(raw)
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
                    }
                )

            if exists:
                if item_number not in uploaded_set:
                    uploaded.append(item_number)
                    uploaded_set.add(item_number)
            else:
                not_uploaded.append(raw)

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

