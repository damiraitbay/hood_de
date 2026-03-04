import os
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Set, Tuple

from app.items.storage import load_all_items
from app.items.utils import normalize_item
from app.logger import get_logger
from hood_api.api.parsers import parse_item_list_response
from hood_api.builders import build_item_list
from hood_api.client import send_request
from hood_api.config import ApiConfig

_ITEM_STATUSES: Tuple[str, ...] = ("running", "sold", "unsuccessful")
logger = get_logger("items")


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


def _load_status_reference_ids(
    cfg: ApiConfig,
    item_status: str,
    group_size: int = 500,
) -> Tuple[Set[str], List[Dict[str, Any]], int]:
    status_item_numbers: Set[str] = set()
    warnings: List[Dict[str, Any]] = []
    pages = 0
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
        response_xml = ""
        try:
            response_xml = send_request(xml_body, config=cfg)
            page = parse_item_list_response(response_xml)
            pages += 1
        except ET.ParseError as exc:
            snippet = response_xml[:200].replace("\n", " ").strip()
            warnings.append(
                {
                    "status": item_status,
                    "start_at": start_at,
                    "reason": f"non_xml_response: {exc}",
                    "response_snippet": snippet,
                }
            )
            logger.warning(
                "uploaded_split: parse error for status=%s start_at=%s: %s; snippet=%s",
                item_status,
                start_at,
                exc,
                snippet,
            )
            break
        except Exception as exc:
            warnings.append(
                {
                    "status": item_status,
                    "start_at": start_at,
                    "reason": f"request_error: {exc}",
                }
            )
            logger.warning(
                "uploaded_split: request error for status=%s start_at=%s: %s",
                item_status,
                start_at,
                exc,
            )
            break

        errors = page.get("errors") or []
        if errors:
            warnings.append(
                {
                    "status": item_status,
                    "start_at": start_at,
                    "reason": "api_errors",
                    "errors": [str(err) for err in errors],
                }
            )
            logger.warning(
                "uploaded_split: hood api errors for status=%s start_at=%s: %s",
                item_status,
                start_at,
                "; ".join(str(err) for err in errors),
            )
            break

        items = page.get("items") or []
        for hood_item in items:
            item_number = _normalize_item_number(hood_item.get("itemNumber"))
            if item_number:
                status_item_numbers.add(item_number)

        if not items:
            break

        total_records = int(page.get("total_records") or 0)
        if total_records and start_at + len(items) > total_records:
            break
        effective_group_size = int(page.get("group_size") or 0)
        step = effective_group_size if effective_group_size > 0 else len(items)
        if len(items) < step:
            break
        start_at += step

    return status_item_numbers, warnings, pages


def _load_hood_reference_ids(
    cfg: ApiConfig,
    group_size: int = 500,
    workers: int = 3,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[Set[str], List[Dict[str, Any]]]:
    statuses = list(_ITEM_STATUSES)
    max_workers = max(1, min(int(workers), len(statuses), 8))
    hood_item_numbers: Set[str] = set()
    warnings: List[Dict[str, Any]] = []

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "loading_hood_items",
                "statuses_total": len(statuses),
                "statuses_done": 0,
                "workers": max_workers,
                "hood_item_number_count": 0,
            }
        )

    done_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_load_status_reference_ids, cfg, status_name, group_size): status_name
            for status_name in statuses
        }
        for future in as_completed(futures):
            status_name = futures[future]
            status_item_numbers: Set[str] = set()
            status_warnings: List[Dict[str, Any]] = []
            pages = 0
            try:
                status_item_numbers, status_warnings, pages = future.result()
            except Exception as exc:
                status_warnings = [{"status": status_name, "reason": f"worker_error: {exc}"}]

            hood_item_numbers.update(status_item_numbers)
            warnings.extend(status_warnings)
            done_count += 1

            if progress_cb is not None:
                progress_cb(
                    {
                        "phase": "loading_hood_items",
                        "statuses_total": len(statuses),
                        "statuses_done": done_count,
                        "current_status": status_name,
                        "current_status_pages": pages,
                        "current_status_item_numbers": len(status_item_numbers),
                        "warnings_count": len(warnings),
                        "hood_item_number_count": len(hood_item_numbers),
                        "workers": max_workers,
                    }
                )

    return hood_item_numbers, warnings


def split_uploaded_items(
    account: str | None = None,
    json_folder: str | None = None,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[List[str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    cfg = ApiConfig.from_env(account=account)
    workers = max(1, min(int(os.environ.get("HOOD_UPLOADED_SPLIT_WORKERS", "3")), 8))
    hood_item_numbers, warnings = _load_hood_reference_ids(
        cfg=cfg,
        workers=workers,
        progress_cb=progress_cb,
    )
    local_items = load_all_items(json_folder=json_folder)
    uploaded: List[str] = []
    not_uploaded: List[Dict[str, Any]] = []

    if progress_cb is not None:
        progress_cb(
            {
                "phase": "splitting_local_items",
                "total_items": len(local_items),
                "processed_items": 0,
                "uploaded": 0,
                "not_uploaded": 0,
                "warnings_count": len(warnings),
                "hood_item_number_count": len(hood_item_numbers),
            }
        )

    for idx, item in enumerate(local_items, start=1):
        norm = normalize_item(item)
        item_number = _normalize_item_number(norm.get("item_number") or norm.get("ean"))
        if not item_number:
            not_uploaded.append(item)
        else:
            if item_number in hood_item_numbers:
                uploaded.append(item_number)
            else:
                not_uploaded.append(item)

        if progress_cb is not None and (idx % 200 == 0 or idx == len(local_items)):
            progress_cb(
                {
                    "phase": "splitting_local_items",
                    "total_items": len(local_items),
                    "processed_items": idx,
                    "uploaded": len(uploaded),
                    "not_uploaded": len(not_uploaded),
                    "warnings_count": len(warnings),
                    "hood_item_number_count": len(hood_item_numbers),
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
                "hood_item_number_count": len(hood_item_numbers),
            }
        )

    return uploaded, not_uploaded, warnings

