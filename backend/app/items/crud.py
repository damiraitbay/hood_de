from typing import Any, Dict, List, Set, Tuple

from app.items.storage import load_all_items
from hood_api.api.parsers import parse_item_list_response
from hood_api.builders import build_item_list
from hood_api.client import send_request
from hood_api.config import ApiConfig

_ITEM_STATUSES: Tuple[str, ...] = ("running", "sold", "unsuccessful")


def get_server_items(json_folder: str | None = None) -> List[Dict[str, Any]]:
    return load_all_items(json_folder=json_folder)


def _load_hood_reference_ids(cfg: ApiConfig, group_size: int = 500) -> Set[str]:
    reference_ids: Set[str] = set()

    for item_status in _ITEM_STATUSES:
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
            response_xml = send_request(xml_body, config=cfg)
            page = parse_item_list_response(response_xml)
            errors = page.get("errors") or []
            if errors:
                raise RuntimeError("; ".join(str(err) for err in errors))

            items = page.get("items") or []
            for hood_item in items:
                ref = str(hood_item.get("referenceID") or "").strip()
                if ref:
                    reference_ids.add(ref)

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

    return reference_ids


def split_uploaded_items(
    account: str | None = None,
    json_folder: str | None = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    cfg = ApiConfig.from_env(account=account)
    hood_reference_ids = _load_hood_reference_ids(cfg=cfg)
    uploaded: List[str] = []
    not_uploaded: List[Dict[str, Any]] = []

    for item in load_all_items(json_folder=json_folder):
        raw_id = str(item.get("ID") or item.get("id") or "").strip()
        if not raw_id:
            not_uploaded.append(item)
            continue

        ref = f"ART{raw_id}"
        if ref in hood_reference_ids:
            uploaded.append(ref)
        else:
            not_uploaded.append(item)

    return uploaded, not_uploaded

