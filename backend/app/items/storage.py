import json
from pathlib import Path
from typing import Any, Dict, List

from app.config import settings


def _attach_source(obj: Dict[str, Any], file: Path) -> Dict[str, Any]:
    item = dict(obj)
    item["__source_file__"] = str(file)
    item["__source_name__"] = file.name
    return item


def _load_items_from_file_path(file: Path) -> List[Dict[str, Any]]:
    try:
        text = file.read_text(encoding="utf-8")
        data = json.loads(text)
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return []

    if isinstance(data, list):
        return [_attach_source(it, file) for it in data if isinstance(it, dict)]
    if isinstance(data, dict):
        return [_attach_source(data, file)]
    return []


def _get_json_folder() -> Path:
    folder_raw = settings.JSON_FOLDER.strip()
    if not folder_raw:
        raise ValueError("JSON_FOLDER is not configured")
    folder = Path(folder_raw)
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"JSON folder not found: {folder}")
    return folder


def resolve_source_file(source_file: str) -> Path:
    """
    Returns a safe path to a JSON file inside JSON_FOLDER.
    Accepts only plain file names (no directories).
    """
    file_name = Path(source_file or "").name
    if not file_name or file_name != source_file:
        raise ValueError("source_file must be a plain file name")
    if not file_name.lower().endswith(".json"):
        raise ValueError("source_file must end with .json")

    file = _get_json_folder() / file_name
    if not file.exists() or not file.is_file():
        raise FileNotFoundError(file_name)
    return file


def list_json_source_files() -> List[Dict[str, Any]]:
    folder = _get_json_folder()
    files = sorted(folder.glob("*.json"), reverse=True)
    result: List[Dict[str, Any]] = []
    for file in files:
        result.append(
            {
                "file_name": file.name,
                "item_count": len(_load_items_from_file_path(file)),
            }
        )
    return result


def load_items_from_source_file(source_file: str) -> List[Dict[str, Any]]:
    file = resolve_source_file(source_file)
    return _load_items_from_file_path(file)


def load_all_items() -> List[Dict[str, Any]]:
    """
    Loads all items from all JSON files in JSON_FOLDER.
    """
    items: List[Dict[str, Any]] = []
    folder = _get_json_folder()
    files = sorted(folder.glob("*.json"), reverse=True)
    for file in files:
        items.extend(_load_items_from_file_path(file))
    return items


def delete_item_from_source(raw_item: Dict[str, Any]) -> None:
    """
    Deletes an item from its original JSON file.
    Uses '__source_file__' and 'ID'.
    """
    source_path = raw_item.get("__source_file__")
    if not source_path:
        return

    file = Path(source_path)
    if not file.exists():
        return

    try:
        data = json.loads(file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error reading {file} during delete: {e}")
        return

    item_id = raw_item.get("ID")

    changed = False
    if isinstance(data, list):
        if item_id is not None:
            new_data = [it for it in data if it.get("ID") != item_id]
        else:
            new_data = [it for it in data if it != raw_item]
        if len(new_data) != len(data):
            data = new_data
            changed = True
    else:
        if (item_id is not None and data.get("ID") == item_id) or data == raw_item:
            data = []
            changed = True

    if changed:
        try:
            file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Error writing {file} during delete: {e}")
