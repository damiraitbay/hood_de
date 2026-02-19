import asyncio
from typing import List
from app.items.storage import load_all_items
from app.items.utils import normalize_item

from app.hood_service.items import(
    hood_item_exists, 
    hood_upload_item

)

from app.logger import get_logger

logger = get_logger("items")

def get_server_items():
    return load_all_items()

def split_uploaded_items():
    uploaded, not_uploaded = [], []

    for item in load_all_items():
        ref = f"ART{item.get('ID')}"
        if hood_item_exists(ref):
            uploaded.append(ref)
        else:
            not_uploaded.append(item)

    return uploaded, not_uploaded

async def upload_items_async(items: List[dict]):
    semaphore = asyncio.Semaphore(5)
    results = []

    async def worker(item):
        async with semaphore:
            ref = f"ART{item.get('ID')}"
            try:
                normalized = normalize_item(item)
                resp = await asyncio.to_thread(hood_upload_item, normalized)
                logger.info(f"{ref} uploaded")
                return {"reference_id": ref, "status": "ok"}
            except Exception as e:
                logger.error(f"{ref} failed: {e}")
                return {"reference_id": ref, "status": "error", "error": str(e)}

    tasks = [worker(item) for item in items]
    return await asyncio.gather(*tasks)

