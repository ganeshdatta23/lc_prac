import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import urlencode, urljoin

import httpx
from fastapi import BackgroundTasks, FastAPI
import redis.asyncio as redis

REDIS_URL = "redis://localhost:6379"
BASE_API_URL = os.getenv("BASE_API_URL", "http://localhost:8000")
MAX_WORKERS = int(os.getenv("CACHE_REFRESH_WORKERS", "8"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DynamicCacheWorker")

app = FastAPI()
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def _build_refresh_url(record: dict) -> Optional[str]:
    """Reconstruct the API URL using cached metadata.

    Priority order:
    1) Explicit `refresh_url` saved alongside the cached payload.
    2) `path`/`endpoint` + `filters`/`params` to rebuild the URL against BASE_API_URL.
    """

    if not isinstance(record, dict):
        return None

    if record.get("refresh_url"):
        return record["refresh_url"]

    path = record.get("path") or record.get("endpoint") or ""
    filters = record.get("filters") or record.get("params") or {}

    if not path and not filters:
        return None

    path = path.lstrip("/")
    base = urljoin(f"{BASE_API_URL.rstrip('/')}/", path)

    if filters:
        return f"{base}?{urlencode(filters, doseq=True)}"
    return base


def _trigger_url(url: str) -> None:
    """Fire the API call synchronously (executed in a thread)."""
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001 - want to catch/log all failures
        logger.warning("Refresh call failed for %s: %s", url, exc)


async def universal_cache_refresh(main_id: str):
    """Remove old cache entries for main_id and repopulate by re-hitting source APIs."""
    if not main_id:
        return

    matched_keys = []
    urls_to_trigger = set()

    async for key in redis_client.scan_iter(match=f"*{main_id}*"):
        matched_keys.append(key)
        raw_val = await redis_client.get(key)
        if raw_val:
            try:
                data = json.loads(raw_val)
            except json.JSONDecodeError:
                data = None

            url = _build_refresh_url(data) if data else None
            if url:
                urls_to_trigger.add(url)

    if not matched_keys:
        logger.info("No cache keys found for ID %s", main_id)
        return

    logger.debug("Matched keys for %s: %s", main_id, matched_keys)

    await redis_client.unlink(*matched_keys)
    logger.info("Cleaned %s keys for ID %s", len(matched_keys), main_id)

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(executor, _trigger_url, url) for url in urls_to_trigger]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Re-triggered %s unique URLs automatically.", len(tasks))


@app.on_event("shutdown")
async def _shutdown_executor() -> None:
    executor.shutdown(wait=False)


@app.post("/ingest")
async def ingestion_api(
    background_tasks: BackgroundTasks,
    main_id: Optional[str] = None
):
    if not main_id:
        return {"status": "error", "message": "Missing main_id"}

    background_tasks.add_task(universal_cache_refresh, main_id)

    return {"status": "accepted", "main_id": main_id}
