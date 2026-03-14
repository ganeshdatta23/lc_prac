import asyncio
import logging
from typing import Optional
from fastapi import FastAPI, BackgroundTasks
import redis.asyncio as redis
import httpx

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379"
BASE_API_URL = "https://api.yourservice.com"

# Every Redis cache key must start with this prefix.
# The remainder of the key is the raw API path + query string.
CACHE_KEY_PREFIX = "cached:"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("CacheWorker")


# ─────────────────────────────────────────────
# APP & REDIS
# ─────────────────────────────────────────────

app = FastAPI(title="Dynamic Cache Worker")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def key_to_url(key: str) -> Optional[str]:
    """
    Derive the downstream API URL directly from a Redis cache key.

    Convention:
        Redis key  ->  "cached:<path>[?<query>]"
        API URL    ->  "<BASE_API_URL>/<path>[?<query>]"


    Returns None if the key does not match the expected prefix.
    """
    if not key.startswith(CACHE_KEY_PREFIX):
        logger.warning(f"Skipping unexpected key format: {key!r}")
        return None

    path = key[len(CACHE_KEY_PREFIX):]   # strip "cached:" prefix
    return f"{BASE_API_URL.rstrip('/')}/{path}"


async def scan_keys_for_main_id(main_id: str) -> list[str]:
    """
    Scan Redis for all cache keys that contain the given main_id.

    Pattern:  cached:*<main_id>*
    Matches any key whose path contains the main_id, e.g.:
    """
    pattern = f"{CACHE_KEY_PREFIX}*{main_id}*"
    return [key async for key in redis_client.scan_iter(match=pattern)]


async def delete_keys(keys: list[str], main_id: str) -> None:
    """Atomically delete keys using UNLINK (non-blocking server-side delete)."""
    if not keys:
        return
    await redis_client.unlink(*keys)
    logger.info(f"[{main_id}] Deleted {len(keys)} keys.")


async def trigger_url(client: httpx.AsyncClient, url: str, main_id: str) -> dict:
    """
    Fire a single GET request to re-populate a cache entry.
    Errors are caught per-URL so one failure does not abort the others.
    """
    try:
        response = await client.get(url)
        logger.info(f"[{main_id}]  {url}  ->  HTTP {response.status_code}")
        return {"url": url, "status": response.status_code, "error": None}
    except Exception as exc:
        logger.error(f"[{main_id}]  {url}  ->  {exc}")
        return {"url": url, "status": None, "error": str(exc)}


# ─────────────────────────────────────────────
# CORE FUNCTION
# ─────────────────────────────────────────────

async def universal_cache_refresh(main_id: str) -> None:
    """
    Full cache refresh lifecycle for a given main_id:

    1. Scan Redis for all keys matching  cached:*<main_id>*
    2. Record matched key names (for logging / audit)
    3. Atomically delete those keys (UNLINK = non-blocking)
    4. Derive API URLs directly from the key names themselves
       (key -> strip "cached:" prefix -> BASE_API_URL + path)
    5. Fire all API calls concurrently using asyncio.gather
       so every downstream endpoint is re-populated in parallel

    Runs as a FastAPI BackgroundTask — HTTP response is already
    sent to the caller before this begins, so it never blocks the API.
    """
    logger.info(f"[{main_id}] Starting cache refresh...")

    # Step 1 & 2: Scan
    matched_keys = await scan_keys_for_main_id(main_id)

    if not matched_keys:
        logger.info(f"[{main_id}] No matching keys found — nothing to refresh.")
        return

    logger.info(f"[{main_id}] Found {len(matched_keys)} keys: {matched_keys}")

    # Step 3: Delete stale keys
    await delete_keys(matched_keys, main_id)

    # Step 4: Derive API URLs from the keys themselves
    urls = []
    for key in matched_keys:
        url = key_to_url(key)
        if url:
            urls.append(url)

    if not urls:
        logger.warning(f"[{main_id}] No valid URLs derived from keys.")
        return

    # Step 5: Re-trigger all URLs concurrently
    async with httpx.AsyncClient(timeout=30.0) as client:
        results = await asyncio.gather(
            *[trigger_url(client, url, main_id) for url in urls],
            return_exceptions=False,  # safe — trigger_url never raises
        )

    success_count = sum(1 for r in results if r["error"] is None)
    logger.info(
        f"[{main_id}] Refresh complete. "
        f"Keys deleted: {len(matched_keys)}, "
        f"URLs triggered: {len(urls)}, "
        f"Successful: {success_count}/{len(urls)}"
    )


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@app.post("/ingest")
async def ingestion_api(
    background_tasks: BackgroundTasks,
    main_id: Optional[str] = None,
):
    """
    Ingestion endpoint.

    Returns 202 immediately and schedules cache refresh in the background.
    The refresh runs after the HTTP response is sent — zero latency impact
    on the caller.
    """
    if not main_id:
        return {"status": "error", "message": "Missing main_id"}

    

    return {
        "status": "accepted",
        "main_id": main_id,
        "message": "Cache refresh scheduled in background.",
    }


@app.get("/health")
async def health():
    try:
        await redis_client.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as exc:
        return {"status": "degraded", "redis": str(exc)}
