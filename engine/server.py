"""
FastAPI Server & Route Handlers:
Provides /shopify (single), /batch (bulk), /site_check, /status, and /delcache endpoints.
"""
import asyncio
import logging
import os
import sys
import time
from typing import Optional, List, Dict, Any

try:
    from fastapi import FastAPI, Query, HTTPException, Request
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    class DummyApp:
        def __init__(self, *args, **kwargs): pass
        def get(self, *args, **kwargs): return lambda f: f
        def post(self, *args, **kwargs): return lambda f: f
        def on_event(self, *args, **kwargs): return lambda f: f
        async def __call__(self, scope, receive, send):
            if scope.get('type') == 'http':
                await send({'type': 'http.response.start', 'status': 200, 'headers': [[b'content-type', b'application/json']]})
                await send({'type': 'http.response.body', 'body': b'{"status":"online","service":"shopify_bot_api","version":"2.0"}'})
    FastAPI = DummyApp
    def Query(default=None, **kwargs): return default
    class HTTPException(Exception):
        def __init__(self, status_code=400, detail=None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail
    Request = None
    def JSONResponse(status_code=200, content=None): return content or {}
    class BaseModel:
        def __init__(self, **data):
            for k, v in data.items():
                setattr(self, k, v)
    uvicorn = None
    FASTAPI_AVAILABLE = False

from engine.pipeline import (
    _throttled_process,
    _build_result,
    parse_cc_string,
    _prune_user_semaphores,
    MAX_CONCURRENT,
    ACTIVE_WORKERS,
)
from engine.network import (
    _get_max_per_site,
    _SESSION_POOL,
    _SESSION_POOL_LOCKS,
)
from engine.discovery import (
    fetch_products,
    _VARIANT_CACHE,
)

logger = logging.getLogger("shopi_api.server")

app = FastAPI(title="Shopi Checker API", version="2.0")

# ── Coolify / Docker root health check endpoint ────────────────────
@app.get('/')
async def root_health_check():
    return {"status": "online", "service": "shopify_bot_api", "version": "2.0"}

# ── Single-card endpoint (backward compatible) ──────────────────────
@app.get('/shopify')
async def shopify_checker(
    site: Optional[str] = None,
    url: Optional[str] = None,
    cc: Optional[str] = None,
    proxy: Optional[str] = None,
    variant: Optional[str] = None,
    timeout: Optional[int] = 45,
    kill_mode: Optional[bool] = False,
    check_only: Optional[str] = '0',
    uid: Optional[str] = None,
    bin_country: Optional[str] = None,
):
    try:
        actual_site = site or url
        if not actual_site:
            raise HTTPException(status_code=400, detail={"error": "Missing 'site' parameter", "status": False})
        if not cc:
            raise HTTPException(status_code=400, detail={"error": "Missing 'cc' parameter in format CC|MM|YYYY|CVV", "status": False})
            
        try:
            cc_parts = parse_cc_string(cc)
            if kill_mode:
                import random
                real_cvv = cc_parts['cvv']
                wrong_cvv = real_cvv
                while wrong_cvv == real_cvv:
                    wrong_cvv = str(random.randint(0, (10**len(real_cvv))-1)).zfill(len(real_cvv))
                cc_parts['cvv'] = wrong_cvv
        except ValueError as e:
            raise HTTPException(status_code=400, detail={"error": str(e), "status": False})

        is_check_only = check_only in ('1', 'true', 'True')
        
        logger.info(f"[{cc}] Submitting request...")
        success, message, gateway, price, currency = await _throttled_process(
            cc_parts['cc'], cc_parts['mes'], cc_parts['ano'], cc_parts['cvv'],
            actual_site, variant, proxy, timeout, check_only=is_check_only, uid=uid, bin_country=bin_country
        )
        logger.info(f"[{cc}] Request completed.")
        return _build_result(cc, success, message, gateway, price, currency)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": str(e), "status": False,
            "Gateway": "UNKNOWN", "Price": 0.0,
            "Response": f"ERROR: {str(e)}",
            "cc": cc or ''
        })

# ── Batch endpoint — up to 60 cards concurrently ────────────────────
class BatchRequest(BaseModel):
    site: str
    cards: List[str]
    proxy: Optional[str] = None
    proxies: Optional[List[str]] = None
    variant: Optional[str] = None
    timeout: Optional[int] = 40
    kill_mode: Optional[bool] = False
    uid: Optional[str] = None

@app.post('/batch')
async def batch_checker(data: BatchRequest):
    try:
        site = data.site
        cards = data.cards
        variant_id = data.variant
        uid = data.uid

        proxy_list = data.proxies or []
        single_proxy = data.proxy
        if not proxy_list and single_proxy:
            proxy_list = [single_proxy]

        if not site:
            raise HTTPException(status_code=400, detail={"error": "Missing 'site' field", "status": False})
        if not cards or not isinstance(cards, list):
            raise HTTPException(status_code=400, detail={"error": "Missing or invalid 'cards' array", "status": False})
        if len(cards) > 2500:
            raise HTTPException(status_code=400, detail={"error": "Max 2500 cards per batch request. Please chunk your lists.", "status": False})

        parsed = []
        for i, cc_string in enumerate(cards):
            proxy_for_card = proxy_list[i % len(proxy_list)] if proxy_list else None
            try:
                parts = parse_cc_string(cc_string.strip())
                if data.kill_mode:
                    import random
                    real_cvv = parts['cvv']
                    wrong_cvv = real_cvv
                    while wrong_cvv == real_cvv:
                        wrong_cvv = str(random.randint(0, (10**len(real_cvv))-1)).zfill(len(real_cvv))
                    parts['cvv'] = wrong_cvv
                parsed.append((cc_string.strip(), parts, proxy_for_card))
            except ValueError:
                parsed.append((cc_string.strip(), None, proxy_for_card))

        timeout_sec = data.timeout or 40

        tasks = []
        for cc_string, parts, px in parsed:
            if parts is None:
                async def _bad(cs=cc_string):
                    return cs, False, "Invalid CC format", "UNKNOWN", "0.00", "USD"
                tasks.append(_bad())
            else:
                async def _check(cs=cc_string, p=parts, prx=px):
                    try:
                        success, msg, gw, price, cur = await _throttled_process(
                            p['cc'], p['mes'], p['ano'], p['cvv'],
                            site, variant_id, prx, timeout_sec, uid=uid
                        )
                        return cs, success, msg, gw, price, cur
                    except Exception as ex:
                        return cs, False, str(ex), "UNKNOWN", "0.00", "USD"
                tasks.append(_check())

        results = await asyncio.gather(*tasks)

        output = []
        for cc_string, success, message, gateway, price, currency in results:
            output.append(_build_result(cc_string, success, message, gateway, price, currency))

        return output
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "status": False})

# ── Site-only check endpoint (pre-warms variant cache, no CC needed) ──
@app.get('/site_check')
async def site_check(
    site: Optional[str] = None,
    url: Optional[str] = None,
    proxy: Optional[str] = None,
    timeout: Optional[int] = 20,
):
    try:
        actual_site = site or url
        if not actual_site:
            raise HTTPException(status_code=400, detail={"valid": False, "error": "Missing 'site' parameter"})

        ourl = actual_site if actual_site.startswith('http') else f'https://{actual_site}'

        result = await fetch_products(ourl, proxy, timeout)

        if isinstance(result, tuple) and result[0] is False:
            return {
                "valid": False,
                "site": actual_site,
                "error": str(result[1])
            }

        return {
            "valid": True,
            "site": actual_site,
            "variant_id": result.get('variant_id'),
            "price": result.get('price'),
            "link": result.get('link'),
            "currency": result.get('currency', 'USD'),
            "requires_shipping": result.get('requires_shipping', False),
            "gateway": result.get('gateway', 'shopify')
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"valid": False, "site": site or url or '', "error": str(e)})


# ── Clear Cache endpoint ──────────────────────────────────────────────
@app.get('/delcache')
async def clear_cache():
    global _VARIANT_CACHE
    cleared = len(_VARIANT_CACHE)
    _VARIANT_CACHE.clear()
    return {"status": "success", "cleared": cleared}

# ── Status endpoint ─────────────────────────────────────────────────
@app.get('/status')
async def status():
    cached_count = len(_VARIANT_CACHE)
    pool_stats = {k: len(v) for k, v in _SESSION_POOL.items()}
    pool_total = sum(pool_stats.values())

    sys_stats = {}
    try:
        import psutil, time as _time
        sys_stats["cpu_usage"] = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory()
        sys_stats["ram_percent"] = ram.percent
        sys_stats["ram_used_gb"] = round(ram.used / (1024**3), 1)
        sys_stats["ram_total_gb"] = round(ram.total / (1024**3), 1)
        uptime_seconds = _time.time() - psutil.boot_time()
        days, rem = divmod(uptime_seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        sys_stats["uptime_str"] = f"{int(days)}d {int(hours)}h {int(minutes)}m"
    except Exception as e:
        sys_stats["error"] = str(e)

    return {
        "status": "online",
        "max_concurrent": MAX_CONCURRENT,
        "active_workers": ACTIVE_WORKERS,
        "max_per_site": _get_max_per_site(),
        "variant_cache_size": cached_count,
        "session_pool": pool_stats,
        "session_pool_total_idle": pool_total,
        "system_specs": sys_stats,
        "hardware_profile": __import__("engine.system_tuner", fromlist=["get_hardware_profile"]).get_hardware_profile(),
        "endpoints": {
            "single":     "GET /shopify?site=...&cc=...&proxy=...",
            "batch":      "POST /batch {site, cards[], proxy}",
            "site_check": "GET /site_check?site=...&proxy=... (pre-warm cache, no CC)",
            "status":     "GET /status"
        }
    }

@app.on_event("startup")
async def startup_event():
    try:
        from engine.system_tuner import apply_hardware_optimizations
        apply_hardware_optimizations()
    except Exception as e:
        logger.debug(f"Hardware tuner error: {e}")
    logger.info(f"[ENGINE] Starting up — Max concurrent: {MAX_CONCURRENT}")
    logger.info("[ENGINE] Single:     GET /shopify?site=...&cc=...&proxy=...")
    logger.info("[ENGINE] Batch:      POST /batch  {site, cards[], proxy}")
    logger.info("[ENGINE] Site-check: GET /site_check?site=...&proxy=... (pre-warm variant cache)")
    # Pre-init session pool locks
    global _SESSION_POOL_LOCKS
    _SESSION_POOL_LOCKS = {}
    # Start pruner
    asyncio.create_task(_prune_user_semaphores())
    logger.info("[ENGINE] Memory manager started — GC every 60s, site semaphore TTL 10min")

if __name__ == "__main__":
    if not FASTAPI_AVAILABLE:
        print("[ERROR] FastAPI and uvicorn are required to run shopi_api_v2 as a standalone server.")
        print("Please run: pip install fastapi uvicorn pydantic")
        sys.exit(1)
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(
        "shopi_api_v2:app",
        host="0.0.0.0",
        port=port,
        loop="auto",
        workers=1,
        access_log=False,
    )

