"""
Shopify Checker API v2 — Primary Entry Point & Facade

This module serves as the stable, backward-compatible top-level interface
for the modular `engine/` package. All consumers (Telegram Bot, Desktop UI,
Docker container, and batch scripts) continue to import or run `shopi_api_v2`
without any modifications.
"""
import sys
import os

# Re-export core engine components
from engine.config import (
    _BROWSER_PROFILES,
    _pick_browser_profile,
    book,
    C2C,
    _US_ADDRESSES,
    Utils,
    extract_between,
    generate_valid_phone,
    _get_alternate_phone,
    bin_info_for,
    bin_info_for_async,
)
from engine.network import (
    AiohttpCurlCffiSession,
    parse_proxy,
    extract_session_token,
    is_captcha_required,
    make_graphql_request_with_captcha_handling,
    _get_site_semaphore,
    _get_fallback_proxies,
    _rotate_fallback_proxy,
    _SESSION_POOL,
)
from engine.discovery import (
    _VARIANT_CACHE,
    fetch_products,
    prune_variant_cache,
    normalize_cache_key,
    extract_clean_response,
)
from engine.tokenization import tokenize_card_shopify
from engine.pipeline import (
    process_card,
    _process_card_inner,
    _throttled_process,
    parse_cc_string,
    _build_result,
    MAX_CONCURRENT,
    ACTIVE_WORKERS,
)
from engine.server import (
    app,
    shopify_checker,
    batch_checker,
    site_check,
    clear_cache,
    status,
    FASTAPI_AVAILABLE,
    uvicorn,
)

__all__ = [
    "app",
    "FASTAPI_AVAILABLE",
    "process_card",
    "_process_card_inner",
    "_throttled_process",
    "_build_result",
    "parse_cc_string",
    "fetch_products",
    "_VARIANT_CACHE",
    "prune_variant_cache",
    "normalize_cache_key",
    "extract_clean_response",
    "tokenize_card_shopify",
    "book",
    "C2C",
    "_US_ADDRESSES",
    "Utils",
    "bin_info_for",
    "bin_info_for_async",
    "parse_proxy",
    "_get_fallback_proxies",
    "_rotate_fallback_proxy",
    "MAX_CONCURRENT",
    "ACTIVE_WORKERS",
]

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
