"""
Network utilities: DNS caching, curl_cffi wrappers, session pooling,
proxy rotation, site semaphores, and GraphQL request helpers.
"""
import asyncio
import aiohttp
import socket
import time
import random
import re
import os
import sys
import json
import logging
from typing import Optional, Tuple, Dict, Any, List
from urllib.parse import urlparse
from curl_cffi.requests import AsyncSession

from engine.config import _order_headers_like_chrome, _pick_browser_profile, extract_between

logger = logging.getLogger("shopi_api.network")

# Native OS thread-safe DNS resolution used (prevents thread pool deadlocks in Docker/Coolify)



class AiohttpCurlCffiResponseContextManager:
    def __init__(self, coro):
        self.coro = coro
        self.resp = None

    def __await__(self):
        async def _wrapper():
            self.resp = await self.coro
            return AiohttpCurlCffiResponse(self.resp)
        return _wrapper().__await__()

    async def __aenter__(self):
        self.resp = await self.coro
        return AiohttpCurlCffiResponse(self.resp)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class AiohttpCurlCffiResponse:
    def __init__(self, resp):
        self._resp = resp
        self.status = resp.status_code
        self.status_code = resp.status_code
        self.headers = resp.headers
        self.url = str(resp.url)

    def close(self):
        pass

    async def text(self):
        return self._resp.text

    async def json(self):
        return self._resp.json()

    async def read(self):
        return self._resp.content
        
    def raise_for_status(self):
        if self.status >= 400:
            raise Exception(f"HTTP Error {self.status}")

# ── Advanced Browser Profile System ─────────────────────────────────


# ── curl_cffi Session Pool ───────────────────────────────────────────
# Reuse AsyncSession objects instead of creating a new one per card check.
# curl_cffi sessions maintain internal connection pools (keep-alive), so
# reusing them avoids cold TLS handshakes and dramatically reduces
# errors under high concurrency.
#
# Pool is keyed by impersonate profile. Max pool size per profile = 30.
# Sessions are checked out (pop) and returned (append) after each use.
# If pool is empty a fresh session is created on demand.

_SESSION_POOL: dict = {}          # profile_name -> list[AsyncSession]
_SESSION_POOL_LOCKS: dict = {}    # profile_name -> asyncio.Lock
try:
    from engine.system_tuner import get_hardware_profile
    _SESSION_POOL_MAX = get_hardware_profile().get("session_pool_max", 100)
except Exception:
    _SESSION_POOL_MAX = 100

def _get_pool_lock(impersonate: str) -> asyncio.Lock:
    """Get or lazily create an asyncio.Lock for the given impersonate profile."""
    if impersonate not in _SESSION_POOL_LOCKS:
        _SESSION_POOL_LOCKS[impersonate] = asyncio.Lock()
    return _SESSION_POOL_LOCKS[impersonate]

async def _get_pooled_session(impersonate: str) -> 'AsyncSession':
    """Check out a reusable AsyncSession from the pool, or create one."""
    lock = _get_pool_lock(impersonate)
    async with lock:
        pool = _SESSION_POOL.setdefault(impersonate, [])
        while pool:
            session = pool.pop()
            req_count = getattr(session, '_req_count', 0)
            if req_count >= 20:
                try:
                    await session.close()
                except Exception:
                    pass
                continue
            try:
                session.cookies.clear()
                if hasattr(session, '_shopify_prewarmed'):
                    delattr(session, '_shopify_prewarmed')
            except Exception:
                pass
            session._req_count = req_count + 1
            return session
    # default_headers must be a boolean in curl_cffi 0.15.0 to avoid TypeError in impersonate
    s = AsyncSession(impersonate=impersonate, default_headers=False)
    s._req_count = 1
    return s

async def _return_pooled_session(session: 'AsyncSession', impersonate: str):
    """Return a session to the pool, or close it if pool is full or session has reached its request limit."""
    req_count = getattr(session, '_req_count', 0)
    if req_count >= 20:
        try:
            await session.close()
        except Exception:
            pass
        return

    lock = _get_pool_lock(impersonate)
    async with lock:
        pool = _SESSION_POOL.setdefault(impersonate, [])
        if len(pool) < _SESSION_POOL_MAX:
            pool.append(session)
            return
    # Pool full — close the session to release resources
    try:
        await session.close()
    except Exception:
        pass

class AiohttpCurlCffiSession:
    def __init__(self, connector=None, connector_owner=False, timeout=None, browser_profile=None):
        self._profile = browser_profile or _pick_browser_profile()
        self.impersonate = self._profile["impersonate"]
        self.session = None
        self._from_pool = False
        self.timeout_sec = 18  # default request-level timeout (fast fail-safe)
        if timeout:
            if hasattr(timeout, 'sock_read') and timeout.sock_read is not None:
                self.timeout_sec = timeout.sock_read
            elif hasattr(timeout, 'total') and timeout.total is not None:
                self.timeout_sec = min(18, timeout.total)
            elif isinstance(timeout, (int, float)):
                self.timeout_sec = timeout
    
    @property
    def browser_profile(self):
        return self._profile

    async def __aenter__(self):
        self.session = await _get_pooled_session(self.impersonate)
        self._from_pool = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type is None:
                # No exception — return session to pool for reuse
                await _return_pooled_session(self.session, self.impersonate)
            else:
                # Exception occurred — close to avoid corrupted state
                try:
                    await self.session.close()
                except Exception:
                    pass
            self.session = None

    def _convert_kwargs(self, kwargs):
        new_kwargs = kwargs.copy()
        if "headers" in new_kwargs and isinstance(new_kwargs["headers"], dict):
            new_kwargs["headers"] = _order_headers_like_chrome(new_kwargs["headers"])
        if "proxy" in new_kwargs:
            proxy = new_kwargs.pop("proxy")
            if proxy:
                if not proxy.startswith('http') and not proxy.startswith('socks'):
                    proxy = f"http://{proxy}"
                new_kwargs["proxies"] = {"http": proxy, "https": proxy}
        if "data" in new_kwargs and isinstance(new_kwargs["data"], dict):
            pass 
        if "timeout" in new_kwargs:
            t = new_kwargs["timeout"]
            if hasattr(t, 'sock_read') and t.sock_read is not None:
                new_kwargs["timeout"] = t.sock_read
            elif hasattr(t, 'total') and t.total is not None:
                new_kwargs["timeout"] = t.total
        return new_kwargs

    def get(self, url, **kwargs):
        converted = self._convert_kwargs(kwargs)
        if "timeout" not in converted:
            converted["timeout"] = self.timeout_sec
        return AiohttpCurlCffiResponseContextManager(self.session.get(url, **converted))
    
    def post(self, url, **kwargs):
        converted = self._convert_kwargs(kwargs)
        if "timeout" not in converted:
            converted["timeout"] = self.timeout_sec
        return AiohttpCurlCffiResponseContextManager(self.session.post(url, **converted))

    def head(self, url, **kwargs):
        converted = self._convert_kwargs(kwargs)
        if "timeout" not in converted:
            converted["timeout"] = self.timeout_sec
        return AiohttpCurlCffiResponseContextManager(self.session.head(url, **converted))

import sys
import requests
import time
import threading

# ── Feature #3: Chrome-like HTTP Header Ordering ───────────────────────


def extract_session_token(text: str, headers) -> str:
    import re
    # 1. From headers
    sst = headers.get('X-Checkout-One-Session-Token') or headers.get('x-checkout-one-session-token')
    if sst:
        return sst
        
    if not text:
        return None
        
    # 2. Try common extract_between patterns first (fast)
    sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
    if sst: return sst
    sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
    if sst: return sst
    sst = extract_between(text, '"serializedSessionToken":"', '"')
    if sst: return sst
    sst = extract_between(text, 'data-session-token="', '"')
    if sst: return sst
    sst = extract_between(text, '"sessionToken":"', '"')
    if sst: return sst

    # 3. Robust regex-based searches (including unescaped/escaped forms)
    match = re.search(r'"serializedSessionToken"\s*:\s*"([^"]+)"', text)
    if match: return match.group(1)
    
    match = re.search(r'"sessionToken"\s*:\s*"([^"]+)"', text)
    if match: return match.group(1)
    
    match = re.search(r'sessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)
    
    match = re.search(r'serializedSessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)
    
    # Try with HTML entities &quot;
    match = re.search(r'sessionToken&quot;\s*:\s*&quot;([^&"]+)&quot;', text)
    if match: return match.group(1)
    
    match = re.search(r'serializedSessionToken&quot;\s*:\s*&quot;([^&"]+)&quot;', text)
    if match: return match.group(1)

    # Search inside window.ShopifyConfig or window.serializedSessionToken
    match = re.search(r'window\.serializedSessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)
    
    match = re.search(r'window\.sessionToken\s*=\s*["\']([^"\']+)["\']', text)
    if match: return match.group(1)

    return None



def _get_fallback_proxies(uid=None):
    import os

    # 2. Try global/system proxy files
    try:
        from bot.core.config import PROXIES_FILE, ST_PROXIES_FILE, GW_PROXIES_FILE
    except ImportError:
        # Standalone fallback
        DATA_DIR = os.getenv('DATA_DIR', '/data')
        if not os.path.exists(DATA_DIR):
            DATA_DIR = os.path.dirname(os.path.abspath(__file__))
        PROXIES_FILE = os.path.join(DATA_DIR, 'proxies.txt')
        ST_PROXIES_FILE = os.path.join(DATA_DIR, 'st_proxies.txt')
        GW_PROXIES_FILE = os.path.join(DATA_DIR, 'gw_proxies.txt')

    proxy_files = [PROXIES_FILE, ST_PROXIES_FILE, GW_PROXIES_FILE]
    all_proxies = []
    
    for f_path in proxy_files:
        if os.path.exists(f_path):
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            parsed = parse_proxy(line)
                            if parsed and "example.com" not in parsed:
                                all_proxies.append(parsed)
            except Exception:
                pass
    return all_proxies

def parse_proxy(proxy_str):
    if not proxy_str:
        return None
    proxy_str = proxy_str.replace(" ", "").strip()
    # If the user explicitly provided a protocol scheme, use it directly
    if proxy_str.lower().startswith(('http://', 'https://', 'socks5://', 'socks5h://', 'socks4://', 'socks4a://', 'socks://')):
        return proxy_str
    if '@' in proxy_str:
        return f"http://{proxy_str}"
    parts = proxy_str.split(':')
    if len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    elif len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    else:
        return f"http://{proxy_str}" if proxy_str else None

format_proxy = parse_proxy

def _get_fallback_proxies(uid=None):
    import os

    try:
        from bot.core.config import PROXIES_FILE, ST_PROXIES_FILE, GW_PROXIES_FILE
    except ImportError:
        DATA_DIR = os.getenv('DATA_DIR', '/data')
        if not os.path.exists(DATA_DIR):
            DATA_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        PROXIES_FILE = os.path.join(DATA_DIR, 'proxies.txt')
        ST_PROXIES_FILE = os.path.join(DATA_DIR, 'st_proxies.txt')
        GW_PROXIES_FILE = os.path.join(DATA_DIR, 'gw_proxies.txt')

    proxy_files = [PROXIES_FILE, ST_PROXIES_FILE, GW_PROXIES_FILE]
    all_proxies = []
    
    for f_path in proxy_files:
        if os.path.exists(f_path):
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            parsed = parse_proxy(line)
                            if parsed and "example.com" not in parsed:
                                all_proxies.append(parsed)
            except Exception:
                pass
    return all_proxies

def _rotate_fallback_proxy(current_proxy: str = None) -> str:
    """Return a different proxy from the fallback pool to use after a captcha/block.
    Never returns the same proxy that is currently flagged."""
    try:
        candidates = _get_fallback_proxies()
        if not candidates:
            return None
        # Filter out the flagged proxy so we always get a fresh one
        clean = [p for p in candidates if p != current_proxy]
        if not clean:
            clean = candidates  # all proxies same? still try a random one
        return random.choice(clean)
    except Exception:
        return None

def is_captcha_required(response_text):
    if not response_text:
        return False
    lower = response_text.lower()
    indicators = [
        'captcha_required',
        'recaptcha',
        'hcaptcha',
        'g-recaptcha',
        'shopify-challenge',
        'challenge-form',
        'cf-challenge',
        'window._cf_chl_opt',
        'shopify_recaptcha',
        'recaptchav2',
        # JSON-level hCaptcha: only match when provider is explicitly hcaptcha
        '"provider":"hcaptcha"',
    ]
    if any(ind in lower for ind in indicators):
        return True
    if '/challenge' in lower or 'action="/challenge"' in lower:
        return True
    return False

# Network errors that warrant a retry (curl_cffi error codes)
_CURL_RETRY_ERRORS = (
    'curl: (56)', 'curl: (52)', 'curl: (35)', 'curl: (28)', 'curl: (7)',
    'curl: (18)', 'curl: (92)', 'curl: (55)',
    'failure in receiving', 'receiving network data', 'without response',
    'connection reset', 'connection timed out', 'connection refused',
    'failed to perform', 'empty reply', 'network error', 'ssl handshake',
    'eof occurred', 'remote end closed', 'broken pipe', 'transfer closed',
)

async def make_graphql_request_with_captcha_handling(
    session, graphql_url, params, headers, json_data,
    checkout_url, max_retries=0, solve_captcha=True, proxy=None
):
    # Always allow at least 3 internal attempts for transient curl/network errors
    _internal_max = max(max_retries, 2)
    response = None
    response_text = ''
    for attempt in range(_internal_max + 1):
        try:
            response = await session.post(graphql_url, params=params, headers=headers, json=json_data, proxy=proxy)
            response_text = await response.text()
            return response, response_text, False
        except Exception as e:
            err_str = str(e).lower()
            is_curl_error = any(marker in err_str for marker in _CURL_RETRY_ERRORS)
            if attempt < _internal_max and is_curl_error:
                # Exponential backoff for curl/network errors
                wait = min(0.5 * (2 ** attempt), 3.0)
                await asyncio.sleep(wait)
                continue
            if attempt >= _internal_max:
                return None, str(e), False
            await asyncio.sleep(random.uniform(0.3, 0.8))

    return response, response_text, False

_global_connector = None
_global_connector_loop = None

# ── Per-site concurrency limiter ────────────────────────────────────
# Prevents hammering the same Shopify store with too many checkouts
_PER_SITE_SEMAPHORES = {}    # domain -> asyncio.Semaphore
_PER_SITE_LAST_SEEN = {}     # domain -> timestamp (for leak cleanup)
_PER_SITE_LOCK = None        # asyncio.Lock

def _get_max_per_site():
    try:
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from bot.core.config import bot_settings
        return int(bot_settings.get("max_per_site", 150))
    except Exception:
        return 150

async def _get_site_semaphore(domain):
    """Get or create a per-site semaphore to limit concurrent checkouts."""
    global _PER_SITE_LOCK
    if _PER_SITE_LOCK is None:
        _PER_SITE_LOCK = asyncio.Lock()
    async with _PER_SITE_LOCK:
        if domain not in _PER_SITE_SEMAPHORES:
            limit = _get_max_per_site()
            _PER_SITE_SEMAPHORES[domain] = asyncio.Semaphore(limit)
        _PER_SITE_LAST_SEEN[domain] = time.time()
        return _PER_SITE_SEMAPHORES[domain]

def get_global_connector():
    global _global_connector, _global_connector_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if (_global_connector is None or
        _global_connector.closed or
        _global_connector_loop is not current_loop):
        ssl_ctx = None if os.environ.get("VERIFY_SSL", "0") in ("1", "true", "True") else False
        _global_connector = aiohttp.TCPConnector(
            ssl=ssl_ctx,
            limit=2000,            # high-capacity connection pool for high concurrency
            limit_per_host=100,    # higher per-host socket ceiling
            use_dns_cache=True,
            ttl_dns_cache=600,     # 10-min DNS cache
            keepalive_timeout=30,  # keep sockets warm for reuse
            enable_cleanup_closed=True,
        )
        _global_connector_loop = current_loop
    return _global_connector



