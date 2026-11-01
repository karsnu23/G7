"""
Product Discovery & Variant Scraper:
Handles catalog inspection (/products.json), HTML scraping fallback,
variant cache management, and response cleaning.
"""
import asyncio
import aiohttp
import random
import re
import json
import time
import logging
from typing import Optional, Tuple, Dict, Any, Union

from engine.network import get_global_connector, parse_proxy, AiohttpCurlCffiSession, _CURL_RETRY_ERRORS
from engine.config import _pick_browser_profile, get_exchange_rate, convert_to_usd, EXCHANGE_RATES

logger = logging.getLogger("shopi_api.discovery")

_VARIANT_CACHE = {}

_SKIP_KEYWORDS = {
    'gift card', 'gift cards', 'gift_card', 'gift_cards', 'e-gift card',
    'egift card', 'e-gift', 'egift', 'gift certificate', 'gift voucher',
    'donation', 'donations', 'tip', 'tips', 'custom tip', 'tip jar',
    'warranty', 'extended warranty', 'route package protection', 'route protection',
    'navidium', 'package protection', 'shipping protection', 'shipping insurance',
    'insurance', 'membership', 'subscription', 'monthly subscription',
    'annual subscription', 'store credit'
}

def prune_variant_cache():
    global _VARIANT_CACHE
    now = time.time()
    # 1. Clean entries that have exceeded 2x their TTL (allow stale fallback up to 2x TTL)
    expired_keys = []
    for k, v in list(_VARIANT_CACHE.items()):
        try:
            ttl = v[5] if len(v) > 5 else 7200
            if (now - v[1]) > ttl * 2:
                expired_keys.append(k)
        except Exception:
            expired_keys.append(k)
    for k in expired_keys:
        _VARIANT_CACHE.pop(k, None)

    # 2. Soft eviction: if still > 500, evict oldest 20%
    if len(_VARIANT_CACHE) > 500:
        sorted_keys = sorted(
            _VARIANT_CACHE.keys(),
            key=lambda k: _VARIANT_CACHE[k][1] if (isinstance(_VARIANT_CACHE[k], (tuple, list)) and len(_VARIANT_CACHE[k]) > 1) else 0
        )
        num_to_remove = max(1, int(len(sorted_keys) * 0.2))
        for k in sorted_keys[:num_to_remove]:
            _VARIANT_CACHE.pop(k, None)

def normalize_cache_key(url):
    if not url:
        return ""
    url = url.strip().lower()
    if not url.startswith('http'):
        url = "https://" + url
    return url.rstrip('/')

async def fetch_products(domain, proxy_str=None, timeout_sec=20):
    try:
        if not domain.startswith('http'):
            domain = "https://" + domain
            
        # ── 0. High-Speed Memory Cache Fast-Path (0ms Network Latency) ──
        cache_key = normalize_cache_key(domain)
        cached = _VARIANT_CACHE.get(cache_key)
        if cached:
            now = time.time()
            ttl = cached[5] if len(cached) > 5 else 7200
            if (now - cached[1]) < ttl:
                usd_p = cached[4] if len(cached) > 4 else 0.0
                curr = cached[3] if len(cached) > 3 else "USD"
                req_ship = cached[2] if len(cached) > 2 else False
                vid = cached[0]
                return {
                    'site': domain,
                    'price': f"{usd_p:.2f}",
                    'usd_price': usd_p,
                    'variant_id': str(vid),
                    'link': f"{domain}/products",
                    'requires_shipping': req_ship,
                    'currency': curr,
                    'title': 'Cached Product',
                }
        
        connector = get_global_connector()
        eff_timeout = min(12, timeout_sec)
        timeout = aiohttp.ClientTimeout(total=eff_timeout)
        proxy = parse_proxy(proxy_str) if proxy_str else None
        _json = json
        
        result = []
        last_err = ""
        currency = "USD"
        
        async with AiohttpCurlCffiSession(connector=connector, connector_owner=False, timeout=timeout) as session:
            _bp = session._profile
            
            api_headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'br, zstd, gzip, deflate',
                'Accept-Language': _bp.get('accept_language', 'en-US,en;q=0.9'),
                'Connection': 'keep-alive',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'Referer': domain + '/',
            }
            if _bp.get('ua'):
                api_headers['User-Agent'] = _bp['ua']
            if _bp.get('sec_ch_ua'):
                api_headers['sec-ch-ua'] = _bp['sec_ch_ua']
                api_headers['sec-ch-ua-mobile'] = _bp['sec_ch_ua_mobile']
                api_headers['sec-ch-ua-platform'] = _bp['sec_ch_ua_platform']

            _PRODUCTS_FIELDS = "id,handle,title,variants"

            def _safe_parse_products(text):
                try:
                    if not text or '<html' in text[:200].lower():
                        return []
                    data = _json.loads(text)
                    return data.get('products', []) if isinstance(data, dict) else []
                except Exception:
                    return []

            def _has_cheap_product(prods, max_price=10.0):
                for p in prods:
                    for v in p.get('variants', []):
                        try:
                            pv = float(str(v.get('price', '999')).replace(',', ''))
                            if 0.1 <= pv <= max_price:
                                return True
                        except (ValueError, TypeError):
                            pass
                return False

            # ── 1. Lowest Price Sorting Discovery: /collections/all/products.json?sort_by=price-ascending ──
            try:
                col_url = f"{domain}/collections/all/products.json?sort_by=price-ascending&limit=50&fields={_PRODUCTS_FIELDS}"
                async with session.get(
                    col_url,
                    proxy=proxy,
                    timeout=aiohttp.ClientTimeout(total=min(6, timeout_sec)),
                    headers=api_headers,
                ) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if "shopify" in text.lower() or "products" in text.lower():
                            col_p1 = _safe_parse_products(text)
                            if col_p1:
                                result.extend(col_p1)
                                # Deep Catalog Multi-Page Discovery:
                                # If page 1 returned 50 items and no item <= $10 USD found, fetch pages 2 & 3 in parallel!
                                if len(col_p1) >= 50 and not _has_cheap_product(col_p1, max_price=10.0):
                                    async def _fetch_col_page(p):
                                        try:
                                            async with session.get(
                                                f"{domain}/collections/all/products.json?sort_by=price-ascending&page={p}&limit=50&fields={_PRODUCTS_FIELDS}",
                                                proxy=proxy,
                                                timeout=aiohttp.ClientTimeout(total=min(5, timeout_sec)),
                                                headers=api_headers,
                                            ) as p_resp:
                                                if p_resp.status == 200:
                                                    return _safe_parse_products(await p_resp.text())
                                        except Exception:
                                            pass
                                        return []

                                    p2_res, p3_res = await asyncio.gather(_fetch_col_page(2), _fetch_col_page(3))
                                    if p2_res:
                                        result.extend(p2_res)
                                    if p3_res:
                                        result.extend(p3_res)
                    elif resp.status in (400, 404):
                        async with session.get(
                            f"{domain}/collections/all/products.json?sort_by=price-ascending&limit=30",
                            proxy=proxy,
                            timeout=aiohttp.ClientTimeout(total=min(4, timeout_sec)),
                            headers=api_headers,
                        ) as resp_fb:
                            if resp_fb.status == 200:
                                result.extend(_safe_parse_products(await resp_fb.text()))
            except Exception as e:
                last_err = f"col_error: {str(e)}"

            # ── 2. Fallback / Enhancement: Storefront GraphQL API (Guaranteed Lowest Price Ascending) ──
            if not result or not _has_cheap_product(result, max_price=10.0):
                try:
                    gql_payload = _json.dumps({
                        "query": """{ products(first: 20, sortKey: PRICE, reverse: false) { edges { node {
                            handle
                            title
                            productType
                            variants(first: 5) { edges { node {
                                id
                                price { amount currencyCode }
                                requiresShipping
                                availableForSale
                            } } }
                        } } } }"""
                    })
                    gql_headers = {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'Accept-Encoding': 'br, zstd, gzip, deflate',
                        'Accept-Language': _bp.get('accept_language', 'en-US,en;q=0.9'),
                        'Origin': domain,
                        'Referer': domain + '/',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                    }
                    if _bp.get('ua'):
                        gql_headers['User-Agent'] = _bp['ua']
                    async with session.post(
                        f"{domain}/api/2024-10/graphql.json",
                        data=gql_payload,
                        proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=min(5, timeout_sec)),
                        headers=gql_headers,
                    ) as gql_resp:
                        if gql_resp.status == 200:
                            gql_data = _json.loads(await gql_resp.text())
                            edges = (gql_data.get('data') or {}).get('products', {}).get('edges', [])
                            for edge in edges:
                                node = edge.get('node', {})
                                handle = node.get('handle', '')
                                title = node.get('title', '')
                                ptype = node.get('productType', '')
                                v_edges = node.get('variants', {}).get('edges', [])
                                prod_variants = []
                                for ve in v_edges:
                                    v = ve.get('node', {})
                                    if not v.get('availableForSale', True):
                                        continue
                                    raw_id = v.get('id', '')
                                    variant_id = raw_id.split('/')[-1] if '/' in raw_id else raw_id
                                    price_info = v.get('price') or {}
                                    price_val = price_info.get('amount', '0')
                                    curr = price_info.get('currencyCode')
                                    if curr:
                                        currency = curr
                                    prod_variants.append({
                                        'id': variant_id,
                                        'price': str(price_val),
                                        'available': True,
                                        'requires_shipping': v.get('requiresShipping', True),
                                    })
                                if prod_variants:
                                    result.append({
                                        'handle': handle,
                                        'title': title,
                                        'product_type': ptype,
                                        'requires_shipping': prod_variants[0].get('requires_shipping', True),
                                        'variants': prod_variants,
                                        '_from_storefront_gql': True,
                                    })
                except Exception as e:
                    last_err = f"gql_error: {str(e)}"

            # ── 3. Fallback: Standard /products.json (with Multi-Page Parallel Discovery) ──
            if not result:
                try:
                    async with session.get(
                        f"{domain}/products.json?limit=50&fields={_PRODUCTS_FIELDS}",
                        proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=min(5, timeout_sec)),
                        headers=api_headers,
                    ) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            if "shopify" in text.lower() or "products" in text.lower():
                                std_p1 = _safe_parse_products(text)
                                if std_p1:
                                    result.extend(std_p1)
                                    if len(std_p1) >= 50 and not _has_cheap_product(std_p1, max_price=10.0):
                                        async def _fetch_std_page(p):
                                            try:
                                                async with session.get(
                                                    f"{domain}/products.json?page={p}&limit=50&fields={_PRODUCTS_FIELDS}",
                                                    proxy=proxy,
                                                    timeout=aiohttp.ClientTimeout(total=min(5, timeout_sec)),
                                                    headers=api_headers,
                                                ) as p_resp:
                                                    if p_resp.status == 200:
                                                        return _safe_parse_products(await p_resp.text())
                                            except Exception:
                                                pass
                                            return []
                                        p2_res, p3_res = await asyncio.gather(_fetch_std_page(2), _fetch_std_page(3))
                                        if p2_res:
                                            result.extend(p2_res)
                                        if p3_res:
                                            result.extend(p3_res)
                        elif resp.status in (400, 404):
                            async with session.get(
                                f"{domain}/products.json?limit=30",
                                proxy=proxy,
                                timeout=aiohttp.ClientTimeout(total=min(4, timeout_sec)),
                                headers=api_headers,
                            ) as resp_fb:
                                if resp_fb.status == 200:
                                    result.extend(_safe_parse_products(await resp_fb.text()))
                except Exception as e:
                    last_err = f"std_error: {str(e)}"

            # ── 4. Fallback: Predictive Search → product.js ──
            if not result:
                try:
                    search_url = f"{domain}/search/suggest.json?q=a&resources[type]=product&resources[limit]=5"
                    async with session.get(
                        search_url, proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=min(5, timeout_sec)),
                        headers=api_headers,
                    ) as resp:
                        if resp.status == 200:
                            search_data = _json.loads(await resp.text())
                            products = search_data.get('resources', {}).get('results', {}).get('products', [])
                            for p in products[:3]:
                                url_path = p.get('url', '')
                                handle = None
                                if '/products/' in url_path:
                                    handle = url_path.split('/products/')[-1].split('?')[0]
                                elif p.get('handle'):
                                    handle = p.get('handle')
                                if handle:
                                    try:
                                        async with session.get(
                                            f"{domain}/products/{handle}.js",
                                            proxy=proxy,
                                            timeout=aiohttp.ClientTimeout(total=min(4, timeout_sec)),
                                            headers=api_headers,
                                        ) as p_resp:
                                            if p_resp.status == 200:
                                                p_json = _json.loads(await p_resp.text())
                                                p_json['is_ajax'] = True
                                                result.append(p_json)
                                                break
                                    except Exception:
                                        pass
                except Exception as e:
                    last_err = f"search_error: {str(e)}"

            # ── Fast Currency Detection ──
            if result and currency == "USD":
                try:
                    async with session.get(
                        f"{domain}/cart.js", proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=min(3, timeout_sec)),
                        headers=api_headers,
                    ) as cart_resp:
                        if cart_resp.status == 200:
                            cart_data = _json.loads(await cart_resp.text())
                            if isinstance(cart_data, dict) and cart_data.get('currency'):
                                currency = cart_data.get('currency').upper()
                except Exception:
                    pass
        
        if not result:
            if "HTTP" in last_err or "error" in last_err:
                return False, f"error: failed to fetch products ({last_err})"
            return False, "<b>No Products found on site!</b>"

        # ── Two-pass selection: prefer non-shipping (digital) products ────────
        # Score key: (requires_shipping, price)
        # Digital (requires_shipping=False) score: usd_price (no shipping fee)
        # Physical (requires_shipping=True)  score: usd_price + 6.0 (calibrated shipping offset)
        # This accurately minimizes total checkout cost without picking expensive digital goods over cheap physical goods.
        candidates = []
        seen_vids = set()
        rate = get_exchange_rate(currency)

        if not isinstance(result, list):
            # Sometimes Shopify returns a dict of products instead of a list
            if isinstance(result, dict):
                result = list(result.values())
            else:
                result = []



        for product in result:
            if not isinstance(product, dict):
                continue

            ptype = (product.get('product_type') or '').lower().strip()
            handle = (product.get('handle') or '').lower()
            title = (product.get('title') or '').lower().strip()
            handle_clean = handle.replace('-', ' ').replace('_', ' ')

            # Skip gift cards, donations, tips, warranties, shipping insurance, memberships
            if any(k in ptype for k in _SKIP_KEYWORDS) or \
               any(k in handle_clean for k in _SKIP_KEYWORDS) or \
               any(k in title for k in _SKIP_KEYWORDS):
                continue
            
            variants = product.get('variants')
            if not variants or not isinstance(variants, list):
                continue

            # Product-level requires_shipping (fallback True for physical goods)
            prod_requires_shipping = product.get('requires_shipping', True)

            for variant in variants:
                # Skip unavailable variants
                if not variant.get('available', True):
                    continue
                vid_str = str(variant.get('id', ''))
                if vid_str in seen_vids:
                    continue
                seen_vids.add(vid_str)
                if variant.get('requires_selling_plan'):
                    continue

                try:
                    price_raw = variant.get('price', '0')
                    # Some Shopify endpoints return price as a dict: {'amount': '10.00', 'currencyCode': 'USD'}
                    if isinstance(price_raw, dict):
                        price_raw = price_raw.get('amount', '0')
                    if product.get('is_ajax'):
                        price = float(str(price_raw).replace(',', '')) / 100.0
                    else:
                        if isinstance(price_raw, str):
                            price = float(price_raw.replace(',', ''))
                        else:
                            price = float(price_raw)

                    if price <= 0:
                        continue

                    # Variant-level flag overrides product-level if present
                    v_requires_shipping = variant.get('requires_shipping', prod_requires_shipping)

                    usd_price = price * rate

                    # Threshold: Shopify & Stripe charge minimum is $0.50 USD for digital goods.
                    # For physical goods, shipping adds $3-$10+, so items as low as $0.10 USD
                    # safely exceed the gateway minimum and should NOT be discarded.
                    min_usd_threshold = 0.10 if v_requires_shipping else 0.50
                    if usd_price < min_usd_threshold:
                        continue

                    candidates.append({
                        'price': price,
                        'usd_price': usd_price,
                        'requires_shipping': bool(v_requires_shipping),
                        'variant_id': str(variant['id']),
                        'handle': product.get('handle', ''),
                        'title': product.get('title', ''),
                    })
                except (ValueError, TypeError, AttributeError):
                    continue

        if not candidates:
            return False, "No Valid Products"

        # Physical goods add shipping (typically $4-$8).
        # Adding a $6.00 shipping offset to physical goods balances digital vs physical:
        # e.g.:
        # - $3.00 digital product -> score = 3.0
        # - $1.00 physical product -> score = 1.0 + 6.0 = 7.0  (Digital wins!)
        # - $15.00 digital product -> score = 15.0
        # - $1.00 physical product -> score = 1.0 + 6.0 = 7.0  (Physical wins! Saves $8+ total checkout cost)
        def _score(c):
            return c['usd_price'] if not c['requires_shipping'] else c['usd_price'] + 6.0
            
        candidates.sort(key=_score)
        best = candidates[0]

        min_product = {
            'site': domain,
            'price': f"{best['price']:.2f}",
            'usd_price': best['usd_price'],
            'variant_id': best['variant_id'],
            'link': f"{domain}/products/{best['handle']}",
            'requires_shipping': best['requires_shipping'],
            'currency': currency,
            'title': best.get('title', ''),
        }

        # ── Auto-warm _VARIANT_CACHE so CC checks skip product fetch ──────────
        # TTL is price-aware: cheap products (<$10) expire in 4h, <$20 in 8h, else 12h.
        # Cheap products are more likely to sell out or get flagged after heavy checking.
        import time as _time
        usd_p = best['usd_price']
        if usd_p < 10.0:
            cache_ttl = 14400      # 4 hours
        elif usd_p < 20.0:
            cache_ttl = 28800      # 8 hours
        else:
            cache_ttl = 43200      # 12 hours
        cache_key = normalize_cache_key(domain)
        prune_variant_cache()
        _VARIANT_CACHE[cache_key] = (min_product['variant_id'], time.time(), min_product['requires_shipping'], currency, usd_p, cache_ttl)
        print(f"[CACHE] Cached: {cache_key} -> variant_id: {min_product['variant_id']}, requires_shipping: {min_product['requires_shipping']}, currency: {currency}, usd_price: {usd_p:.2f}, ttl: {cache_ttl}s")
        return min_product

    except Exception as e:
        return False, f"error: {str(e)}"

def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    
    message = str(message)
    
    patterns = [
        r'(PAYMENTS_[A-Z_]+)',
        r'(CARD_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z_]+)',
        r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?',
        r'{"code":"([^"]+)"',
        r"'code':'([^']+)'"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 50:
                match = match.strip("{}:'\" ")
                if match.upper() in ('PAYMENTS_CREDIT_CARD_GENERIC', 'PAYMENTS_CREDIT_CARD_GENERIC_DECLINE'):
                    return 'GENERIC_DECLINE'
                return match
    
    words = message.split()
    if words:
        first_word = words[0].strip("{}:'\" ")
        if "_" in first_word and first_word.isupper():
            if first_word.upper() in ('PAYMENTS_CREDIT_CARD_GENERIC', 'PAYMENTS_CREDIT_CARD_GENERIC_DECLINE'):
                return 'GENERIC_DECLINE'
            return first_word

    # If we still have raw JSON or a dictionary string, try to parse out a readable message
    clean = message
    if '{' in clean and '}' in clean:
        try:
            # First try parsing as JSON
            data = json.loads(clean.replace("'", '"'))
            if isinstance(data, dict):
                # Try common error keys
                for key in ['messageUntranslated', 'message', 'error', 'detail', 'localizedMessage']:
                    if key in data and data[key]:
                        return str(data[key])
                
                # Check nested errors
                if 'errors' in data and isinstance(data['errors'], list) and len(data['errors']) > 0:
                    err = data['errors'][0]
                    if isinstance(err, dict) and 'message' in err:
                        return str(err['message'])
        except Exception:
            pass

    # Truncate to 80 chars for clean display; strip HTML/JSON noise
    clean = re.sub(r'[{}"\[\]\\]', '', clean).strip()
    clean = re.sub(r'<[^>]+>', '', clean).strip()
    return clean[:80] if clean else "UNKNOWN_ERROR"


