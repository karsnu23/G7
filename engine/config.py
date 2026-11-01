"""
Configuration, Browser Profiles, Address Book, Locale, and Utility Definitions.
"""
import asyncio
import random
import re
import string
import threading
import time
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    requests = None

try:
    import phonenumbers as _pn
except Exception:
    _pn = None

_BROWSER_PROFILES = [
    # ── Chrome 146 (Windows) — latest stable Chrome as of July 2025 ──
    {
        "impersonate": "chrome146",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="146", "Google Chrome";v="146", "Not.A/Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    # ── Chrome 146 (macOS) ──
    {
        "impersonate": "chrome146",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="146", "Google Chrome";v="146", "Not.A/Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"macOS"',
    },
    # ── Chrome 136 (Windows) ──
    {
        "impersonate": "chrome136",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    # ── Chrome 136 (macOS) ──
    {
        "impersonate": "chrome136",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"macOS"',
    },
    # ── Chrome 131 (Windows) ──
    {
        "impersonate": "chrome131",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    # ── Safari 18.4 (macOS) — valid curl_cffi name: safari184 ──
    {
        "impersonate": "safari184",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
        "sec_ch_ua": "",
        "sec_ch_ua_mobile": "",
        "sec_ch_ua_platform": "",
    },
    # ── Safari 17.0 (macOS) — valid curl_cffi name: safari17_0 ──
    {
        "impersonate": "safari17_0",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
        "sec_ch_ua": "",
        "sec_ch_ua_mobile": "",
        "sec_ch_ua_platform": "",
    },
]

_ACCEPT_LANGUAGES = (
    'en-US,en;q=0.9',
    'en-GB,en;q=0.9,en-US;q=0.8',
    'en-US,en;q=0.8,en-GB;q=0.6',
    'en-CA,en-US;q=0.9,en;q=0.8',
    'en-AU,en-US;q=0.9,en;q=0.8',
)

def _pick_browser_profile():
    """Pick a random browser profile with matched TLS + headers.
    Weighted toward Chrome 146/136 (most common real-world browser in 2025)."""
    weights = [3, 2, 2, 1, 1, 1, 1]  # chrome146 win/mac weighted highest
    prof = random.choices(_BROWSER_PROFILES, weights=weights, k=1)[0].copy()
    prof['accept_language'] = random.choice(_ACCEPT_LANGUAGES)
    return prof




_CHROME_HEADER_ORDER = [
    'accept', 'accept-encoding', 'accept-language', 'content-type',
    'origin', 'priority', 'referer', 'sec-ch-ua', 'sec-ch-ua-mobile',
    'sec-ch-ua-platform', 'sec-fetch-dest', 'sec-fetch-mode',
    'sec-fetch-site', 'sec-fetch-user', 'upgrade-insecure-requests',
    'user-agent', 'dnt',
    'shopify-checkout-client', 'shopify-checkout-source',
    'x-checkout-one-session-token', 'x-checkout-web-build-id',
    'x-checkout-web-deploy-stage', 'x-checkout-web-server-handling',
    'x-checkout-web-server-rendering', 'x-checkout-web-source-id',
    'x-requested-with',
]

def _order_headers_like_chrome(headers_dict):
    """Emit headers in one fixed Chrome-like order."""
    import collections
    rank = {k.lower(): i for i, k in enumerate(_CHROME_HEADER_ORDER)}
    items = list(headers_dict.items())
    items.sort(key=lambda kv: rank.get(str(kv[0]).lower(), len(rank) + 100))
    return collections.OrderedDict(items)

# ── Feature #2: Valid Carrier & National Phone Number Synthesis ─────────


_PHONE_RULES = {
    'US': ('1', (10,)),   'CA': ('1', (10,)),   'GB': ('44', (10,)),
    'AU': ('61', (9,)),   'NZ': ('64', (9,)),   'IE': ('353', (9,)),
    'IN': ('91', (10,)),  'SG': ('65', (8,)),   'HK': ('852', (8,)),
    'MY': ('60', (9, 10)),'TH': ('66', (9,)),   'PH': ('63', (10,)),
    'ID': ('62', (10, 11)),'JP': ('81', (10,)), 'CN': ('86', (11,)),
    'AE': ('971', (9,)),  'CH': ('41', (9,)),   'DE': ('49', (10, 11)),
    'FR': ('33', (9,)),   'IT': ('39', (10,)),  'ES': ('34', (9,)),
    'NL': ('31', (9,)),   'BE': ('32', (9,)),   'SE': ('46', (9,)),
    'NO': ('47', (8,)),   'DK': ('45', (8,)),   'FI': ('358', (9,)),
    'AT': ('43', (10,)),  'PL': ('48', (9,)),   'MX': ('52', (10,)),
    'BR': ('55', (11,)),  'ZA': ('27', (9,)),
}

try:
    import phonenumbers as _pn
except Exception:
    _pn = None

def _make_phone(cc):
    region = str(cc or '').strip().upper()
    if region not in _PHONE_RULES:
        region = 'US'
    if region == 'US':
        return f"+1212{random.randint(1000000, 9999999)}"
    if _pn is not None:
        try:
            _ex = _pn.example_number(region)
            if _ex is not None:
                _base = _pn.format_number(_ex, _pn.PhoneNumberFormat.E164)
                if len(_base) > 5:
                    _head = _base[:-4]
                    for _ in range(6):
                        _cand = _head + ''.join(random.choice('0123456789') for _ in range(4))
                        try:
                            _n2 = _pn.parse(_cand, region)
                            if _pn.is_valid_number(_n2) and _pn.region_code_for_number(_n2) == region:
                                return _cand
                        except Exception:
                            pass
                return _base
        except Exception:
            pass
    rule = _PHONE_RULES.get(region) or _PHONE_RULES['US']
    dial, lengths = rule
    target_len = lengths[0]
    return f"+{dial}" + ''.join(random.choice('0123456789') for _ in range(target_len))

def _normalize_phone(phone, cc):
    region = str(cc or '').strip().upper()
    if region not in _PHONE_RULES:
        region = 'US'
    if phone and _pn is not None:
        try:
            _num = _pn.parse(str(phone), region)
            if _pn.is_valid_number(_num) and _pn.region_code_for_number(_num) == region:
                return _pn.format_number(_num, _pn.PhoneNumberFormat.E164)
        except Exception:
            pass
    return _make_phone(region)

def _national_phone(phone, cc):
    num = _normalize_phone(phone, cc)
    digits = re.sub(r'\D', '', str(num))
    dial = (_PHONE_RULES.get(str(cc or '').upper()) or ('1', (10,)))[0]
    if digits.startswith(dial) and len(digits) > len(dial):
        return digits[len(dial):]
    return digits

def generate_valid_phone(country_code: str, raw_phone: str = None) -> str:
    """Generate a clean phone number matching standard Shopify checkout validator expectations (no hyphens/spaces)."""
    cc = (country_code or 'US').strip().upper()
    raw_digits = re.sub(r'\D', '', str(raw_phone or ''))
    
    if cc in ('US', 'CA'):
        # 10 pure digits without hyphens or spaces (Shopify default regex: ^[0-9]{10}$)
        prefix = raw_digits[:3] if len(raw_digits) >= 6 else random.choice(['212', '312', '415', '646', '718', '213', '305', '917'])
        mid = raw_digits[3:6] if len(raw_digits) >= 6 else str(random.randint(200, 899))
        end = str(random.randint(1000, 9999))
        return f"{prefix}{mid}{end}"
    elif cc == 'GB':
        # UK: clean E.164 without spaces (+447700XXXXXX)
        return f"+447700{random.randint(900000, 999999)}"
    elif cc == 'AU':
        # AU: clean E.164 without spaces (+61491570XXX)
        return f"+61491570{random.randint(100, 999)}"
    else:
        # Standard: clean digits
        if len(raw_digits) >= 8:
            return raw_digits[:-4] + str(random.randint(1000, 9999))
        elif raw_digits:
            return raw_digits + str(random.randint(1000, 9999))
        else:
            dial = (_PHONE_RULES.get(cc) or ('1', (10,)))[0]
            return f"+{dial}" + str(random.randint(1000000, 9999999))

def _get_alternate_phone(current_phone: str, country_code: str, attempt: int = 1) -> str:
    """
    Returns an alternate phone format when Shopify returns DELIVERY_PHONE_NUMBER_DOES_NOT_MATCH_EXPECTED_PATTERN:
    - Attempt 1: If 10 digits -> try E.164 (+1XXXXXXXXXX). If E.164 -> try 10 digits.
    - Attempt 2: If dashed format required -> try standard dashed format (XXX-XXX-XXXX).
    - Attempt 3: Empty string "" (for stores where phone is optional).
    """
    cc = (country_code or 'US').strip().upper()
    raw = str(current_phone or '').strip()
    digits = re.sub(r'\D', '', raw)
    
    if cc in ('US', 'CA'):
        if len(digits) == 11 and digits.startswith('1'):
            ten_digits = digits[1:]
        elif len(digits) == 10:
            ten_digits = digits
        else:
            ten_digits = f"212{random.randint(2000000, 8999999)}"

        if attempt == 1:
            if raw.startswith('+'):
                return ten_digits
            else:
                return f"+1{ten_digits}"
        elif attempt == 2:
            return f"{ten_digits[:3]}-{ten_digits[3:6]}-{ten_digits[6:10]}"
        else:
            return ""
    else:
        dial = (_PHONE_RULES.get(cc) or ('1', (10,)))[0]
        if attempt == 1:
            if raw.startswith('+'):
                return digits[len(dial):] if digits.startswith(dial) else digits
            else:
                return f"+{dial}{digits}"
        elif attempt == 2:
            return digits
        else:
            return ""




C2C = {
    "USD": "US",
    "CAD": "CA", 
    "INR": "IN",
    "AED": "AE",
    "HKD": "HK",
    "GBP": "GB",
    "CHF": "CH",
    "EUR": "DE",
    "AUD": "AU",
    "NZD": "NZ",
    "SGD": "SG",
    "SEK": "SE",
    "NOK": "NO",
    "DKK": "DK",
    "MXN": "MX",
    "BRL": "BR",
    "JPY": "JP",
    "CNY": "CN",
}

class _AddressBook(dict):
    """Address book that randomly picks an address if a country has multiple addresses.
    Always returns a fresh copy so mutations don't alter the master templates."""
    def __getitem__(self, key):
        val = super().__getitem__(key)
        if isinstance(val, list):
            return random.choice(val).copy()
        elif isinstance(val, dict):
            return val.copy()
        return val

    def get(self, key, default=None):
        if key in self:
            return self[key]
        if default is not None:
            if isinstance(default, list):
                return random.choice(default).copy()
            elif isinstance(default, dict):
                return default.copy()
            return default
        return None

book = _AddressBook({
    "US": [
        {"address1": "1201 N Market St", "city": "Wilmington", "postalCode": "19801", "zoneCode": "DE", "countryCode": "US", "phone": "3025550100"},
        {"address1": "100 W 10th St", "city": "Wilmington", "postalCode": "19801", "zoneCode": "DE", "countryCode": "US", "phone": "3025550101"},
        {"address1": "1120 SW 5th Ave", "city": "Portland", "postalCode": "97204", "zoneCode": "OR", "countryCode": "US", "phone": "5035550102"},
        {"address1": "800 SW 6th Ave", "city": "Portland", "postalCode": "97204", "zoneCode": "OR", "countryCode": "US", "phone": "5035550103"},
        {"address1": "415 N 31st St", "city": "Billings", "postalCode": "59101", "zoneCode": "MT", "countryCode": "US", "phone": "4065550104"},
        {"address1": "1000 Elm St", "city": "Manchester", "postalCode": "03101", "zoneCode": "NH", "countryCode": "US", "phone": "6035550105"},
        {"address1": "3000 C St", "city": "Anchorage", "postalCode": "99503", "zoneCode": "AK", "countryCode": "US", "phone": "9075550106"},
        {"address1": "300 S State St", "city": "Dover", "postalCode": "19901", "zoneCode": "DE", "countryCode": "US", "phone": "3025550108"},
    ],
    "CA": [
        {"address1": "88 Queen St W", "city": "Toronto", "postalCode": "M5H 2M4", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
        {"address1": "1055 W Georgia St", "city": "Vancouver", "postalCode": "V6E 3P3", "zoneCode": "BC", "countryCode": "CA", "phone": "6045550100"},
        {"address1": "1000 De La Commune", "city": "Montreal", "postalCode": "H2L 1X3", "zoneCode": "QC", "countryCode": "CA", "phone": "5145550145"},
        {"address1": "10123 99 St NW", "city": "Edmonton", "postalCode": "T5J 3H1", "zoneCode": "AB", "countryCode": "CA", "phone": "7805550110"},
    ],
    "GB": [
        {"address1": "221B Baker St", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "ENG", "countryCode": "GB", "phone": "2079460123"},
        {"address1": "1 Piccadilly", "city": "London", "postalCode": "W1J 0DA", "zoneCode": "ENG", "countryCode": "GB", "phone": "2074990234"},
        {"address1": "30 St Mary Axe", "city": "London", "postalCode": "EC3A 8BF", "zoneCode": "ENG", "countryCode": "GB", "phone": "2070717234"},
        {"address1": "12 George St", "city": "Edinburgh", "postalCode": "EH2 2PF", "zoneCode": "SCT", "countryCode": "GB", "phone": "1315550102"},
        {"address1": "5 Park Place", "city": "Cardiff", "postalCode": "CF10 3DP", "zoneCode": "WLS", "countryCode": "GB", "phone": "2920550191"},
    ],
    "AU": [
        {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
        {"address1": "120 Collins St", "city": "Melbourne", "postalCode": "3000", "zoneCode": "VIC", "countryCode": "AU", "phone": "396541234"},
        {"address1": "45 Eagle St", "city": "Brisbane", "postalCode": "4000", "zoneCode": "QLD", "countryCode": "AU", "phone": "732210000"},
        {"address1": "250 St Georges Tce", "city": "Perth", "postalCode": "6000", "zoneCode": "WA", "countryCode": "AU", "phone": "893211234"},
    ],
    "IN": [
        {"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "2222625757"},
        {"address1": "15 Brigade Road", "city": "Bangalore", "postalCode": "560001", "zoneCode": "KA", "countryCode": "IN", "phone": "8022221000"},
        {"address1": "10 Connaught Place", "city": "New Delhi", "postalCode": "110001", "zoneCode": "DL", "countryCode": "IN", "phone": "1123341234"},
        {"address1": "32 Anna Salai", "city": "Chennai", "postalCode": "600002", "zoneCode": "TN", "countryCode": "IN", "phone": "4428415678"},
        {"address1": "1 Park St", "city": "Kolkata", "postalCode": "700016", "zoneCode": "WB", "countryCode": "IN", "phone": "3322001234"},
    ],
    "AE": [
        {"address1": "Burj Khalifa Tower", "city": "Dubai", "postalCode": "00000", "zoneCode": "DU", "countryCode": "AE", "phone": "97143231234"},
        {"address1": "Al Maryah Island", "city": "Abu Dhabi", "postalCode": "00000", "zoneCode": "AZ", "countryCode": "AE", "phone": "97126969696"},
        {"address1": "Hamdan St 55", "city": "Dubai", "postalCode": "00000", "zoneCode": "DU", "countryCode": "AE", "phone": "97143551234"},
    ],
    "HK": [
        {"address1": "88 Queensway", "city": "Admiralty", "postalCode": "999077", "zoneCode": "HK", "countryCode": "HK", "phone": "85228689111"},
        {"address1": "Nathan Road 100", "city": "Kowloon", "postalCode": "999077", "zoneCode": "KL", "countryCode": "HK", "phone": "85227216688"},
    ],
    "CN": [
        {"address1": "8 Zhongguancun St", "city": "Beijing", "postalCode": "100080", "zoneCode": "BJ", "countryCode": "CN", "phone": "1062512345"},
        {"address1": "100 Century Ave", "city": "Shanghai", "postalCode": "200120", "zoneCode": "SH", "countryCode": "CN", "phone": "2150504000"},
    ],
    "CH": [
        {"address1": "Gotthardstrasse 17", "city": "Zurich", "postalCode": "8002", "zoneCode": "ZH", "countryCode": "CH", "phone": "442012000"},
        {"address1": "Rue du Rhone 48", "city": "Geneva", "postalCode": "1204", "zoneCode": "GE", "countryCode": "CH", "phone": "227083300"},
    ],
    "DE": [
        {"address1": "Unter den Linden 77", "city": "Berlin", "postalCode": "10117", "zoneCode": "BE", "countryCode": "DE", "phone": "3020450"},
        {"address1": "Maximilianstr 5", "city": "Munich", "postalCode": "80539", "zoneCode": "BY", "countryCode": "DE", "phone": "8921230"},
        {"address1": "Kaiserstr 10", "city": "Frankfurt", "postalCode": "60311", "zoneCode": "HE", "countryCode": "DE", "phone": "6921230"},
    ],
    "FR": [
        {"address1": "1 Ave des Champs-Elysees", "city": "Paris", "postalCode": "75008", "zoneCode": "IDF", "countryCode": "FR", "phone": "140695000"},
        {"address1": "20 Rue de la Paix", "city": "Paris", "postalCode": "75002", "zoneCode": "IDF", "countryCode": "FR", "phone": "142333333"},
        {"address1": "3 Quai Sarrail", "city": "Lyon", "postalCode": "69006", "zoneCode": "ARA", "countryCode": "FR", "phone": "472406040"},
    ],
    "IT": {"address1": "Via Roma 1", "city": "Rome", "postalCode": "00184", "zoneCode": "RM", "countryCode": "IT", "phone": "061234567"},
    "ES": {"address1": "Gran Vía 1", "city": "Madrid", "postalCode": "28013", "zoneCode": "M", "countryCode": "ES", "phone": "911234567"},
    "NL": [
        {"address1": "Dam 1", "city": "Amsterdam", "postalCode": "1012 JS", "zoneCode": "NH", "countryCode": "NL", "phone": "2055000200"},
        {"address1": "Binnenhof 1A", "city": "The Hague", "postalCode": "2513 AA", "zoneCode": "ZH", "countryCode": "NL", "phone": "703564434"},
    ],
    "BE": {"address1": "Grand Place 1", "city": "Brussels", "postalCode": "1000", "zoneCode": "BRU", "countryCode": "BE", "phone": "021234567"},
    "SE": {"address1": "Drottninggatan 53", "city": "Stockholm", "postalCode": "111 21", "zoneCode": "AB", "countryCode": "SE", "phone": "86790000"},
    "NO": {"address1": "Karl Johans Gate 22", "city": "Oslo", "postalCode": "0026", "zoneCode": "03", "countryCode": "NO", "phone": "22341000"},
    "DK": {"address1": "Stroget 5", "city": "Copenhagen", "postalCode": "1001", "zoneCode": "84", "countryCode": "DK", "phone": "33150000"},
    "FI": {"address1": "Aleksanterinkatu 1", "city": "Helsinki", "postalCode": "00100", "zoneCode": "18", "countryCode": "FI", "phone": "091234567"},
    "IE": {"address1": "O'Connell Street", "city": "Dublin", "postalCode": "D01", "zoneCode": "D", "countryCode": "IE", "phone": "011234567"},
    "AT": {"address1": "Kärntner Straße 1", "city": "Vienna", "postalCode": "1010", "zoneCode": "9", "countryCode": "AT", "phone": "011234567"},
    "PL": {"address1": "Krakowskie Przedmieście", "city": "Warsaw", "postalCode": "00-068", "zoneCode": "MZ", "countryCode": "PL", "phone": "221234567"},
    "NZ": [
        {"address1": "1 Queen St", "city": "Auckland", "postalCode": "1010", "zoneCode": "AUK", "countryCode": "NZ", "phone": "93090000"},
        {"address1": "148 Lambton Quay", "city": "Wellington", "postalCode": "6011", "zoneCode": "WGN", "countryCode": "NZ", "phone": "44990000"},
        {"address1": "75 Cashel St", "city": "Christchurch", "postalCode": "8011", "zoneCode": "CAN", "countryCode": "NZ", "phone": "33660000"},
        {"address1": "50 Princes St", "city": "Dunedin", "postalCode": "9016", "zoneCode": "OTA", "countryCode": "NZ", "phone": "34774000"},
    ],
    "SG": [
        {"address1": "1 Raffles Place", "city": "Singapore", "postalCode": "048616", "zoneCode": "SG", "countryCode": "SG", "phone": "65350001"},
        {"address1": "8 Marina View", "city": "Singapore", "postalCode": "018960", "zoneCode": "SG", "countryCode": "SG", "phone": "65350002"},
    ],
    "MX": [
        {"address1": "Paseo de la Reforma 505", "city": "Mexico City", "postalCode": "06600", "zoneCode": "CMX", "countryCode": "MX", "phone": "5555550100"},
        {"address1": "Av Constitucion 1065", "city": "Monterrey", "postalCode": "64000", "zoneCode": "NLE", "countryCode": "MX", "phone": "8183330100"},
    ],
    "BR": {"address1": "Avenida Paulista", "city": "São Paulo", "postalCode": "01311", "zoneCode": "SP", "countryCode": "BR", "phone": "1112345678"},
    "JP": [
        {"address1": "2-1 Marunouchi", "city": "Tokyo", "postalCode": "100-0005", "zoneCode": "13", "countryCode": "JP", "phone": "332138888"},
        {"address1": "1-chome Namba", "city": "Osaka", "postalCode": "542-0076", "zoneCode": "27", "countryCode": "JP", "phone": "662414101"},
    ],
    "DEFAULT": [
        {"address1": "1201 N Market St", "city": "Wilmington", "postalCode": "19801", "zoneCode": "DE", "countryCode": "US", "phone": "3025550100"},
        {"address1": "1120 SW 5th Ave", "city": "Portland", "postalCode": "97204", "zoneCode": "OR", "countryCode": "US", "phone": "5035550102"},
        {"address1": "1000 Elm St", "city": "Manchester", "postalCode": "03101", "zoneCode": "NH", "countryCode": "US", "phone": "6035550105"},
    ],
})

# Multiple realistic US addresses using 100% TAX-FREE states (DE, OR, MT, NH, AK)
_US_ADDRESSES = dict.get(book, "US")

def pick_addr(url, cc=None, rc=None):
    cc = (cc or "").upper()
    rc = (rc or "").upper()
    try:
        dom = urlparse(url).netloc
        tcn = dom.split('.')[-1].upper()
        if tcn == 'UK':
            tcn = 'GB'
        if tcn in book and tcn != 'US':
            return book[tcn].copy()
    except Exception:
        pass

    ccn = C2C.get(cc)
    if ccn and ccn in book and ccn != 'US':
        return book[ccn].copy()
    if rc in book and rc != 'US':
        return book[rc].copy()

    # Default to a realistic tax-free US address (DE, OR, MT, NH, AK)
    addr = book["US"]
    try:
        street_parts = addr["address1"].split(" ", 1)
        if len(street_parts) > 1:
            addr["address1"] = f"{random.randint(100, 9999)} {street_parts[1]}"
    except Exception:
        pass
        
    return addr



def capture(data, first, last):
    try:
        start = data.index(first) + len(first)
        end = data.index(last, start)
        return data[start:end]
    except ValueError:
        return None

def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1:
                if end in parts[1]:
                    result = parts[1].split(end, 1)[0]
                    return result if result else None
        return None
    except Exception:
        return None

class Utils:
    @staticmethod
    def get_random_name():
        first_names = [
            "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph",
            "Thomas", "Christopher", "Charles", "Daniel", "Matthew", "Anthony", "Mark",
            "Donald", "Steven", "Andrew", "Paul", "Joshua", "Kenneth", "Kevin", "Brian",
            "George", "Timothy", "Ronald", "Jason", "Edward", "Jeffrey", "Ryan",
            "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
            "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
            "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
            "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
            "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Brenda", "Emma", "Anna",
        ]
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
            "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
            "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
            "Campbell", "Mitchell", "Carter", "Roberts", "Turner", "Phillips", "Parker",
        ]
        return (random.choice(first_names), random.choice(last_names))
    
    @staticmethod
    def generate_email(first, last):
        domains = [
            "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
            "me.com", "mac.com", "protonmail.com", "pm.me", "aol.com", "live.com",
            "msn.com", "mail.com", "proton.me", "zoho.com", "gmx.com",
        ]
        n = random.randint(1, 999)
        variants = [
            f"{first.lower()}.{last.lower()}",
            f"{first.lower()}{last.lower()}",
            f"{first.lower()}{last.lower()}{n}",
            f"{first.lower()}{random.randint(10,99)}.{last.lower()}",
            f"{first[0].lower()}{last.lower()}",
            f"{first[0].lower()}{last.lower()}{n}",
            f"{first.lower()}_{last.lower()}",
            f"{last.lower()}.{first.lower()}",
        ]
        return f"{random.choice(variants)}@{random.choice(domains)}"


# ── Feature #6: Fast Resilient BIN Lookup with Circuit Breaker ─────────
_BIN_CACHE = {}
_BIN_CACHE_LOCK = threading.Lock()
_BINLIST_RATE_LIMITED_UNTIL = 0.0
_BIN_HTTP_SESSION = None

def _get_bin_http_session():
    global _BIN_HTTP_SESSION
    if _BIN_HTTP_SESSION is None and requests is not None:
        _BIN_HTTP_SESSION = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=50, max_retries=1)
        _BIN_HTTP_SESSION.mount("https://", adapter)
        _BIN_HTTP_SESSION.mount("http://", adapter)
    return _BIN_HTTP_SESSION

def bin_info_for(cc: str) -> dict:
    """Fast resilient BIN lookup: handyapi first, 5-min circuit breaker on binlist."""
    global _BINLIST_RATE_LIMITED_UNTIL
    clean_cc = (cc or "").strip().replace(" ", "").replace("-", "")
    bin_key = clean_cc[:8]
    out = {"brand": "UNKNOWN", "type": "", "level": "", "bank": "", "country": "", "country_code": "US", "flag": ""}
    if len(bin_key) < 6:
        return out
    
    with _BIN_CACHE_LOCK:
        if bin_key in _BIN_CACHE:
            return _BIN_CACHE[bin_key].copy()
        if bin_key[:6] in _BIN_CACHE:
            return _BIN_CACHE[bin_key[:6]].copy()

    s = _get_bin_http_session()
    if s is None:
        return out

    # 1. Try handyapi first
    try:
        r2 = s.get(f"https://data.handyapi.com/bin/{bin_key}", timeout=1.5)
        if r2.status_code == 200:
            h = r2.json() or {}
            if str(h.get("Status") or "").upper() == "SUCCESS":
                out["brand"] = str(h.get("Scheme") or "UNKNOWN").upper()
                out["type"] = str(h.get("Type") or "").upper()
                out["level"] = str(h.get("CardTier") or "").upper()
                out["bank"] = str(h.get("Issuer") or "").upper()
                c2 = h.get("Country") or {}
                out["country"] = str(c2.get("Name") or "").upper()
                out["country_code"] = str(c2.get("A2") or "US").upper()
    except Exception:
        pass

    # 2. Fallback to binlist only if handyapi missed and not rate-limited
    now = time.time()
    if (now > _BINLIST_RATE_LIMITED_UNTIL) and (not out["bank"] or out["country_code"] == "US"):
        try:
            r = s.get(f"https://lookup.binlist.net/{bin_key}", headers={"Accept-Version": "3"}, timeout=1.0)
            if r.status_code == 200:
                d = r.json()
                out["brand"] = str(d.get("scheme") or d.get("brand") or out["brand"]).upper()
                out["bank"] = str((d.get("bank") or {}).get("name") or out["bank"]).upper()
                ctry = d.get("country") or {}
                out["country"] = str(ctry.get("name") or out["country"]).upper()
                out["country_code"] = str(ctry.get("alpha2") or out["country_code"]).upper()
            elif r.status_code == 429:
                _BINLIST_RATE_LIMITED_UNTIL = now + 300.0
        except Exception:
            pass

    with _BIN_CACHE_LOCK:
        if len(_BIN_CACHE) > 5000:
            for k in list(_BIN_CACHE.keys())[:1000]:
                _BIN_CACHE.pop(k, None)
        _BIN_CACHE[bin_key] = out.copy()

    return out

async def bin_info_for_async(cc: str) -> dict:
    """Non-blocking async wrapper for bin_info_for."""
    clean_cc = (cc or "").strip().replace(" ", "").replace("-", "")
    bin_key = clean_cc[:8]
    with _BIN_CACHE_LOCK:
        if bin_key in _BIN_CACHE:
            return _BIN_CACHE[bin_key].copy()
        if bin_key[:6] in _BIN_CACHE:
            return _BIN_CACHE[bin_key[:6]].copy()
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, bin_info_for, cc)



# ── Currency Exchange Rates & Pricing Helpers ───────────────────────────────
EXCHANGE_RATES: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CAD": 0.73,
    "AUD": 0.66,
    "NZD": 0.61,
    "INR": 0.012,
    "JPY": 0.0064,
    "SGD": 0.74,
    "HKD": 0.128,
    "CHF": 1.10,
    "MXN": 0.058,
    "BRL": 0.18,
    "SEK": 0.096,
    "NOK": 0.093,
    "DKK": 0.145,
    "PLN": 0.25,
    "ZAR": 0.054,
    "AED": 0.272,
    "SAR": 0.267,
    "THB": 0.029,
    "IDR": 0.000062,
    "PHP": 0.018,
    "TRY": 0.029,
    "KRW": 0.00073,
    "CNY": 0.138,
    "MYR": 0.225,
    "TWD": 0.031,
    "CZK": 0.043,
    "HUF": 0.0027,
    "ILS": 0.27,
    "CLP": 0.00105,
    "COP": 0.00024,
    "PEN": 0.27,
    "VND": 0.00004,
    "RUB": 0.011,
}

CURRENCY_SYMBOLS: Dict[str, str] = {
    "USD": "$", "CAD": "CA$", "AUD": "A$", "NZD": "NZ$", "SGD": "SG$", "HKD": "HK$",
    "EUR": "€", "GBP": "£", "JPY": "¥", "INR": "₹", "CHF": "CHF", "MXN": "MX$",
    "BRL": "R$", "SEK": "kr", "NOK": "kr", "DKK": "kr", "PLN": "zł", "ZAR": "R",
    "AED": "AED", "SAR": "SAR", "THB": "฿", "IDR": "Rp", "PHP": "₱", "TRY": "₺",
    "KRW": "₩", "CNY": "¥", "MYR": "RM", "TWD": "NT$", "CZK": "Kč", "ILS": "₪",
}

def get_exchange_rate(currency: Optional[str]) -> float:
    """Returns FX rate to convert 1 unit of `currency` into USD."""
    if not currency:
        return 1.0
    return EXCHANGE_RATES.get(str(currency).upper().strip(), 1.0)

def convert_to_usd(amount: Any, currency: Optional[str]) -> float:
    """Convert amount in `currency` to USD (rounded to 2 decimal places)."""
    try:
        amt = float(amount)
        rate = get_exchange_rate(currency)
        return round(amt * rate, 2)
    except (ValueError, TypeError):
        return 0.0
