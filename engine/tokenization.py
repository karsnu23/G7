"""
Card Tokenization Service:
Tokenizes credit cards via Shopify's PCI cardsink (https://checkout.pci.shopifyinc.com/sessions).
"""
from typing import Optional, Tuple, Dict, Any

async def tokenize_card_shopify(
    session,
    proxy: Optional[str],
    cc: str,
    mes: str,
    ano: str,
    cvv: str,
    full_name: str,
    scope_host: str,
    ident_sig: Optional[str],
    browser_profile: Dict[str, Any]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Submits card details to Shopify PCI cardsink and returns (token_id, error_message).
    """
    payload = {
        "credit_card": {
            "number": cc,
            "month": int(mes) if mes.isdigit() else mes,
            "year": int(ano) if ano.isdigit() else ano,
            "verification_value": cvv,
            "issue_number": "",
            "name": full_name
        },
        "payment_session_scope": scope_host
    }
    
    vault_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Accept-Encoding': 'br, zstd, gzip, deflate',
        'Accept-Language': browser_profile.get('accept_language', 'en-US,en;q=0.9'),
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
        
    try:
        response = await session.post('https://checkout.pci.shopifyinc.com/sessions', json=payload, headers=vault_headers, proxy=proxy)
        token_data = await response.json()
        token = token_data.get('id')
        if not token:
            return None, 'Proxy Error: Unable to get payment token'
        return token, None
    except Exception as e:
        return None, f'Proxy Error: Unable to get payment token: {str(e)}'
