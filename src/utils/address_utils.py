from pytoniq_core import Address


def normalize_address(address: str) -> str:
    try:
        addr = Address(address)
        return addr.to_str(is_bounceable=False, is_url_safe=False, is_user_friendly=False)
    except Exception as e:
        print(f"Error normalizing address {address}: {e}")
        return address.lower()

