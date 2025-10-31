import base64
import hashlib


def address_to_raw(address: str) -> str:
    try:
        if ':' in address:
            return address
        
        if address.startswith('EQ') or address.startswith('UQ'):
            address_bytes = base64.urlsafe_b64decode(address + '==')
            
            workchain = int.from_bytes(address_bytes[0:1], byteorder='big', signed=True)
            hash_part = address_bytes[1:33].hex()
            
            return f"{workchain}:{hash_part}"
        
        return address
    except Exception as e:
        print(f"Error converting address to raw: {e}")
        return address


def address_to_friendly(raw_address: str, bounceable: bool = True, test_only: bool = False) -> str:
    try:
        if not ':' in raw_address:
            return raw_address
        
        parts = raw_address.split(':')
        if len(parts) != 2:
            return raw_address
        
        workchain = int(parts[0])
        hash_hex = parts[1]
        
        tag = 0x11 if bounceable else 0x51
        if test_only:
            tag |= 0x80
        
        workchain_byte = workchain.to_bytes(1, byteorder='big', signed=True)
        hash_bytes = bytes.fromhex(hash_hex)
        
        addr = bytes([tag]) + workchain_byte + hash_bytes
        
        crc = crc16(addr)
        addr_with_crc = addr + crc.to_bytes(2, byteorder='big')
        
        encoded = base64.urlsafe_b64encode(addr_with_crc).decode('utf-8')
        return encoded.rstrip('=')
    
    except Exception as e:
        print(f"Error converting address to friendly: {e}")
        return raw_address


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc = crc << 1
            crc &= 0xFFFF
    return crc


def normalize_address(address: str) -> str:
    if not address:
        return ""
    
    address = address.strip()
    
    if address.startswith('EQ') or address.startswith('UQ'):
        return address
    
    if ':' in address:
        return address_to_friendly(address, bounceable=False)
    
    return address


def is_valid_ton_address(address: str) -> bool:
    if not address or len(address) < 48:
        return False
    
    if address.startswith('EQ') or address.startswith('UQ'):
        return True
    
    if ':' in address:
        parts = address.split(':')
        if len(parts) != 2:
            return False
        try:
            int(parts[0])
            int(parts[1], 16)
            return len(parts[1]) == 64
        except:
            return False
    
    return False

