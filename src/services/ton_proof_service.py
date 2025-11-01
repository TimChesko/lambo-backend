from datetime import datetime
from typing import Optional, Dict, Any
from nacl.utils import random
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError
import base64
import hashlib
import logging
import redis
from pytoniq_core import Cell
from src.config import settings

logger = logging.getLogger(__name__)

_redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

TON_PROOF_PREFIX = b'ton-proof-item-v2/'
TON_CONNECT_PREFIX = b'ton-connect'


def generate_payload(user_id: str, ttl: int = 300) -> str:
    payload = bytearray(random(8))

    ts = int(datetime.now().timestamp()) + ttl
    payload.extend(ts.to_bytes(8, 'big'))

    payload_hex = payload.hex()
    
    redis_key = f"ton_payload:{user_id}"
    _redis_client.setex(redis_key, ttl, payload_hex)
    
    logger.info(f"Generated payload for user {user_id}: {payload_hex[:16]}... (TTL: {ttl}s)")
    return payload_hex


def check_proof(
    address: str,
    proof: Dict[str, Any],
    user_id: str,
    domain: Optional[str] = None
) -> bool:
    try:
        if domain is None:
            domain = proof.get("domain", {}).get("value")
        
        payload = proof.get("payload")
        
        if len(payload) < 32:
            logger.warning('Payload length error')
            return False
        
        redis_key = f"ton_payload:{user_id}"
        stored_payload = _redis_client.get(redis_key)
        
        if not stored_payload:
            logger.warning(f"Payload not found in Redis for user {user_id}")
            return False
        
        if stored_payload != payload:
            logger.warning(f"Payload mismatch for user {user_id}")
            return False
        
        ts = int(payload[16:32], 16)
        if datetime.now().timestamp() > ts:
            logger.warning('Request timeout error')
            _redis_client.delete(redis_key)
            return False
        
        public_key_hex = proof.get("public_key")
        if not public_key_hex:
            logger.warning("Missing public_key in proof")
            return False
        
        public_key_bytes = bytes.fromhex(public_key_hex)
        
        workchain = int(address.split(':')[0])
        address_hash = bytes.fromhex(address.split(':')[1])
        
        timestamp = proof.get("timestamp")
        domain_value = proof.get("domain", {}).get("value", domain)
        domain_len = proof.get("domain", {}).get("lengthBytes", len(domain_value))
        signature = base64.b64decode(proof.get("signature"))
        
        wc = workchain.to_bytes(4, 'little')
        ts_bytes = timestamp.to_bytes(8, 'little')
        dl = domain_len.to_bytes(4, 'little')
        
        message = (
            TON_PROOF_PREFIX +
            wc +
            address_hash +
            dl +
            domain_value.encode('utf-8') +
            ts_bytes +
            payload.encode('utf-8')
        )
        
        msg_hash = hashlib.sha256(message).digest()
        
        full_message = (
            b'\xff\xff' +
            TON_CONNECT_PREFIX +
            msg_hash
        )
        
        sign_hash = hashlib.sha256(full_message).digest()
        
        verify_key = VerifyKey(public_key_bytes)
        verify_key.verify(sign_hash, signature)
        
        _redis_client.delete(redis_key)
        logger.info(f"✅ TON proof VERIFIED for {address}")
        
        return True
        
    except BadSignatureError:
        logger.warning('Invalid signature')
        return False
    except Exception as e:
        logger.error(f"Error in check_proof: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

