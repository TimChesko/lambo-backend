import hashlib
import hmac
import jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from urllib.parse import parse_qsl
from src.config import settings
import logging

logger = logging.getLogger(__name__)


def validate_telegram_init_data(init_data: str) -> Optional[Dict[str, Any]]:
    try:
        parsed_data = dict(parse_qsl(init_data))
        
        if 'hash' not in parsed_data:
            logger.warning("No hash in initData")
            return None
        
        received_hash = parsed_data.pop('hash')
        
        data_check_string = '\n'.join([f"{k}={v}" for k, v in sorted(parsed_data.items())])
        
        secret_key = hmac.new(
            key=b"WebAppData",
            msg=settings.bot_token.encode(),
            digestmod=hashlib.sha256
        ).digest()
        
        calculated_hash = hmac.new(
            key=secret_key,
            msg=data_check_string.encode(),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        if calculated_hash != received_hash:
            logger.warning("Hash mismatch")
            return None
        
        if 'auth_date' in parsed_data:
            auth_date = int(parsed_data['auth_date'])
            current_time = int(datetime.utcnow().timestamp())
            if current_time - auth_date > 86400:
                logger.warning("initData too old")
                return None
        
        return parsed_data
        
    except Exception as e:
        logger.error(f"Error validating initData: {e}")
        return None


def create_jwt_token(user_data: Dict[str, Any]) -> Dict[str, Any]:
    expiration = datetime.utcnow() + timedelta(hours=settings.jwt_expiration_hours)
    
    payload = {
        "user_id": user_data.get("id"),
        "username": user_data.get("username"),
        "exp": expiration,
        "iat": datetime.utcnow()
    }
    
    token = jwt.encode(
        payload,
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm
    )
    
    return {
        "token": token,
        "expiresAt": expiration.isoformat() + "Z"
    }


def verify_jwt_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm]
        )
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        return None

