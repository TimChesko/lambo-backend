from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database import get_db
from src.models import User
from src.schemas import TelegramAuthRequest, AuthResponse, GeneratePayloadResponse
from src.services.auth_service import validate_telegram_init_data, create_jwt_token
from src.services.ton_proof_service import generate_payload
from src.api.middleware import get_current_user
import json
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/telegram", response_model=AuthResponse)
async def telegram_auth(
    request: TelegramAuthRequest,
    db: AsyncSession = Depends(get_db)
):
    validated_data = validate_telegram_init_data(request.initData)
    
    if not validated_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "error": {
                    "code": "INVALID_INIT_DATA",
                    "message": "Invalid Telegram initData"
                }
            }
        )
    
    try:
        if 'user' in validated_data:
            user_data = json.loads(validated_data['user'])
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "error": {
                        "code": "NO_USER_DATA",
                        "message": "No user data in initData"
                    }
                }
            )
        
        telegram_id = int(user_data.get('id'))
        username = user_data.get('username')
        
        result = await db.execute(
            select(User).where(User.telegram_id == telegram_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            user = User(
                telegram_id=telegram_id,
                username=username
            )
            db.add(user)
            await db.commit()
            await db.refresh(user)
            logger.info(f"Created new user: {telegram_id}")
        
        token_data = create_jwt_token({
            "id": str(telegram_id),
            "username": username
        })
        
        return token_data
        
    except Exception as e:
        logger.error(f"Error in telegram_auth: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Internal server error"
                }
            }
        )


@router.post("/generate_payload", response_model=GeneratePayloadResponse)
async def generate_ton_proof_payload(current_user: dict = Depends(get_current_user)):
    """
    Генерирует случайный payload для TON Connect proof.
    Клиент должен использовать этот payload при подключении кошелька.
    """
    try:
        user_id = current_user.get("user_id")
        payload = generate_payload(user_id)
        return GeneratePayloadResponse(payload=payload)
    except Exception as e:
        logger.error(f"Error generating payload: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Failed to generate payload"
                }
            }
        )

