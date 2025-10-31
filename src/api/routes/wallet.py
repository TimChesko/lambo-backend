from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database import get_db
from src.models import User, Wallet
from src.schemas import TonProofRequest, WalletVerifyResponse
from src.api.middleware import get_current_user
from src.services.ton_proof_service import check_proof
from src.services.leaderboard_service import remove_wallet
import logging
import re

router = APIRouter()
logger = logging.getLogger(__name__)


def validate_raw_address(address: str) -> bool:
    pattern = r'^(-1|0):[a-fA-F0-9]{64}$'
    return bool(re.match(pattern, address))


@router.post("/verify", response_model=WalletVerifyResponse)
async def verify_wallet(
    request: TonProofRequest,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    address = request.address
    proof = request.proof
    user_id = current_user.get("user_id")
    
    if not validate_raw_address(address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": {
                    "code": "INVALID_ADDRESS_FORMAT",
                    "message": "Address must be in RAW format: 0:hash"
                }
            }
        )
    
    if not check_proof(address, proof, user_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": {
                    "code": "PROOF_VERIFICATION_FAILED",
                    "message": "Failed to verify wallet ownership"
                }
            }
        )
    
    user_result = await db.execute(
        select(User).where(User.telegram_id == int(user_id))
    )
    user = user_result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": {
                    "code": "USER_NOT_FOUND",
                    "message": "User not found"
                }
            }
        )
    
    existing_wallet_result = await db.execute(
        select(Wallet).where(Wallet.address == address)
    )
    existing_wallet = existing_wallet_result.scalar_one_or_none()
    
    if existing_wallet and existing_wallet.user_id is not None and existing_wallet.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": {
                    "code": "WALLET_ALREADY_TAKEN",
                    "message": "This wallet is already linked to another user"
                }
            }
        )
    
    user_current_wallet_result = await db.execute(
        select(Wallet).where(Wallet.user_id == user.id)
    )
    user_current_wallet = user_current_wallet_result.scalar_one_or_none()
    
    if user_current_wallet and user_current_wallet.address != address:
        logger.info(f"🔄 Unlinking old wallet {user_current_wallet.address} from user {user.telegram_id}")
        user_current_wallet.user_id = None
        user_current_wallet.is_active = False
    
    if existing_wallet:
        existing_wallet.user_id = user.id
        existing_wallet.is_active = True
        await db.commit()
        logger.info(f"✅ Linked existing wallet {address} to user {user.telegram_id}")
    else:
        new_wallet = Wallet(
            user_id=user.id,
            address=address,
            label="TON Connect",
            balance=0.0,
            buy_volume=0.0,
            sell_volume=0.0,
            total_volume=0.0,
            is_active=True
        )
        db.add(new_wallet)
        await db.commit()
        logger.info(f"✅ Created and linked new wallet {address} to user {user.telegram_id}")
    
    return WalletVerifyResponse(
        verified=True,
        address=address
    )


@router.post("/disconnect")
async def disconnect_wallet(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    from src.models import User
    
    user_id = current_user.get("user_id")
    
    user_result = await db.execute(
        select(User).where(User.telegram_id == int(user_id))
    )
    user = user_result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": {
                    "code": "USER_NOT_FOUND",
                    "message": "User not found"
                }
            }
        )
    
    wallet_result = await db.execute(
        select(Wallet).where(Wallet.user_id == user.id)
    )
    wallet = wallet_result.scalar_one_or_none()
    
    if not wallet:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": {
                    "code": "NO_WALLET_LINKED",
                    "message": "No wallet linked to this account"
                }
            }
        )
    
    wallet_address = wallet.address
    wallet.user_id = None
    wallet.is_active = False
    await db.commit()
    
    remove_wallet(wallet_address)
    
    logger.info(f"🔓 Disconnected wallet {wallet_address} from user {user.telegram_id}")
    
    return {"success": True, "message": "Wallet disconnected"}

