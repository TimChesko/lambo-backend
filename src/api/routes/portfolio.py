from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database import get_db
from src.models import Wallet
from src.schemas import PortfolioResponse
from src.api.middleware import get_current_user
from src.services.leaderboard_service import get_rank, get_total_wallets
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("", response_model=PortfolioResponse)
async def get_portfolio(
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
        select(Wallet).where(
            Wallet.user_id == user.id,
            Wallet.is_active == True
        )
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
    
    rank = get_rank(wallet.address) or 1
    total_wallets = get_total_wallets()
    
    top_percentage = int((rank / total_wallets * 100)) if total_wallets > 0 else 100
    
    wallet_status = "ready" if wallet.last_checked else "pending"
    
    return {
        "topPercentage": top_percentage,
        "rank": rank,
        "stats": {
            "buys": {
                "count": wallet.buy_count or 0,
                "amount": wallet.buy_volume
            },
            "sells": {
                "count": wallet.sell_count or 0,
                "amount": wallet.sell_volume
            }
        },
        "status": wallet_status
    }

