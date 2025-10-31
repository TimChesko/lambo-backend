from fastapi import APIRouter, HTTPException, status, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database import get_db
from src.models import User, Wallet
from src.schemas import LeaderboardResponse, LeaderboardItem
from src.api.middleware import get_current_user
from src.services.leaderboard_service import get_top_wallets
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=LeaderboardResponse)
async def get_leaderboard(
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    from src.services.leaderboard_service import get_rank
    
    top_wallets = get_top_wallets(limit, offset)
    
    items = []
    for entry in top_wallets:
        items.append(LeaderboardItem(
            rank=entry["rank"],
            address=entry["address"],
            volume=entry["total_volume"]
        ))
    
    user_id = current_user.get("user_id")
    user_result = await db.execute(
        select(User).where(User.telegram_id == int(user_id))
    )
    user = user_result.scalar_one_or_none()
    
    user_rank = None
    user_volume = None
    
    if user:
        user_wallet_result = await db.execute(
            select(Wallet).where(
                Wallet.user_id == user.id,
                Wallet.is_active == True
            )
        )
        user_wallet = user_wallet_result.scalar_one_or_none()
        
        if user_wallet:
            user_rank = get_rank(user_wallet.address)
            user_volume = user_wallet.total_volume
    
    return LeaderboardResponse(
        items=items,
        userRank=user_rank,
        userVolume=user_volume
    )

