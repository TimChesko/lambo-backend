from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_db
from src.schemas import RankingItem
from src.services.wallet_service import get_rankings

router = APIRouter()


@router.get("/", response_model=list[RankingItem])
async def get_wallet_rankings(limit: int = 100, db: AsyncSession = Depends(get_db)):
    rankings = await get_rankings(db, limit)
    return rankings

