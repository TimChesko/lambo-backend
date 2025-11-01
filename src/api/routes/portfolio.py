from fastapi import APIRouter, HTTPException, status, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from src.database import get_db
from src.models import Wallet, Transaction
from src.schemas import PortfolioResponse
from src.api.middleware import get_current_user
from src.services.leaderboard_service import get_rank, get_total_wallets
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


def get_volume_by_currency(wallet: Wallet, currency: str):
    """
    Получает volume данные из wallet в зависимости от выбранной валюты.
    
    Args:
        wallet: объект Wallet из БД
        currency: одна из "usd", "ton", "lambo"
    
    Returns:
        dict с buy_volume и sell_volume для выбранной валюты
    """
    currency = currency.lower()
    
    if currency == "usd":
        return {
            "buy": wallet.buy_volume_usd,
            "sell": wallet.sell_volume_usd,
            "total": wallet.total_volume_usd
        }
    elif currency == "ton":
        return {
            "buy": wallet.buy_volume_ton,
            "sell": wallet.sell_volume_ton,
            "total": wallet.total_volume_ton
        }
    elif currency == "lambo":
        return {
            "buy": wallet.buy_volume_lambo,
            "sell": wallet.sell_volume_lambo,
            "total": wallet.total_volume_lambo
        }
    else:
        # По умолчанию возвращаем USD если передан неправильный параметр
        logger.warning(f"Unknown currency: {currency}, falling back to USD")
        return {
            "buy": wallet.buy_volume_usd,
            "sell": wallet.sell_volume_usd,
            "total": wallet.total_volume_usd
        }


@router.get("", response_model=PortfolioResponse)
async def get_portfolio(
    currency: str = Query("usd", description="Валюта: usd, ton или lambo"),
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
    
    wallet_status = "synced" if wallet.sync_status == "synced" else "syncing" if wallet.sync_status == "syncing" else "pending"
    
    # Получаем volume данные в зависимости от выбранной валюты
    volumes = get_volume_by_currency(wallet, currency)
    
    # Подсчитываем количество покупок и продаж из таблицы transactions
    buy_count_result = await db.execute(
        select(func.count(Transaction.id)).where(
            Transaction.user_address == wallet.address,
            Transaction.operation_type == "buy",
            Transaction.is_processed == True
        )
    )
    buy_count = buy_count_result.scalar() or 0
    
    sell_count_result = await db.execute(
        select(func.count(Transaction.id)).where(
            Transaction.user_address == wallet.address,
            Transaction.operation_type == "sell",
            Transaction.is_processed == True
        )
    )
    sell_count = sell_count_result.scalar() or 0
    
    return {
        "topPercentage": top_percentage,
        "rank": rank,
        "stats": {
            "buys": {
                "count": int(buy_count),
                "amount": volumes["buy"]
            },
            "sells": {
                "count": int(sell_count),
                "amount": volumes["sell"]
            }
        },
        "status": wallet_status
    }

