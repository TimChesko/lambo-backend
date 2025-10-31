from datetime import datetime
from typing import List, Optional
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from src.models import User, Wallet
from src.schemas import WalletCreate, WalletResponse, RankingItem
from src.services.ton_service import ton_service
from src.utils.ton_address import is_valid_ton_address


async def create_user(db: AsyncSession, telegram_id: int, username: Optional[str] = None) -> User:
    result = await db.execute(select(User).where(User.telegram_id == telegram_id))
    user = result.scalar_one_or_none()
    
    if user:
        return user
    
    user = User(telegram_id=telegram_id, username=username)
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def add_wallet(db: AsyncSession, user_id: int, address: str, label: Optional[str] = None) -> Optional[Wallet]:
    if not is_valid_ton_address(address):
        return None
    
    address = address.strip()
    
    result = await db.execute(
        select(Wallet).where(Wallet.user_id == user_id, Wallet.address == address)
    )
    existing_wallet = result.scalar_one_or_none()
    
    if existing_wallet:
        return existing_wallet
    
    balance = await ton_service.get_balance(address)
    
    if balance is None:
        return None
    
    wallet = Wallet(
        user_id=user_id,
        address=address,
        label=label,
        balance=balance,
        last_checked=datetime.utcnow()
    )
    db.add(wallet)
    await db.commit()
    await db.refresh(wallet)
    return wallet


async def get_user_wallets(db: AsyncSession, user_id: int) -> List[Wallet]:
    result = await db.execute(
        select(Wallet).where(Wallet.user_id == user_id, Wallet.is_active == True)
    )
    return result.scalars().all()


async def update_wallet_balance(db: AsyncSession, wallet_id: int) -> Optional[Wallet]:
    result = await db.execute(select(Wallet).where(Wallet.id == wallet_id))
    wallet = result.scalar_one_or_none()
    
    if not wallet:
        return None
    
    balance = await ton_service.get_balance(wallet.address)
    if balance is not None:
        wallet.balance = balance
        wallet.last_checked = datetime.utcnow()
        await db.commit()
        await db.refresh(wallet)
    
    return wallet


async def get_rankings(db: AsyncSession, limit: int = 100) -> List[RankingItem]:
    result = await db.execute(
        select(Wallet, User)
        .join(User, Wallet.user_id == User.id)
        .where(Wallet.is_active == True)
        .order_by(desc(Wallet.total_volume))
        .limit(limit)
    )
    
    rankings = []
    for rank, (wallet, user) in enumerate(result.all(), start=1):
        rankings.append(
            RankingItem(
                rank=rank,
                address=wallet.address,
                label=wallet.label,
                balance=wallet.balance,
                buy_volume=wallet.buy_volume or 0.0,
                sell_volume=wallet.sell_volume or 0.0,
                total_volume=wallet.total_volume or 0.0,
                username=user.username
            )
        )
    
    return rankings


async def remove_wallet(db: AsyncSession, wallet_id: int, user_id: int) -> bool:
    result = await db.execute(
        select(Wallet).where(Wallet.id == wallet_id, Wallet.user_id == user_id)
    )
    wallet = result.scalar_one_or_none()
    
    if not wallet:
        return False
    
    wallet.is_active = False
    await db.commit()
    return True

