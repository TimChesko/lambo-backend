from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.models import User


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