from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database import get_db
from src.models import User
from src.schemas import UserCreate, UserResponse
from src.services.wallet_service import create_user

router = APIRouter()


@router.post("/", response_model=UserResponse)
async def create_new_user(user_data: UserCreate, db: AsyncSession = Depends(get_db)):
    user = await create_user(db, user_data.telegram_id, user_data.username)
    return user


@router.get("/{telegram_id}", response_model=UserResponse)
async def get_user(telegram_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.telegram_id == telegram_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user


@router.get("/", response_model=list[UserResponse])
async def list_users(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).offset(skip).limit(limit))
    users = result.scalars().all()
    return users

