from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class UserBase(BaseModel):
    telegram_id: int
    username: Optional[str] = None


class UserCreate(UserBase):
    pass


class UserResponse(UserBase):
    id: int
    created_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


class WalletBase(BaseModel):
    address: str
    label: Optional[str] = None


class WalletCreate(WalletBase):
    user_id: int


class WalletResponse(WalletBase):
    id: int
    user_id: int
    balance: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    total_volume: float = 0.0
    last_transaction_lt: Optional[str] = None
    last_checked: Optional[datetime] = None
    created_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


class RankingItem(BaseModel):
    rank: int
    address: str
    label: Optional[str] = None
    balance: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    total_volume: float = 0.0
    username: Optional[str] = None


class BalanceUpdate(BaseModel):
    wallet_id: int
    balance: float
    timestamp: datetime


class TelegramAuthRequest(BaseModel):
    initData: str


class AuthResponse(BaseModel):
    token: str
    expiresAt: str


class GeneratePayloadResponse(BaseModel):
    payload: str


class PortfolioStats(BaseModel):
    buys: dict
    sells: dict


class PortfolioResponse(BaseModel):
    topPercentage: int
    rank: int
    stats: PortfolioStats
    status: str


class LeaderboardItem(BaseModel):
    rank: int
    address: str
    volume: float


class LeaderboardResponse(BaseModel):
    items: list[LeaderboardItem]
    userRank: Optional[int] = None
    userVolume: Optional[float] = None


class ErrorResponse(BaseModel):
    error: dict


class TonProofDomain(BaseModel):
    lengthBytes: int
    value: str


class TonProofRequest(BaseModel):
    address: str
    proof: dict


class WalletVerifyResponse(BaseModel):
    verified: bool
    address: str

