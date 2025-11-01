from sqlalchemy import Column, Integer, BigInteger, String, Float, DateTime, Boolean, ForeignKey, Index, func
from sqlalchemy.orm import relationship
from src.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    is_active = Column(Boolean, default=True)

    wallets = relationship("Wallet", back_populates="user", cascade="all, delete-orphan")

class Wallet(Base):
    __tablename__ = "wallets"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    address = Column(String, nullable=False, unique=True, index=True)
    label = Column(String, nullable=True)

    count_buys = Column(Integer, default=0)
    count_sells = Column(Integer, default=0)
    
    # Volume fields by currency
    buy_volume_lambo = Column(Float, default=0.0)
    sell_volume_lambo = Column(Float, default=0.0)
    total_volume_lambo = Column(Float, default=0.0)
    
    buy_volume_ton = Column(Float, default=0.0)
    sell_volume_ton = Column(Float, default=0.0)
    total_volume_ton = Column(Float, default=0.0)
    
    buy_volume_usd = Column(Float, default=0.0)
    sell_volume_usd = Column(Float, default=0.0)
    total_volume_usd = Column(Float, default=0.0)
    
    # Sync status
    sync_status = Column(String, default='pending')
    initial_sync_completed = Column(Boolean, default=False)
    
    # System fields
    created_at = Column(DateTime, server_default=func.now())
    is_active = Column(Boolean, default=True)

    user = relationship("User", back_populates="wallets")

    __table_args__ = (
        Index("idx_wallet_address_user", "address", "user_id"),
        Index("idx_wallet_total_volume_usd", "total_volume_usd"),
        Index("idx_wallet_sync_status", "sync_status"),
    )


class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    tx_hash = Column(String, unique=True, nullable=False, index=True)
    event_id = Column(String, nullable=True, index=True)  # Для дедупликации событий
    lt = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    
    # Participants
    user_address = Column(String, nullable=True, index=True)
    pool_id = Column(Integer, ForeignKey("pools.id"), nullable=False, index=True)
    
    # Swap data (null if not processed yet)
    operation_type = Column(String, nullable=True)  # 'buy' or 'sell'
    ton_amount = Column(Float, nullable=True)
    lambo_amount = Column(Float, nullable=True)
    ton_usd_price = Column(Float, nullable=True)  # TON price at the moment
    
    # Status
    is_processed = Column(Boolean, default=False, index=True)
    
    __table_args__ = (
        Index("idx_tx_processed", "is_processed", "timestamp"),
    )


class Pool(Base):
    __tablename__ = "pools"
    
    id = Column(Integer, primary_key=True, index=True)
    address = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=True)
    jetton_master = Column(String, nullable=True)
    
    # Sync tracking - where did we stop processing
    last_processed_lt = Column(String, nullable=True) 
    last_sync_timestamp = Column(Integer, nullable=True) 
    
    # Status
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

