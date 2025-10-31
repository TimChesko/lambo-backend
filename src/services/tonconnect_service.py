from pytonconnect import TonConnect
from pytonconnect.storage import IStorage
import json
from typing import Optional, Dict
from src.config import settings


class RedisStorage(IStorage):
    """Simple in-memory storage for TON Connect sessions"""
    
    def __init__(self):
        self.storage: Dict[str, str] = {}
    
    async def set_item(self, key: str, value: str):
        self.storage[key] = value
    
    async def get_item(self, key: str, default_value: Optional[str] = None) -> Optional[str]:
        return self.storage.get(key, default_value)
    
    async def remove_item(self, key: str):
        self.storage.pop(key, None)


class TONConnectService:
    def __init__(self):
        self.connectors: Dict[int, TonConnect] = {}
        self.storage = RedisStorage()
    
    def get_connector(self, user_id: int) -> TonConnect:
        """Получить или создать connector для пользователя"""
        if user_id not in self.connectors:
            self.connectors[user_id] = TonConnect(
                storage=self.storage,
                manifest_url=f"{settings.public_url}/tonconnect-manifest.json"
            )
        return self.connectors[user_id]
    
    async def get_connect_url(self, user_id: int) -> str:
        """Получить URL для подключения кошелька"""
        connector = self.get_connector(user_id)
        
        wallets_list = connector.get_wallets()
        
        # Генерируем универсальный URL для всех кошельков
        connect_url = await connector.connect(wallets_list)
        
        return connect_url
    
    async def get_wallet_address(self, user_id: int) -> Optional[str]:
        """Получить адрес подключенного кошелька"""
        connector = self.get_connector(user_id)
        
        if connector.connected:
            return connector.account.address
        
        return None
    
    async def disconnect(self, user_id: int):
        """Отключить кошелек"""
        connector = self.get_connector(user_id)
        await connector.disconnect()
        
        if user_id in self.connectors:
            del self.connectors[user_id]
    
    async def is_connected(self, user_id: int) -> bool:
        """Проверить, подключен ли кошелек"""
        connector = self.get_connector(user_id)
        return connector.connected


tonconnect_service = TONConnectService()

