import httpx
from typing import Optional, Dict, Any
from src.config import settings


class TONService:
    def __init__(self):
        self.api_url = settings.ton_api_url
        self.api_key = settings.ton_api_key if settings.ton_api_key and len(settings.ton_api_key) > 10 else None
        
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        self.client = httpx.AsyncClient(timeout=30.0, headers=headers)

    async def get_balance(self, address: str) -> Optional[float]:
        try:
            url = f"{self.api_url}/v2/accounts/{address}"
            
            response = await self.client.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            balance_nano = int(data.get("balance", 0))
            balance_ton = balance_nano / 1_000_000_000
            return balance_ton
        except Exception as e:
            print(f"Error getting balance for {address}: {e}")
            return None

    async def get_jetton_history(
        self, 
        address: str, 
        jetton_master: str = None,
        limit: int = 100,
        before_lt: Optional[str] = None,
        start_date: Optional[int] = None
    ) -> Dict[str, Any]:
        try:
            url = f"{self.api_url}/v2/accounts/{address}/jettons/history"
            params = {"limit": limit}
            
            if before_lt:
                params["before_lt"] = before_lt
            
            if jetton_master:
                params["jetton_id"] = jetton_master
            
            if start_date:
                params["start_date"] = start_date
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return {
                "events": data.get("operations", []),
                "next_from": data.get("next_from")
            }
        except Exception as e:
            print(f"Error getting jetton history for {address}: {e}")
            return {"events": [], "next_from": None}
    
    async def get_transactions(self, address: str, limit: int = 10) -> list[Dict[str, Any]]:
        try:
            url = f"{self.api_url}/v2/accounts/{address}/events"
            params = {"limit": limit}
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("events", [])
        except Exception as e:
            print(f"Error getting transactions for {address}: {e}")
            return []

    async def validate_address(self, address: str) -> bool:
        if not address or len(address) < 48:
            return False
        
        if not (address.startswith('EQ') or address.startswith('UQ') or 
                address.startswith('0:') or address.startswith('-1:')):
            return False
        
        try:
            balance = await self.get_balance(address)
            return balance is not None
        except Exception as e:
            print(f"Error validating address {address}: {e}")
            return False

    async def close(self):
        await self.client.aclose()


ton_service = TONService()

