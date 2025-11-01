import asyncio
import logging
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.models import Pool, Transaction, Wallet
from src.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionProcessor:
    def __init__(self):
        self.api_url = settings.ton_api_url
        self.api_key = settings.ton_api_key
        self.delay = 1.0 / settings.requests_per_second
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={'Authorization': f'Bearer {self.api_key}'}
        )
    
    async def close(self):
        await self.client.aclose()
    
    async def get_event_details(self, tx_hash: str) -> dict:
        url = f"{self.api_url}/v2/events/{tx_hash}"
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()
    
    async def get_ton_price_at_time(self, timestamp: int) -> float:
        start_date = timestamp - 300
        end_date = timestamp + 300
        
        url = f"{self.api_url}/v2/rates/chart"
        params = {
            "token": "ton",
            "currency": "usd",
            "start_date": start_date,
            "end_date": end_date,
            "points_count": 10
        }
        
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        points = data.get("points", [])
        if not points:
            logger.warning(f"No price points found for timestamp {timestamp}")
            return 0.0
        
        closest_point = min(points, key=lambda p: abs(p[0] - timestamp))
        return closest_point[1]
    
    def find_swap_action(self, event_data: dict) -> dict:
        for action in event_data.get("actions", []):
            if action.get("type") == "JettonSwap":
                return action.get("JettonSwap", {})
        return {}
    
    async def is_lambo_transaction(self, tx_hash: str, jetton_master: str) -> bool:
        """Проверяет является ли транзакция LAMBO swap'ом"""
        try:
            event_data = await self.get_event_details(tx_hash)
            await asyncio.sleep(self.delay * 1.5)  # Усилили delay для безопасности от 429
            
            swap_action = self.find_swap_action(event_data)
            
            if not swap_action:
                return False
            
            jetton_in = swap_action.get("jetton_master_in", {})
            jetton_out = swap_action.get("jetton_master_out", {})
            
            jetton_in_address = jetton_in.get("address") if isinstance(jetton_in, dict) else None
            jetton_out_address = jetton_out.get("address") if isinstance(jetton_out, dict) else None
            
            return jetton_master in [jetton_in_address, jetton_out_address]
        except Exception as e:
            logger.error(f"Error checking tx {tx_hash}: {e}")
            await asyncio.sleep(self.delay * 2)  # Еще больше delay при ошибке
            return False
    
    async def process_transaction(self, tx: Transaction, db: AsyncSession) -> bool:
        try:
            pool_result = await db.execute(select(Pool).where(Pool.id == tx.pool_id))
            pool = pool_result.scalar_one_or_none()
            
            if not pool or not pool.jetton_master:
                logger.warning(f"Pool not found or jetton_master missing for tx {tx.tx_hash}, deleting")
                await db.delete(tx)
                await db.commit()
                return False
            
            event_data = await self.get_event_details(tx.tx_hash)
            
            swap_action = self.find_swap_action(event_data)
            if not swap_action:
                logger.debug(f"Delete tx {tx.tx_hash[:8]}... - no JettonSwap action")
                await db.delete(tx)
                await db.commit()
                return False
            
            jetton_in = swap_action.get("jetton_master_in", {})
            jetton_out = swap_action.get("jetton_master_out", {})
            
            jetton_in_address = jetton_in.get("address") if isinstance(jetton_in, dict) else None
            jetton_out_address = jetton_out.get("address") if isinstance(jetton_out, dict) else None
            
            if pool.jetton_master not in [jetton_in_address, jetton_out_address]:
                logger.debug(f"Delete tx {tx.tx_hash[:8]}... - not LAMBO jetton")
                await db.delete(tx)
                await db.commit()
                return False
            
            event_timestamp = event_data.get("timestamp")
            if not event_timestamp:
                logger.warning(f"No timestamp for tx {tx.tx_hash}, deleting")
                await db.delete(tx)
                await db.commit()
                return False
            
            ton_usd_price = await self.get_ton_price_at_time(event_timestamp)
            
            ton_in_nano = swap_action.get("ton_in")
            lambo_out_nano_str = swap_action.get("amount_out")
            
            if ton_in_nano is None or lambo_out_nano_str is None:
                logger.warning(f"Missing swap amounts for tx {tx.tx_hash}, deleting")
                await db.delete(tx)
                await db.commit()
                return False
            
            ton_amount = float(ton_in_nano) / 1_000_000_000
            lambo_amount = float(lambo_out_nano_str) / 1_000_000_000
            
            user_wallet = swap_action.get("user_wallet", {})
            user_address = user_wallet.get("address") if isinstance(user_wallet, dict) else None
            
            if not user_address:
                logger.warning(f"No user address for tx {tx.tx_hash}, deleting")
                await db.delete(tx)
                await db.commit()
                return False
            
            tx.user_address = user_address
            tx.operation_type = "buy"
            tx.ton_amount = ton_amount
            tx.lambo_amount = lambo_amount
            tx.ton_usd_price = ton_usd_price
            tx.timestamp = event_timestamp
            tx.is_processed = True
            
            await db.commit()
            
            await self.update_wallet_volumes(user_address, tx, db)
            
            logger.info(
                f"✅ Processed {tx.tx_hash[:8]}... "
                f"User: {user_address[:8]}... "
                f"TON: {ton_amount:.4f} "
                f"LAMBO: {lambo_amount:.2f} "
                f"USD: ${ton_amount * ton_usd_price:.2f}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing transaction {tx.tx_hash}: {e}")
            return False
    
    async def update_wallet_volumes(self, address: str, tx: Transaction, db: AsyncSession):
        result = await db.execute(
            select(Wallet).where(Wallet.address == address)
        )
        wallet = result.scalar_one_or_none()
        
        if not wallet:
            logger.warning(f"Wallet {address} not found, skipping volume update")
            return
        
        usd_amount = tx.ton_amount * tx.ton_usd_price
        
        if tx.operation_type == "buy":
            wallet.buy_volume_lambo += tx.lambo_amount
            wallet.buy_volume_ton += tx.ton_amount
            wallet.buy_volume_usd += usd_amount
        else:
            wallet.sell_volume_lambo += tx.lambo_amount
            wallet.sell_volume_ton += tx.ton_amount
            wallet.sell_volume_usd += usd_amount
        
        wallet.total_volume_lambo = wallet.buy_volume_lambo + wallet.sell_volume_lambo
        wallet.total_volume_ton = wallet.buy_volume_ton + wallet.sell_volume_ton
        wallet.total_volume_usd = wallet.buy_volume_usd + wallet.sell_volume_usd
        
        await db.commit()
        
        from src.services.leaderboard_service import update_leaderboard
        update_leaderboard(wallet.address, wallet.total_volume_usd)