import asyncio
import logging
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.models import Pool, Transaction, Wallet
from src.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Отключаем логи httpx
logging.getLogger("httpx").setLevel(logging.WARNING)

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
        """
        Извлекает информацию о свопе из Events API.
        Ищет JettonSwap в actions[].
        """
        try:
            actions = event_data.get("actions", [])
            
            for action in actions:
                if action.get("type") != "JettonSwap":
                    continue
                
                swap = action.get("JettonSwap", {})
                if not swap:
                    continue
                
                # Извлекаем данные
                ton_in = swap.get("ton_in", 0)
                ton_out = swap.get("ton_out", 0)
                amount_in = swap.get("amount_in", "0")
                amount_out = swap.get("amount_out", "0")
                
                user_wallet = swap.get("user_wallet", {})
                jetton_master_in = swap.get("jetton_master_in", {})
                jetton_master_out = swap.get("jetton_master_out", {})
                
                return {
                    "ton_in": ton_in,
                    "ton_out": ton_out,
                    "amount_in": amount_in,
                    "amount_out": amount_out,
                    "user_wallet": user_wallet,
                    "jetton_master_in": jetton_master_in,
                    "jetton_master_out": jetton_master_out,
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error in find_swap_action: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
                await db.delete(tx)
                await db.commit()
                return False
            
            event_data = await self.get_event_details(tx.tx_hash)
            
            swap_action = self.find_swap_action(event_data)
            if not swap_action:
                await db.delete(tx)
                await db.commit()
                return False
            
            jetton_in = swap_action.get("jetton_master_in", {})
            jetton_out = swap_action.get("jetton_master_out", {})
            
            jetton_in_address = jetton_in.get("address") if isinstance(jetton_in, dict) else None
            jetton_out_address = jetton_out.get("address") if isinstance(jetton_out, dict) else None
            
            if pool.jetton_master not in [jetton_in_address, jetton_out_address]:
                await db.delete(tx)
                await db.commit()
                return False
            
            logger.info(f"💰 LAMBO swap found: {tx.tx_hash[:8]}...")
            
            # Это ЛАМБО транзакция!
            event_id = event_data.get("event_id")
            
            # Проверяем дубликат по event_id
            if event_id:
                existing_event = await db.execute(
                    select(Transaction).where(
                        Transaction.event_id == event_id,
                        Transaction.is_processed == True
                    )
                )
                if existing_event.scalar_one_or_none():
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
            
            # Определяем тип операции: BUY или SELL
            ton_in_nano = swap_action.get("ton_in", 0)
            ton_out_nano = swap_action.get("ton_out", 0)
            amount_in_str = swap_action.get("amount_in", "")
            amount_out_str = swap_action.get("amount_out", "")
            
            # BUY: TON входит, LAMBO выходит (ton_in > 0, amount_out > 0)
            # SELL: LAMBO входит, TON выходит (amount_in > 0, ton_out > 0)
            
            if ton_in_nano and ton_in_nano > 0 and amount_out_str and amount_out_str != "":
                # BUY транзакция
                operation_type = "buy"
                ton_amount = float(ton_in_nano) / 1_000_000_000
                lambo_amount = float(amount_out_str) / 1_000_000_000
            elif ton_out_nano and ton_out_nano > 0 and amount_in_str and amount_in_str != "":
                # SELL транзакция
                operation_type = "sell"
                ton_amount = float(ton_out_nano) / 1_000_000_000
                lambo_amount = float(amount_in_str) / 1_000_000_000
            else:
                await db.delete(tx)
                await db.commit()
                return False
            
            user_wallet = swap_action.get("user_wallet", {})
            user_address = user_wallet.get("address") if isinstance(user_wallet, dict) else None
            
            if not user_address:
                logger.warning(f"No user address for tx {tx.tx_hash}, deleting")
                await db.delete(tx)
                await db.commit()
                return False
            
            tx.user_address = user_address
            tx.event_id = event_id
            tx.operation_type = operation_type
            tx.ton_amount = ton_amount
            tx.lambo_amount = lambo_amount
            tx.ton_usd_price = ton_usd_price
            tx.timestamp = event_timestamp
            
            # Проверяем дубликат по комбинации (user + amount + timestamp)
            existing_similar = await db.execute(
                select(Transaction).where(
                    Transaction.user_address == user_address,
                    Transaction.ton_amount == ton_amount,
                    Transaction.lambo_amount == lambo_amount,
                    Transaction.timestamp == event_timestamp,
                    Transaction.is_processed == True
                )
            )
            if existing_similar.scalar_one_or_none():
                await db.delete(tx)
                await db.commit()
                return False
            
            tx.is_processed = True
            
            await db.commit()
            
            await self.update_wallet_volumes(user_address, tx, db)
            
            logger.info(
                f"✅ Processed {operation_type.upper()} {tx.tx_hash[:8]}... "
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