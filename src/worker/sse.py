import asyncio
import logging
import httpx
import json
from datetime import datetime, timedelta
from sqlalchemy import select, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import async_session_maker, init_db
from src.models import Pool, Transaction, Wallet
from src.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SSEMonitor:
    def __init__(self, pool: Pool):
        self.pool = pool
        self.api_url = settings.ton_api_url
        self.api_key = settings.ton_api_key
        self.is_running = False
    
    async def start(self):
        self.is_running = True
        logger.info(f"🌊 Starting SSE monitor for pool {self.pool.address}")
        
        url = f"{self.api_url}/v2/sse/accounts/transactions?accounts={self.pool.address}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "text/event-stream"
        }
        
        while self.is_running:
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream("GET", url, headers=headers) as response:
                        if response.status_code != 200:
                            logger.error(f"SSE connection failed: {response.status_code}")
                            await asyncio.sleep(10)
                            continue
                        
                        logger.info(f"✅ SSE connected for pool {self.pool.address[:8]}...")
                        
                        async for line in response.aiter_lines():
                            if not line or not line.startswith("data: "):
                                continue
                            
                            try:
                                data = json.loads(line[6:])
                                await self.save_transaction(data)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse SSE data: {e}")
                            except Exception as e:
                                logger.error(f"Error processing SSE event: {e}")
            
            except Exception as e:
                logger.error(f"SSE connection error: {e}")
                await asyncio.sleep(10)
    
    async def save_transaction(self, event_data: dict):
        event_id = event_data.get("event_id")
        if not event_id:
            return
        
        async with async_session_maker() as db:
            # Получаем актуальные данные пула из БД
            pool_result = await db.execute(
                select(Pool).where(Pool.id == self.pool.id)
            )
            current_pool = pool_result.scalar_one_or_none()
            if not current_pool:
                logger.error(f"Pool {self.pool.id} not found")
                return
            
            existing = await db.execute(
                select(Transaction).where(Transaction.tx_hash == event_id)
            )
            if existing.scalar_one_or_none():
                return
            
            timestamp = event_data.get("timestamp", int(datetime.utcnow().timestamp()))
            lt = event_data.get("lt", "0")
            
            tx = Transaction(
                tx_hash=event_id,
                lt=str(lt),
                timestamp=timestamp,
                pool_id=self.pool.id,
                is_processed=False
            )
            
            db.add(tx)
            
            # Сохраняем LT текущей транзакции как checkpoint
            # Это позволит продолжить с того же места при перезапуске
            current_pool.last_processed_lt = str(lt)
            current_pool.last_sync_timestamp = int(datetime.utcnow().timestamp())
            
            await db.commit()
            
            logger.info(f"📝 Saved transaction {event_id[:8]}... (LT: {lt}) from SSE")
    
    def stop(self):
        self.is_running = False