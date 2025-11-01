import asyncio
import httpx
import os
from src.database import async_session_maker, init_db
from src.models import Pool
from sqlalchemy import select
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def register_webhook():
    """
    Регистрирует webhook в TON API для получения уведомлений о транзакциях
    """
    await init_db()
    
    api_key = os.getenv("TON_API_KEY")
    webhook_url = "https://api.durak.bot/webhooks/tonapi"
    
    async with async_session_maker() as db:
        # Получаем активные пулы
        result = await db.execute(
            select(Pool).where(Pool.is_active == True)
        )
        pools = result.scalars().all()
        
        if not pools:
            logger.error("No active pools found")
            return
        
        pool_addresses = [pool.address for pool in pools]
        logger.info(f"Registering webhook for {len(pool_addresses)} pools")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Создаем webhook
            create_url = "https://tonapi.io/v2/webhooks"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            create_payload = {
                "url": webhook_url
            }
            
            try:
                response = await client.post(create_url, json=create_payload, headers=headers)
                response.raise_for_status()
                webhook_data = response.json()
                webhook_id = webhook_data.get("id")
                
                logger.info(f"✅ Webhook created: {webhook_id}")
                logger.info(f"   URL: {webhook_url}")
                
                # Подписываемся на транзакции пулов
                subscribe_url = f"https://tonapi.io/v2/webhooks/{webhook_id}/account-tx/subscribe"
                
                accounts = [{"account_id": addr} for addr in pool_addresses]
                subscribe_payload = {"accounts": accounts}
                
                response = await client.post(subscribe_url, json=subscribe_payload, headers=headers)
                response.raise_for_status()
                
                logger.info(f"✅ Subscribed to {len(accounts)} pool accounts")
                for pool in pools:
                    logger.info(f"   - {pool.name}: {pool.address}")
                
                logger.info(f"\n🎉 Webhook успешно зарегистрирован!")
                logger.info(f"   Webhook ID: {webhook_id}")
                logger.info(f"   Endpoint: {webhook_url}")
                
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP Error: {e.response.status_code}")
                logger.error(f"Response: {e.response.text}")
            except Exception as e:
                logger.error(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(register_webhook())

