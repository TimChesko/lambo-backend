from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select
from src.database import async_session_maker
from src.models import Pool, Transaction
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/tonapi")
async def tonapi_webhook(request: Request):
    """
    Webhook endpoint для получения уведомлений о новых транзакциях от TON API
    """
    try:
        data = await request.json()
        logger.info(f"📡 Webhook: tx received")
        
        # TON API отправляет данные о транзакции напрямую
        event_type = data.get("event_type")
        account_id = data.get("account_id")
        tx_lt = data.get("lt")
        tx_hash = data.get("tx_hash")
        
        if not all([account_id, tx_lt, tx_hash]):
            return {"status": "ok"}
        
        if event_type == "account_tx":
            # Обрабатываем транзакцию
            async with async_session_maker() as db:
                # Находим пул по адресу
                pool_result = await db.execute(
                    select(Pool).where(Pool.address == account_id)
                )
                pool = pool_result.scalar_one_or_none()
                
                if not pool:
                    return {"status": "ok"}
                
                # Проверяем что транзакция еще не существует
                existing = await db.execute(
                    select(Transaction).where(Transaction.tx_hash == tx_hash)
                )
                if existing.scalar_one_or_none():
                    return {"status": "ok"}
                
                # Создаем новую транзакцию
                tx = Transaction(
                    tx_hash=tx_hash,
                    lt=str(tx_lt),
                    timestamp=data.get("timestamp", 0),
                    pool_id=pool.id,
                    is_processed=False
                )
                
                db.add(tx)
                await db.commit()
                
                # Обновляем checkpoint
                if int(tx_lt) > int(pool.last_processed_lt or "0"):
                    pool.last_processed_lt = str(tx_lt)
                    await db.commit()
        
        return {"status": "ok"}
        
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))
