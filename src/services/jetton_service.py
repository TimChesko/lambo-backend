from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.models import Wallet
from src.services.ton_service import ton_service
from src.services.leaderboard_service import update_leaderboard
from src.config import settings
from src.utils.address_utils import normalize_address
import logging
import asyncio

logger = logging.getLogger(__name__)


async def process_jetton_operations(
    db: AsyncSession,
    wallet_id: int,
    address: str,
    last_lt: Optional[str] = None
) -> Dict[str, Any]:
    logger.info(f"🔍 Processing wallet {address[:8]}... last_lt={last_lt}")
    
    wallet_result = await db.execute(select(Wallet).where(Wallet.id == wallet_id))
    wallet = wallet_result.scalar_one_or_none()
    
    if not wallet:
        logger.warning(f"❌ Wallet {wallet_id} not found")
        return {"processed": 0, "buy_volume": 0, "sell_volume": 0}
    
    # Если это первый запуск (нет last_lt), берем только с указанной даты
    start_date_timestamp = None
    if not last_lt:
        try:
            start_date_dt = datetime.strptime(settings.start_date, "%Y-%m-%d")
            start_date_timestamp = int(start_date_dt.timestamp())
            logger.info(f"⏰ First run: fetching from {settings.start_date} (timestamp: {start_date_timestamp})")
        except Exception as e:
            logger.warning(f"⚠️  Failed to parse start_date '{settings.start_date}': {e}")
    
    all_operations = []
    has_more = True
    page = 0
    current_before_lt = None  # Сначала получаем НОВЫЕ события (без before_lt)
    
    while has_more:
        page += 1
        logger.info(f"📄 Fetching page {page}, before_lt={current_before_lt}")
        
        result = await ton_service.get_jetton_history(
            address=address,
            jetton_master=settings.jetton_master,
            limit=100,
            before_lt=current_before_lt
        )
        
        events = result.get("events", [])
        next_from = result.get("next_from")
        
        logger.info(f"📦 Received {len(events)} events, next_from={next_from}")
        
        # Rate limit: пауза между запросами API
        if page > 1:  # Не спим после первого запроса
            delay = 1.0 / settings.requests_per_second
            await asyncio.sleep(delay)
        
        if not events:
            logger.info(f"✅ No more events")
            break
        
        new_events_count = 0
        for event in events:
            event_lt = str(event.get("lt", 0))
            event_timestamp = event.get("utime", 0)
            
            # Если last_lt задан, берем только события НОВЕЕ него
            if last_lt and int(event_lt) <= int(last_lt):
                logger.info(f"⏹️  Reached last_lt={last_lt}, stopping")
                has_more = False
                break
            
            # Если это первый запуск (есть start_date_timestamp), останавливаемся на старых событиях
            if start_date_timestamp and event_timestamp < start_date_timestamp:
                logger.info(f"⏹️  Reached start_date (event from {datetime.fromtimestamp(event_timestamp)}), stopping")
                has_more = False
                break
            
            all_operations.append(event)
            new_events_count += 1
        
        logger.info(f"   Added {new_events_count} new events")
        
        # Продолжаем только если есть next_from и не достигли last_lt
        if next_from and has_more:
            current_before_lt = str(next_from)
        else:
            has_more = False
    
    buy_volume = 0.0
    sell_volume = 0.0
    buy_count = 0
    sell_count = 0
    processed_count = 0
    latest_lt = wallet.last_transaction_lt
    
    logger.info(f"🔄 Processing {len(all_operations)} operations for {address[:8]}...")
    logger.info(f"🎯 Target Jetton: {settings.jetton_master}")
    logger.info(f"🎯 Pool Address: {settings.lambo_pool_address}")
    
    for idx, event in enumerate(all_operations, 1):
        operation = event.get("operation")
        if operation != "transfer":
            logger.debug(f"⏭️  Event {idx}: Skip non-transfer operation: {operation}")
            continue
        
        event_lt = str(event.get("lt", 0))
        event_timestamp = event.get("utime", 0)
        
        if not latest_lt or int(event_lt) > int(latest_lt):
            latest_lt = event_lt
        
        jetton = event.get("jetton", {})
        jetton_address = jetton.get("address", "")
        
        normalized_jetton = normalize_address(jetton_address)
        normalized_jetton_master = normalize_address(settings.jetton_master)
        
        logger.debug(f"🔍 Event {idx}: Jetton={normalized_jetton[:12]}... Target={normalized_jetton_master[:12]}...")
        
        if normalized_jetton != normalized_jetton_master:
            logger.debug(f"⏭️  Event {idx}: Skip - wrong jetton")
            continue
        
        logger.info(f"✅ Event {idx}: LAMBO transfer found! lt={event_lt}")
        
        amount_str = str(event.get("amount", "0"))
        amount = float(amount_str) / 1_000_000_000
        
        source = event.get("source", {})
        destination = event.get("destination", {})
        sender = source.get("address", "")
        recipient = destination.get("address", "")
        
        normalized_address = normalize_address(address)
        normalized_sender = normalize_address(sender)
        normalized_recipient = normalize_address(recipient)
        normalized_pool = normalize_address(settings.lambo_pool_address)
        
        logger.info(f"   From: {normalized_sender[:12]}...")
        logger.info(f"   To:   {normalized_recipient[:12]}...")
        logger.info(f"   Wallet: {normalized_address[:12]}...")
        
        if normalized_sender == normalized_address and normalized_recipient == normalized_pool:
            sell_volume += amount
            sell_count += 1
            logger.info(f"   💰 SELL: {amount:.2f} LAMBO → Pool")
        elif normalized_recipient == normalized_address and normalized_sender == normalized_pool:
            buy_volume += amount
            buy_count += 1
            logger.info(f"   💰 BUY: {amount:.2f} LAMBO ← Pool")
        else:
            logger.info(f"   ⏭️  Skip - not pool transaction")
            continue
        
        processed_count += 1
    
    old_buy = wallet.buy_volume or 0
    old_sell = wallet.sell_volume or 0
    old_total = wallet.total_volume or 0
    old_buy_count = wallet.buy_count or 0
    old_sell_count = wallet.sell_count or 0
    
    wallet.buy_volume = old_buy + buy_volume
    wallet.sell_volume = old_sell + sell_volume
    wallet.total_volume = wallet.buy_volume + wallet.sell_volume
    wallet.buy_count = old_buy_count + buy_count
    wallet.sell_count = old_sell_count + sell_count
    wallet.last_transaction_lt = latest_lt
    wallet.last_checked = datetime.utcnow()
    
    await db.commit()
    
    update_leaderboard(wallet.address, wallet.total_volume)
    
    logger.info(f"💾 Saved to DB:")
    logger.info(f"   Processed: {processed_count} new operations")
    logger.info(f"   Buy: {old_buy:.2f} + {buy_volume:.2f} = {wallet.buy_volume:.2f} ({wallet.buy_count} ops)")
    logger.info(f"   Sell: {old_sell:.2f} + {sell_volume:.2f} = {wallet.sell_volume:.2f} ({wallet.sell_count} ops)")
    logger.info(f"   Total: {old_total:.2f} → {wallet.total_volume:.2f}")
    logger.info(f"   Last LT: {latest_lt}")
    
    return {
        "processed": processed_count,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "total_volume": wallet.total_volume
    }

