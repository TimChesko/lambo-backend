import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import select
from src.database import async_session_maker, init_db
from src.models import Wallet
from src.services.jetton_service import process_jetton_operations
from src.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JettonTracker:
    def __init__(self):
        self.is_running = False
        self.delay = 1.0 / settings.requests_per_second
        self.batch_size = settings.worker_batch_size
        self.last_rebuild = None

    async def start(self):
        logger.info(f"Starting Jetton tracker for Lambo swaps...")
        logger.info(f"Rate limit: {settings.requests_per_second} req/sec (delay: {self.delay:.2f}s)")
        logger.info(f"Batch size: {self.batch_size} wallets per cycle")
        logger.info("🕐 Scheduled leaderboard rebuild every 6 hours")
        
        await init_db()
        
        await self.ensure_leaderboard_ready()
        
        self.is_running = True
        
        while self.is_running:
            try:
                processed = await self.update_wallets_batch()
                await self.check_and_rebuild_leaderboard()
                
                if processed == 0:
                    await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in tracker loop: {e}")
                await asyncio.sleep(10)

    async def stop(self):
        logger.info("Stopping Jetton tracker...")
        self.is_running = False

    async def check_and_rebuild_leaderboard(self):
        now = datetime.utcnow()
        
        if self.last_rebuild is None or (now - self.last_rebuild).total_seconds() >= 6 * 60 * 60:
            try:
                logger.info("🔄 Starting scheduled leaderboard rebuild...")
                async with async_session_maker() as db:
                    from src.services.leaderboard_service import rebuild_leaderboard_from_db
                    result = await rebuild_leaderboard_from_db(db)
                    if result.get("rebuilt"):
                        logger.info(f"✅ Leaderboard rebuild completed: {result}")
                        self.last_rebuild = now
                    else:
                        logger.warning(f"⚠️  Leaderboard rebuild encountered issues: {result}")
            except Exception as e:
                logger.error(f"❌ Error in leaderboard rebuild: {e}")

    async def update_wallets_batch(self):
        async with async_session_maker() as db:
            result = await db.execute(
                select(Wallet)
                .where(
                    Wallet.is_active == True,
                    Wallet.user_id.isnot(None)
                )
                .order_by(
                    Wallet.last_checked.asc().nullsfirst()
                )
                .limit(self.batch_size)
            )
            wallets = result.scalars().all()
            
            pending_count = sum(1 for w in wallets if w.last_checked is None)
            if pending_count > 0:
                logger.info(f"📋 Batch: {pending_count} new (pending) + {len(wallets) - pending_count} existing")
            
            if not wallets:
                return 0
            
            logger.info(f"Processing batch of {len(wallets)} wallets...")
            
            total_new_txs = 0
            for wallet in wallets:
                try:
                    result = await process_jetton_operations(
                        db=db,
                        wallet_id=wallet.id,
                        address=wallet.address,
                        last_lt=wallet.last_transaction_lt
                    )
                    
                    if result["processed"] > 0:
                        total_new_txs += result["processed"]
                        logger.info(
                            f"✅ {wallet.address[:8]}... "
                            f"New: {result['processed']}, "
                            f"Buy: {result['buy_volume']:.2f}, "
                            f"Sell: {result['sell_volume']:.2f}, "
                            f"Total: {result['total_volume']:.2f}"
                        )
                    
                    await asyncio.sleep(self.delay)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {wallet.address[:8]}...: {e}")
                    await asyncio.sleep(self.delay)
            
            if total_new_txs > 0:
                logger.info(f"📊 Batch complete: {total_new_txs} new transactions processed")
            
            return len(wallets)

    async def ensure_leaderboard_ready(self):
        try:
            from src.services.leaderboard_service import get_total_wallets
            
            total = get_total_wallets()
            
            if total == 0:
                logger.warning("⚠️  Redis leaderboard is empty! Rebuilding from database...")
                async with async_session_maker() as db:
                    from src.services.leaderboard_service import rebuild_leaderboard_from_db
                    result = await rebuild_leaderboard_from_db(db)
                    if result.get("rebuilt"):
                        logger.info(f"✅ Leaderboard initialized: {result}")
                        self.last_rebuild = datetime.utcnow()
                    else:
                        logger.warning(f"⚠️  Failed to rebuild leaderboard: {result}")
            else:
                logger.info(f"✅ Redis leaderboard ready: {total} wallets")
        except Exception as e:
            logger.error(f"❌ Error ensuring leaderboard ready: {e}")


async def main():
    tracker = JettonTracker()
    
    try:
        await tracker.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        await tracker.stop()


if __name__ == "__main__":
    asyncio.run(main())

