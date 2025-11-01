import asyncio
import logging
import httpx
from datetime import datetime
from sqlalchemy import select, func, case
from src.database import async_session_maker, init_db
from src.models import Pool, Transaction, Wallet
from src.config import settings
from src.worker.transactions import TransactionProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class JettonTracker:
    def __init__(self):
        self.is_running = False
        self.delay = 1.0 / settings.requests_per_second
        self.batch_size = settings.worker_batch_size
        self.processor = TransactionProcessor()
        self.sse_monitors = []
        self.processing_lock = asyncio.Lock()  # 🔒 Только одна обработка за раз
    
    async def start(self):
        logger.info(f"🚀 Starting Jetton Tracker...")
        logger.info(f"Rate limit: {settings.requests_per_second} req/sec")
        logger.info(f"Batch size: {self.batch_size}")
        
        await init_db()
        await self.ensure_leaderboard_ready()
        await self.initial_pool_sync()
        
        self.is_running = True
        
        while self.is_running:
            try:
                synced = await self.sync_new_wallets()
                
                processed = await self.process_pending_transactions()
                
                if synced == 0 and processed == 0:
                    await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in tracker loop: {e}")
                await asyncio.sleep(10)
    
    async def stop(self):
        logger.info("Stopping Jetton tracker...")
        self.is_running = False
        
        for monitor in self.sse_monitors:
            monitor.stop()
        
        await self.processor.close()
    
    async def initial_pool_sync(self):
        logger.info("🔄 Starting initial pool sync...")
        sync_start_time = asyncio.get_event_loop().time()
        
        async with async_session_maker() as db:
            result = await db.execute(select(Pool).where(Pool.is_active == True))
            pools = result.scalars().all()
            
            if not pools:
                logger.warning("No active pools found for sync")
                return
            
            for pool in pools:
                pool_sync_start = asyncio.get_event_loop().time()
                
                # 🚀 Оптимизация: если есть last_processed_lt, используем его для получения только НОВЫХ транзакций
                if pool.last_processed_lt:
                    logger.info(f"📅 Pool {pool.name}: fetching NEW transactions after LT {pool.last_processed_lt}")
                    all_txs = await self.fetch_pool_transactions(
                        pool_address=pool.address,
                        after_lt=pool.last_processed_lt
                    )
                else:
                    # Иначе начинаем с START_DATE (первый запуск)
                    start_timestamp = int(datetime.strptime(settings.start_date, "%Y-%m-%d").timestamp())
                    logger.info(f"📅 Pool {pool.name}: first sync - fetching transactions from {settings.start_date}")
                    all_txs = await self.fetch_pool_transactions(
                        pool_address=pool.address,
                        start_timestamp=start_timestamp
                    )
                
                try:
                    added = 0
                    logger.info(f"💾 Adding {len(all_txs)} transactions to database...")
                    
                    # Преобразуем last_processed_lt в число для сравнения
                    stop_at_lt = int(pool.last_processed_lt) if pool.last_processed_lt else 0
                    if stop_at_lt > 0:
                        logger.info(f"⛔ Will skip transactions already processed (LT <= {stop_at_lt})")
                    
                    # Просто добавляем ВСЕ TX, не проверяя LAMBO
                    # LAMBO проверка будет позже в process_pending_transactions()
                    for tx_data in all_txs:
                        tx_lt = int(tx_data["lt"])
                        
                        # Если встретили ранее обработанный LT, выходим из цикла
                        # (TX отсортирован от новых к старым, так что все следующие будут старше)
                        if stop_at_lt > 0 and tx_lt <= stop_at_lt:
                            logger.info(f"⛔ Reached previously processed LT {tx_lt}, stopping (checkpoint was {stop_at_lt})")
                            break
                        
                        # Проверяем дубликат в БД
                        existing = await db.execute(
                            select(Transaction).where(Transaction.tx_hash == tx_data["hash"])
                        )
                        if existing.scalar_one_or_none():
                            logger.debug(f"   Skipping duplicate TX {tx_data['hash']}")
                            continue
                        
                        # Добавляем TX без фильтрации - процессор проверит LAMBO позже
                        tx = Transaction(
                            tx_hash=tx_data["hash"],
                            lt=str(tx_data["lt"]),
                            timestamp=tx_data["utime"],
                            pool_id=pool.id,
                            is_processed=False
                        )
                        db.add(tx)
                        added += 1
                        
                        # Commit каждые 100 транзакций
                        if added % 100 == 0:
                            await db.commit()
                            logger.info(f"   Progress: {added} transactions added")
                    
                    # Финальный commit
                    await db.commit()
                    logger.info(f"📊 Added: {added} new transactions to process later (out of {len(all_txs)} fetched)")
                    
                    # Обновляем last_processed_lt - используем последний LT из загруженных
                    if all_txs:
                        if stop_at_lt > 0:
                            # Берем максимальный LT из транзакций ВЫШе checkpoint
                            new_txs = [int(tx["lt"]) for tx in all_txs if int(tx["lt"]) > stop_at_lt]
                            if new_txs:
                                latest_lt = max(new_txs)
                                pool.last_processed_lt = str(latest_lt)
                                pool.last_sync_timestamp = int(datetime.utcnow().timestamp())
                                await db.commit()
                                logger.info(f"✅ Pool {pool.name}: checkpoint updated to LT {latest_lt}")
                            else:
                                logger.info(f"⏭️  No new transactions above checkpoint, keeping LT {stop_at_lt}")
                        else:
                            # Первый запуск - берем максимальный LT из всех
                            latest_lt = max(int(tx["lt"]) for tx in all_txs)
                            pool.last_processed_lt = str(latest_lt)
                            pool.last_sync_timestamp = int(datetime.utcnow().timestamp())
                            await db.commit()
                            logger.info(f"✅ Pool {pool.name}: checkpoint saved to LT {latest_lt}")
                    
                    pool_sync_time = asyncio.get_event_loop().time() - pool_sync_start
                    logger.info(f"⏱️  Pool sync time: {pool_sync_time:.2f}s")
                    
                except Exception as e:
                    logger.error(f"❌ Error syncing pool {pool.name}: {e}")
        
        total_sync_time = asyncio.get_event_loop().time() - sync_start_time
        logger.info(f"✅ Initial pool sync complete (total time: {total_sync_time:.2f}s)")
    
    async def fetch_pool_transactions(self, pool_address: str, start_timestamp: int = None, after_lt: str = None) -> list:
        """
        ИСПРАВЛЕННАЯ версия: используем КОНВЕЙЕР (pipeline) вместо параллельных запросов с одной LT
        
        Вместо отправки 10 одинаковых запросов с одной before_lt:
        - Каждый запрос использует LT из результата предыдущего
        - Минимальная задержка между запросами
        - Оптимальный RPS контроль
        
        ВАЖНО: Когда after_lt указан (не первый запуск):
        - Первый запрос БЕЗ before_lt (получим самые новые!)
        - Проверим есть ли там транзакции выше checkpoint
        - Потом ищем дальше вниз с before_lt если нужно
        """
        url = f"{settings.ton_api_url}/v2/blockchain/accounts/{pool_address}/transactions"
        all_transactions = []
        target_rps = settings.requests_per_second
        
        if after_lt:
            logger.info(f"Fetching new transactions after LT {after_lt}")
        
        logger.info(f"🚀 Starting pipeline fetch - Target: {target_rps} RPS")
        
        async def fetch_page(client, before_lt=None):
            """Загружает одну страницу транзакций"""
            params = {"limit": 1000}
            if before_lt:
                params["before_lt"] = before_lt
            
            request_start = asyncio.get_event_loop().time()
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                request_time = asyncio.get_event_loop().time() - request_start
                data = response.json()
                transactions = data.get("transactions", [])
                return transactions, request_time
            except Exception as e:
                logger.error(f"Error fetching page: {e}")
                return [], 0
        
        async with httpx.AsyncClient(
            timeout=30.0,
            headers={'Authorization': f'Bearer {settings.ton_api_key}'}
        ) as client:
            fetch_start = asyncio.get_event_loop().time()
            total_requests = 0
            request_times = []
            
            # КОНВЕЙЕР: каждый запрос использует LT из результата предыдущего
            current_before_lt = None  # Начинаем БЕЗ before_lt, даже если after_lt есть!
            first_page = True
            
            while True:
                # Получаем одну страницу
                page, req_time = await fetch_page(client, current_before_lt)
                total_requests += 1
                request_times.append(req_time)
                
                if not page:
                    logger.info(f"No more transactions, total: {len(all_transactions)}")
                    break
                
                all_transactions.extend(page)
                logger.info(f"Fetched {len(page)} transactions (total: {len(all_transactions)}), LT range: {page[0]['lt']} -> {page[-1]['lt']}")
                
                # На первой странице проверяем если это не первый запуск
                if first_page and after_lt:
                    first_page = False
                    max_lt_in_page = int(page[0]["lt"])
                    if max_lt_in_page <= int(after_lt):
                        logger.info(f"✅ Max LT in first page {max_lt_in_page} <= checkpoint {after_lt}, no new transactions")
                        break
                    else:
                        logger.info(f"📈 Found newer transactions! Max LT: {max_lt_in_page} > checkpoint {after_lt}")
                
                first_page = False
                
                # Условия остановки
                if len(page) < 1000:
                    logger.info(f"Reached last page (only {len(page)} transactions), stopping")
                    break
                
                # ВАЖНО: если это не первый запуск (after_lt указан), проверяем что дошли до checkpoint
                if after_lt and page:
                    min_lt_in_page = int(page[-1]["lt"])  # самый старый LT на этой странице
                    if min_lt_in_page <= int(after_lt):
                        logger.info(f"✅ Reached checkpoint LT {after_lt}, stopping (min LT in page: {min_lt_in_page})")
                        break
                
                if not after_lt and start_timestamp and page:
                    last_tx_time = int(page[-1]["utime"])
                    if last_tx_time <= start_timestamp:
                        logger.info(f"Reached start_timestamp {start_timestamp}, stopping")
                        break
                
                # Обновляем before_lt для СЛЕДУЮЩЕГО запроса
                current_before_lt = str(page[-1]["lt"])
                
                # Контролируем RPS: если нужно, добавляем задержку
                if total_requests > 0:
                    elapsed = asyncio.get_event_loop().time() - fetch_start
                    current_rps = total_requests / max(elapsed, 0.1)
                    if current_rps > target_rps:
                        delay = (total_requests - 1) / target_rps - elapsed
                        if delay > 0:
                            await asyncio.sleep(delay)
                            logger.debug(f"RPS throttle: {current_rps:.1f} → sleeping {delay:.3f}s")
        
        fetch_time = asyncio.get_event_loop().time() - fetch_start
        actual_rps = total_requests / max(fetch_time, 0.1)
        avg_req_time = sum(request_times) / len(request_times) if request_times else 0
        
        logger.info(f"📊 Pipeline fetch complete: {len(all_transactions)} tx from {total_requests} requests in {fetch_time:.2f}s")
        logger.info(f"   Avg RPS: {actual_rps:.1f}, Avg req time: {avg_req_time:.3f}s")
        
        # Финальная фильтрация по timestamp (только если after_lt не указан)
        if not after_lt and start_timestamp:
            before_filter = len(all_transactions)
            all_transactions = [tx for tx in all_transactions if int(tx["utime"]) >= start_timestamp]
            filtered_out = before_filter - len(all_transactions)
            logger.info(f"After filtering by timestamp: {len(all_transactions)} transactions (removed {filtered_out})")
        
        return all_transactions
    
    async def sync_new_wallets(self) -> int:
        async with async_session_maker() as db:
            result = await db.execute(
                select(Wallet).where(
                    Wallet.sync_status == 'pending',
                    Wallet.is_active == True
                ).limit(5)
            )
            wallets = result.scalars().all()
            
            if not wallets:
                return 0
            
            logger.info(f"🔄 Syncing {len(wallets)} new wallets...")
            
            for wallet in wallets:
                wallet.sync_status = 'syncing'
                await db.commit()
                
                result = await db.execute(
                    select(
                        func.coalesce(func.sum(case((Transaction.operation_type == 'buy', Transaction.lambo_amount), else_=0)), 0).label('buy_lambo'),
                        func.coalesce(func.sum(case((Transaction.operation_type == 'buy', Transaction.ton_amount), else_=0)), 0).label('buy_ton'),
                        func.coalesce(func.sum(case((Transaction.operation_type == 'buy', Transaction.ton_amount * Transaction.ton_usd_price), else_=0)), 0).label('buy_usd'),
                        func.coalesce(func.sum(case((Transaction.operation_type == 'sell', Transaction.lambo_amount), else_=0)), 0).label('sell_lambo'),
                        func.coalesce(func.sum(case((Transaction.operation_type == 'sell', Transaction.ton_amount), else_=0)), 0).label('sell_ton'),
                        func.coalesce(func.sum(case((Transaction.operation_type == 'sell', Transaction.ton_amount * Transaction.ton_usd_price), else_=0)), 0).label('sell_usd'),
                        func.count(Transaction.id).label('tx_count')
                    )
                    .where(
                        Transaction.user_address == wallet.address,
                        Transaction.is_processed == True,
                        Transaction.timestamp >= wallet.created_at
                    )
                )
                
                volumes = result.one()
                
                wallet.buy_volume_lambo = float(volumes.buy_lambo)
                wallet.buy_volume_ton = float(volumes.buy_ton)
                wallet.buy_volume_usd = float(volumes.buy_usd)
                wallet.sell_volume_lambo = float(volumes.sell_lambo)
                wallet.sell_volume_ton = float(volumes.sell_ton)
                wallet.sell_volume_usd = float(volumes.sell_usd)
                wallet.total_volume_lambo = wallet.buy_volume_lambo + wallet.sell_volume_lambo
                wallet.total_volume_ton = wallet.buy_volume_ton + wallet.sell_volume_ton
                wallet.total_volume_usd = wallet.buy_volume_usd + wallet.sell_volume_usd
                
                wallet.sync_status = 'synced'
                wallet.initial_sync_completed = True
                
                await db.commit()
                
                from src.services.leaderboard_service import update_leaderboard
                update_leaderboard(wallet.address, wallet.total_volume_usd)
                
                logger.info(
                    f"✅ Synced {wallet.address[:8]}... "
                    f"from {volumes.tx_count} tx, "
                    f"USD: ${wallet.total_volume_usd:.2f}"
                )
            
            return len(wallets)
    
    async def process_pending_transactions(self) -> int:
        async with self.processing_lock:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(Transaction)
                    .where(Transaction.is_processed == False)
                    .order_by(Transaction.timestamp.asc())
                    .limit(self.batch_size)
                )
                transactions = result.scalars().all()
                
                if not transactions:
                    return 0
                
                processed_count = 0
                for tx in transactions:
                    try:
                        success = await self.processor.process_transaction(tx, db)
                        if success:
                            processed_count += 1
                        
                        await asyncio.sleep(self.delay)
                        
                    except Exception as e:
                        logger.error(f"Error processing tx {tx.tx_hash}: {e}")
                        await asyncio.sleep(self.delay)
                
                return len(transactions)
    
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
