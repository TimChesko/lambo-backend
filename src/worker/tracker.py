import asyncio
import logging
import httpx
from datetime import datetime
from sqlalchemy import select, func, case
from src.database import async_session_maker, init_db
from src.models import Pool, Transaction, Wallet
from src.config import settings
from src.worker.transactions import TransactionProcessor
from src.worker.sse import SSEMonitor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class JettonTracker:
    def __init__(self):
        self.is_running = False
        self.delay = 1.0 / settings.requests_per_second
        self.batch_size = settings.worker_batch_size
        self.processor = TransactionProcessor()
        self.sse_monitors = []
    
    async def start(self):
        logger.info(f"🚀 Starting Jetton Tracker...")
        logger.info(f"Rate limit: {settings.requests_per_second} req/sec")
        logger.info(f"Batch size: {self.batch_size}")
        
        await init_db()
        await self.ensure_leaderboard_ready()
        await self.start_sse_monitors()
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
    
    async def start_sse_monitors(self):
        async with async_session_maker() as db:
            result = await db.execute(
                select(Pool).where(Pool.is_active == True)
            )
            pools = result.scalars().all()
            
            if not pools:
                logger.warning("⚠️  No active pools found!")
                return
            
            for pool in pools:
                monitor = SSEMonitor(pool)
                self.sse_monitors.append(monitor)
                asyncio.create_task(monitor.start())
                logger.info(f"Started SSE monitor for pool: {pool.name or pool.address[:8]}")
    
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
                    skipped = 0
                    logger.info(f"🔍 Filtering {len(all_txs)} transactions with parallel batch processing...")
                    
                    # Преобразуем last_processed_lt в число для сравнения
                    stop_at_lt = int(pool.last_processed_lt) if pool.last_processed_lt else 0
                    if stop_at_lt > 0:
                        logger.info(f"⛔ Will stop processing at LT {stop_at_lt} (previously synced)")
                    
                    max_added_lt = 0  # Отслеживаем максимальный LT только для ДОБАВЛЕННЫХ транзакций
                    
                    # Используем семафор для ограничения одновременных запросов согласно RPS
                    max_concurrent_checks = 2  # Максимум 2 одновременно для безопасности от 429
                    filter_semaphore = asyncio.Semaphore(max_concurrent_checks)
                    logger.info(f"⏱️  Using {max_concurrent_checks} concurrent checks for LAMBO verification")
                    
                    async def check_lambo_with_limit(tx_hash, jetton_master):
                        """Проверяет LAMBO с ограничением по семафору"""
                        async with filter_semaphore:
                            return await self.processor.is_lambo_transaction(tx_hash, jetton_master)
                    
                    filter_batch_size = max_concurrent_checks * 2  # Динамический размер батча
                    
                    # Разбиваем на батчи для параллельной обработки
                    for batch_idx in range(0, len(all_txs), filter_batch_size):
                        batch = all_txs[batch_idx:batch_idx + filter_batch_size]
                        batch_progress = min(batch_idx + filter_batch_size, len(all_txs))
                        
                        if batch_progress % 100 == 0 or batch_progress == len(all_txs):
                            logger.info(f"   Progress: {batch_progress}/{len(all_txs)}, added: {added}, skipped: {skipped}")
                        
                        # Подготавливаем список задач для параллельной проверки
                        check_tasks = []
                        for tx_data in batch:
                            tx_lt = int(tx_data["lt"])
                            
                            # Если встретили ранее обработанный LT, останавливаемся
                            if stop_at_lt > 0 and tx_lt <= stop_at_lt:
                                logger.info(f"⛔ Reached previously synced LT {tx_lt}, stopping batch processing")
                                break
                            
                            # Проверяем дубликат в БД
                            existing = await db.execute(
                                select(Transaction).where(Transaction.tx_hash == tx_data["hash"])
                            )
                            if not existing.scalar_one_or_none():
                                # Параллельно проверяем что это LAMBO транзакция (с ограничением)
                                check_tasks.append((tx_data, check_lambo_with_limit(tx_data["hash"], pool.jetton_master)))
                        
                        # Если встретили ранее обработанный LT, выходим из основного цикла
                        if check_tasks and batch and int(batch[-1]["lt"]) <= stop_at_lt:
                            logger.info(f"📍 Batch contains previously synced data, finishing...")
                        
                        # Ждем результаты всех проверок параллельно
                        if check_tasks:
                            results = await asyncio.gather(*[task for _, task in check_tasks], return_exceptions=True)
                            
                            for (tx_data, _), is_lambo_result in zip(check_tasks, results):
                                if isinstance(is_lambo_result, Exception):
                                    logger.error(f"Error checking tx {tx_data['hash']}: {is_lambo_result}")
                                    skipped += 1
                                    continue
                                
                                if not is_lambo_result:
                                    skipped += 1
                                    continue
                                
                                tx = Transaction(
                                    tx_hash=tx_data["hash"],
                                    lt=str(tx_data["lt"]),
                                    timestamp=tx_data["utime"],
                                    pool_id=pool.id,
                                    is_processed=False
                                )
                                db.add(tx)
                                added += 1
                                max_added_lt = max(max_added_lt, int(tx_data["lt"]))
                        
                        # Commit каждые 50 добавленных транзакций
                        if added % 50 == 0:
                            await db.commit()
                            # Сохраняем промежуточный LT чтобы знать где остановились
                            if max_added_lt > stop_at_lt:
                                pool.last_processed_lt = str(max_added_lt)
                                pool.last_sync_timestamp = int(datetime.utcnow().timestamp())
                                await db.commit()
                                logger.info(f"💾 Intermediate checkpoint: saved LT {max_added_lt} after {added} transactions")
                                max_added_lt = 0  # Сбрасываем для следующего батча
                    
                    await db.commit()
                    logger.info(f"📊 Filtered: added {added} LAMBO txs, skipped {skipped} non-LAMBO")
                    
                    # Обновляем last_processed_lt только если добавили новые транзакции
                    # или если это первая синхронизация
                    if added > 0 and all_txs:
                        # Находим максимальный LT среди новых добавленных (не всех загруженных)
                        # Используем LT первой транзакции которая была добавлена
                        latest_lt = max(int(tx["lt"]) for tx in all_txs if int(tx["lt"]) > stop_at_lt)
                        pool.last_processed_lt = str(latest_lt)
                        pool.last_sync_timestamp = int(datetime.utcnow().timestamp())
                        await db.commit()
                        logger.info(f"✅ Pool {pool.name}: synced to LT {latest_lt}, added {added} new transactions")
                    elif stop_at_lt > 0:
                        # Уже были обработаны ранее
                        logger.info(f"✅ Pool {pool.name}: no new transactions (already synced up to LT {stop_at_lt})")
                    else:
                        logger.info(f"✅ Pool {pool.name}: no new transactions")
                    
                    pool_sync_time = asyncio.get_event_loop().time() - pool_sync_start
                    logger.info(f"⏱️  Pool sync time: {pool_sync_time:.2f}s")
                    
                except Exception as e:
                    logger.error(f"❌ Error syncing pool {pool.name}: {e}")
        
        total_sync_time = asyncio.get_event_loop().time() - sync_start_time
        logger.info(f"✅ Initial pool sync complete (total time: {total_sync_time:.2f}s)")
    
    async def fetch_pool_transactions(self, pool_address: str, start_timestamp: int = None, after_lt: str = None) -> list:
        url = f"{settings.ton_api_url}/v2/blockchain/accounts/{pool_address}/transactions"
        all_transactions = []
        max_concurrent_requests = 10  # Отправляем до 10 запросов одновременно
        target_rps = settings.requests_per_second  # Целевой RPS (10)
        
        if after_lt:
            logger.info(f"Fetching new transactions after LT {after_lt}")
        
        logger.info(f"🚀 Starting parallel fetch - Target: {target_rps} RPS, Initial concurrent: {max_concurrent_requests}")
        
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
            semaphore = asyncio.Semaphore(max_concurrent_requests)
            
            async def fetch_with_semaphore(before_lt=None):
                async with semaphore:
                    return await fetch_page(client, before_lt)
            
            # Начинаем с первого запроса
            first_result = await fetch_page(client, after_lt)
            if isinstance(first_result, tuple):
                first_page, _ = first_result
            else:
                first_page = first_result
                
            if not first_page:
                logger.info("No transactions found")
                return []
            
            all_transactions.extend(first_page)
            
            # Если вернулось меньше limit, это последняя страница
            if len(first_page) < 1000:
                logger.info("First page has less than limit, no more pages")
                return all_transactions
            
            # Подготавливаем список задач для параллельной загрузки
            current_before_lt = str(first_page[-1]["lt"])
            
            fetch_start = asyncio.get_event_loop().time()
            total_requests = 1  # Первый запрос уже был
            batch_count = 0
            request_times = []
            
            # Загружаем страницы параллельно, проверяя условия
            while True:
                batch_count += 1
                batch_start = asyncio.get_event_loop().time()
                
                # Создаем батч из max_concurrent_requests задач
                batch = []
                for _ in range(max_concurrent_requests):
                    task = asyncio.create_task(fetch_with_semaphore(current_before_lt))
                    batch.append(task)
                
                # Ждем результатов батча
                results = await asyncio.gather(*batch, return_exceptions=True)
                batch_time = asyncio.get_event_loop().time() - batch_start
                
                # Подсчитываем успешные запросы и время
                successful_requests = 0
                batch_requests_time = []
                for result in results:
                    if isinstance(result, tuple) and len(result) == 2:
                        transactions, req_time = result
                        if transactions:  # Успешный запрос
                            successful_requests += 1
                            batch_requests_time.append(req_time)
                            request_times.append(req_time)
                    elif isinstance(result, Exception):
                        logger.error(f"Task error: {result}")
                
                total_requests += successful_requests
                
                # Вычисляем текущий RPS за этот батч
                current_rps = successful_requests / max(batch_time, 0.1)
                
                # Адаптивное увеличение: если RPS < целевого, пробуем увеличить параллелизм
                if current_rps < target_rps * 0.9 and max_concurrent_requests < 30:
                    old_max = max_concurrent_requests
                    max_concurrent_requests = min(max_concurrent_requests + 3, 30)
                    logger.info(f"⚡ Increasing concurrent requests: {old_max} → {max_concurrent_requests} (current RPS: {current_rps:.1f})")
                elif current_rps > target_rps * 1.1 and max_concurrent_requests > 5:
                    old_max = max_concurrent_requests
                    max_concurrent_requests = max(max_concurrent_requests - 2, 5)
                    logger.info(f"⬇️  Decreasing concurrent requests: {old_max} → {max_concurrent_requests} (current RPS: {current_rps:.1f})")
                
                batch_has_data = False
                for result in results:
                    if isinstance(result, tuple):
                        transactions, _ = result
                        if transactions:
                            all_transactions.extend(transactions)
                            batch_has_data = True
                            
                            # Обновляем before_lt для следующего запроса
                            if len(transactions) >= 1000:
                                current_before_lt = str(transactions[-1]["lt"])
                            else:
                                # Если в этом батче была последняя страница, выходим
                                logger.info(f"Reached last page in batch, total: {len(all_transactions)}")
                                fetch_time = asyncio.get_event_loop().time() - fetch_start
                                actual_rps = total_requests / max(fetch_time, 0.1)
                                logger.info(f"📊 Parallel fetch complete: {len(all_transactions)} tx from {total_requests} requests in {fetch_time:.2f}s (avg RPS: {actual_rps:.1f})")
                                return all_transactions
                    elif isinstance(result, Exception):
                        logger.error(f"Task error: {result}")
                
                if not batch_has_data:
                    logger.info(f"No more data in batch, total: {len(all_transactions)}")
                    break
                
                # Проверяем условие start_timestamp если указан
                if not after_lt and start_timestamp and all_transactions:
                    last_tx_time = int(all_transactions[-1]["utime"])
                    if last_tx_time <= start_timestamp:
                        logger.info(f"Reached start_timestamp {start_timestamp}, stopping")
                        break
        
        fetch_time = asyncio.get_event_loop().time() - fetch_start
        actual_rps = total_requests / max(fetch_time, 0.1)
        logger.info(f"📊 Parallel fetch complete: {len(all_transactions)} tx from {total_requests} requests in {fetch_time:.2f}s (avg RPS: {actual_rps:.1f})")
        
        # Фильтруем транзакции по start_timestamp (только если after_lt не указан)
        if not after_lt and start_timestamp:
            all_transactions = [tx for tx in all_transactions if int(tx["utime"]) >= start_timestamp]
            logger.info(f"After filtering by timestamp: {len(all_transactions)} transactions")
        
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
                        Transaction.is_processed == True
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
            
            logger.info(f"🔧 Processing {len(transactions)} pending transactions...")
            
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
            
            if processed_count > 0:
                logger.info(f"✅ Successfully processed {processed_count}/{len(transactions)} transactions")
            
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
