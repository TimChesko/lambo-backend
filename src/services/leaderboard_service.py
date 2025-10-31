import redis
import logging
from typing import List, Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.models import Wallet

logger = logging.getLogger(__name__)

_redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

LEADERBOARD_KEY = "leaderboard:total_volume"


def update_leaderboard(wallet_address: str, total_volume: float):
    try:
        _redis_client.zadd(LEADERBOARD_KEY, {wallet_address: total_volume})
        logger.debug(f"Updated leaderboard: {wallet_address[:12]}... = {total_volume}")
    except Exception as e:
        logger.error(f"Error updating leaderboard: {e}")


def get_rank(wallet_address: str) -> Optional[int]:
    try:
        rank = _redis_client.zrevrank(LEADERBOARD_KEY, wallet_address)
        if rank is not None:
            return rank + 1
        return None
    except Exception as e:
        logger.error(f"Error getting rank: {e}")
        return None


def get_total_wallets() -> int:
    try:
        return _redis_client.zcard(LEADERBOARD_KEY)
    except Exception as e:
        logger.error(f"Error getting total wallets: {e}")
        return 0


def get_top_wallets(limit: int = 100, offset: int = 0) -> List[Dict]:
    try:
        results = _redis_client.zrevrange(
            LEADERBOARD_KEY,
            offset,
            offset + limit - 1,
            withscores=True
        )
        
        leaderboard = []
        for rank, (address, score) in enumerate(results, start=offset + 1):
            leaderboard.append({
                "rank": rank,
                "address": address,
                "total_volume": float(score)
            })
        
        return leaderboard
    except Exception as e:
        logger.error(f"Error getting top wallets: {e}")
        return []


def remove_wallet(wallet_address: str):
    try:
        _redis_client.zrem(LEADERBOARD_KEY, wallet_address)
        logger.debug(f"Removed from leaderboard: {wallet_address[:12]}...")
    except Exception as e:
        logger.error(f"Error removing from leaderboard: {e}")


async def rebuild_leaderboard_from_db(db: AsyncSession) -> Dict[str, int]:
    try:
        _redis_client.delete(LEADERBOARD_KEY)
        logger.info("🔄 Cleared Redis leaderboard")
        
        result = await db.execute(
            select(Wallet)
            .where(Wallet.is_active == True, Wallet.user_id.isnot(None))
            .order_by(Wallet.total_volume.desc())
        )
        wallets = result.scalars().all()
        
        if not wallets:
            logger.warning("⚠️  No active wallets found in database")
            return {"rebuilt": 0, "total": 0}
        
        for wallet in wallets:
            volume = wallet.total_volume or 0.0
            _redis_client.zadd(LEADERBOARD_KEY, {wallet.address: volume})
        
        logger.info(f"✅ Rebuilt leaderboard from DB: {len(wallets)} wallets")
        return {
            "rebuilt": len(wallets),
            "total": len(wallets)
        }
    except Exception as e:
        logger.error(f"❌ Error rebuilding leaderboard: {e}")
        return {"rebuilt": 0, "error": str(e)}

