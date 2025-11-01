import asyncio
from src.database import async_session_maker, init_db
from src.models import Pool
from src.config import settings
from sqlalchemy import select
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def init_lambo_pool():
    await init_db()
    
    async with async_session_maker() as db:
        result = await db.execute(
            select(Pool).where(Pool.address == settings.lambo_pool_address)
        )
        existing_pool = result.scalar_one_or_none()
        
        if existing_pool:
            logger.info(f"✅ LAMBO pool already exists: {existing_pool.address}")
            if not existing_pool.is_active:
                existing_pool.is_active = True
                await db.commit()
                logger.info("✅ Activated existing pool")
        else:
            new_pool = Pool(
                address=settings.lambo_pool_address,
                name="LAMBO/TON StonFi Pool",
                jetton_master=settings.jetton_master,
                is_active=True
            )
            db.add(new_pool)
            await db.commit()
            logger.info(f"✅ Created new LAMBO pool: {new_pool.address}")


if __name__ == "__main__":
    asyncio.run(init_lambo_pool())

