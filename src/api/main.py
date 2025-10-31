from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from src.api.routes import auth, portfolio, leaderboard, wallet
from src.database import init_db
from src.config import settings
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="TON Wallet Tracker API",
    description="API для отслеживания кошельков TON",
    version="1.0.0"
)

allowed_origins = []
for domain in settings.allowed_domains.split(","):
    domain = domain.strip()
    allowed_origins.append(f"https://{domain}")
    allowed_origins.append(f"http://{domain}")

allowed_origins.extend([
    "http://localhost:3000",
    "http://localhost:5173"
])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/v1/auth", tags=["auth"])
app.include_router(wallet.router, prefix="/v1/wallet", tags=["wallet"])
app.include_router(portfolio.router, prefix="/v1/portfolio", tags=["portfolio"])
app.include_router(leaderboard.router, prefix="/v1/leaderboard", tags=["leaderboard"])


@app.on_event("startup")
async def startup_event():
    logger.info("Initializing database...")
    await init_db()
    logger.info("Database initialized")
    
    from src.services.leaderboard_service import get_total_wallets
    total = get_total_wallets()
    
    if total == 0:
        logger.warning("⚠️  Redis leaderboard is empty! Rebuilding from database...")
        try:
            from src.database import async_session_maker
            from src.services.leaderboard_service import rebuild_leaderboard_from_db
            
            async with async_session_maker() as db:
                result = await rebuild_leaderboard_from_db(db)
                if result.get("rebuilt"):
                    logger.info(f"✅ Leaderboard initialized: {result}")
                else:
                    logger.warning(f"⚠️  Failed to rebuild leaderboard: {result}")
        except Exception as e:
            logger.error(f"❌ Error rebuilding leaderboard: {e}")
    else:
        logger.info(f"✅ Redis leaderboard ready: {total} wallets")


@app.get("/")
async def root():
    return {"message": "TON Wallet Tracker API", "status": "active"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/tonconnect-manifest.json")
async def tonconnect_manifest():
    manifest_path = os.path.join(os.path.dirname(__file__), "../../tonconnect-manifest.json")
    if os.path.exists(manifest_path):
        return FileResponse(manifest_path, media_type="application/json")
    return {"error": "Manifest not found"}

