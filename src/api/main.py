from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import auth, portfolio, leaderboard, wallet
from src.config import settings
import logging

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


@app.get("/")
async def root():
    return {"message": "TON Wallet Tracker API", "status": "active"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
