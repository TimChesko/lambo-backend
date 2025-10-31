from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    database_url: str = Field(..., env="DATABASE_URL")
    bot_token: str = Field(..., env="BOT_TOKEN")
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    public_url: str = Field(default="https://your-domain.com", env="PUBLIC_URL")
    ton_api_url: str = Field(default="https://tonapi.io", env="TON_API_URL")
    ton_api_key: str = Field(default="", env="TON_API_KEY")
    redis_url: str = Field(default="redis://localhost:6379", env="REDIS_URL")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    jetton_master: str = Field(
        default="0:c4d623eb3fcd0bd7b473907dd896e5ec11c9f98be6cf15fb9edb9f6e30a28513",
        env="JETTON_MASTER"
    )
    
    lambo_pool_address: str = Field(
        default="0:031053133270be82ee6fd94d1963c0186868403a4f537040a0d533aab805b7af",
        env="LAMBO_POOL_ADDRESS"
    )
    
    requests_per_second: float = Field(default=1.0, env="REQUESTS_PER_SECOND")
    worker_batch_size: int = Field(default=10, env="WORKER_BATCH_SIZE")
    start_date: str = Field(default="2025-10-28", env="START_DATE")
    
    jwt_secret: str = Field(default="your-secret-key-change-in-production", env="JWT_SECRET")
    jwt_algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    jwt_expiration_hours: int = Field(default=2, env="JWT_EXPIRATION_HOURS")
    allowed_domains: str = Field(default="api.durak.bot,dev.durak.bot,durak.bot", env="ALLOWED_DOMAINS")

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
