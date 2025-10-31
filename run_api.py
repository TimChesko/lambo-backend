import uvicorn
import os
from src.config import settings

if __name__ == "__main__":
    reload = os.getenv("RELOAD", "true").lower() == "true"
    
    uvicorn.run(
        "src.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=reload,
        log_level=settings.log_level.lower()
    )

