import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv(dotenv_path=".env")

def get_required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(f"🌪️ Variable de entorno obligatoria '{key}' no está definida en .env")
    return value

@dataclass
class Config:
    # === OLLAMA ===
    OLLAMA_URL: str = os.getenv("OLLAMA_URL")
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL")
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL")

    # === QDRANT ===
    QDRANT_URL: str = os.getenv("QDRANT_URL")
    QDRANT_COLLECTION: str = os.getenv("QDRANT_COLLECTION")

    # === PostgreSQL (⚠️ sin defaults para password) ===
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT"))
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD: str = get_required_env("POSTGRES_PASSWORD")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB")

    # === n8n ===
    N8N_ENCRYPTION_KEY: str = get_required_env("N8N_ENCRYPTION_KEY")
    N8N_JWT_SECRET: str = get_required_env("N8N_USER_MANAGEMENT_JWT_SECRET")
    MCP_BRAVE_API_KEY: str = os.getenv("MCP_BRAVE_API_KEY")
    WEBHOOK_URL: Optional[str] = os.getenv("WEBHOOK_URL")

    # === Postiz ===
    POSTIZ_MAIN_URL: Optional[str] = os.getenv("POSTIZ_MAIN_URL")
    POSTIZ_FRONTEND_URL: Optional[str] = os.getenv("POSTIZ_FRONTEND_URL")
    POSTIZ_BACKEND_URL: Optional[str] = os.getenv("POSTIZ_BACKEND_URL")
    POSTIZ_JWT_SECRET: Optional[str] = os.getenv("POSTIZ_JWT_SECRET")
    POSTIZ_NOT_SECURED: bool = os.getenv("POSTIZ_NOT_SECURED", "false").lower() == "true"
    POSTIZ_DISABLE_REGISTRATION: bool = os.getenv("POSTIZ_DISABLE_REGISTRATION", "false").lower() == "true"

    # === Análisis ===
    TOXICITY_THRESHOLD: float = float(os.getenv("TOXICITY_THRESHOLD", 0.7))
    SPAM_THRESHOLD: float = float(os.getenv("SPAM_THRESHOLD", 0.8))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 50))

    # === Paths ===
    LOGS_PATH: str = os.getenv("LOGS_PATH", "./data/logs")
    OUTPUT_PATH: str = os.getenv("OUTPUT_PATH", "./data/processed")

# Exportar configuración centralizada
config = Config()
