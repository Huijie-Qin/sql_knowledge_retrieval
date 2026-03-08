from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    llm_api_key: str
    llm_base_url: str = "https://api.openai.com/v1"
    llm_model: str = "gpt-4o-mini"
    source_dir: Path = Path("./案例")
    output_dir: Path = Path("./数据源")
    max_context_tokens: int = 128000

settings = Settings()
