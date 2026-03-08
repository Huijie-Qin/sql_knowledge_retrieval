from pathlib import Path
from config.settings import Settings

def test_settings_load():
    settings = Settings()
    assert settings.llm_api_key is not None
    assert settings.source_dir == Path("./案例")
    assert settings.output_dir == Path("./数据源")
    assert settings.max_context_tokens > 0
