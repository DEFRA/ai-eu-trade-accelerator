from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str = Field(
        default="http://127.0.0.1:4000/v1",
        validation_alias="JUDIT_LLM_BASE_URL",
    )
    api_key: str = Field(
        default="judit-dev-key",
        validation_alias="JUDIT_LLM_API_KEY",
    )

    local_extract_model: str = Field(
        default="local_extract",
        validation_alias="JUDIT_MODEL_LOCAL_EXTRACT",
    )
    local_classify_model: str = Field(
        default="local_classify",
        validation_alias="JUDIT_MODEL_LOCAL_CLASSIFY",
    )
    frontier_reason_model: str = Field(
        default="frontier_reason",
        validation_alias="JUDIT_MODEL_FRONTIER_REASON",
    )
    frontier_extract_model: str = Field(
        default="frontier_extract",
        validation_alias="JUDIT_MODEL_FRONTIER_EXTRACT",
    )
    # Conservative max estimated input tokens (prompt + system) before chunking / skipping LLM.
    max_extract_input_tokens: int = Field(
        default=150_000,
        ge=1024,
        validation_alias="JUDIT_LLM_MAX_EXTRACT_INPUT_TOKENS",
    )
    # Documented provider context window for traces / diagnostics (not enforced directly).
    extract_model_context_limit: int = Field(
        default=200_000,
        ge=4096,
        validation_alias="JUDIT_LLM_EXTRACT_MODEL_CONTEXT_LIMIT",
    )
    frontier_writeup_model: str = Field(
        default="frontier_writeup",
        validation_alias="JUDIT_MODEL_FRONTIER_WRITEUP",
    )


settings = LLMSettings()
