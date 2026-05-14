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
        validation_alias="LLM_BASE_URL",
    )
    api_key: str = Field(
        default="beatrice-dev-key",
        validation_alias="LLM_API_KEY",
    )

    local_extract_model: str = Field(
        default="local_extract",
        validation_alias="MODEL_LOCAL_EXTRACT",
    )
    local_classify_model: str = Field(
        default="local_classify",
        validation_alias="MODEL_LOCAL_CLASSIFY",
    )
    frontier_reason_model: str = Field(
        default="frontier_reason",
        validation_alias="MODEL_FRONTIER_REASON",
    )
    frontier_writeup_model: str = Field(
        default="frontier_writeup",
        validation_alias="MODEL_FRONTIER_WRITEUP",
    )

    guidance_extract_model: str = Field(
        default="local_extract",
        validation_alias="MODEL_GUIDANCE_EXTRACT",
    )
    guidance_classify_model: str = Field(
        default="frontier_reason",
        validation_alias="MODEL_GUIDANCE_CLASSIFY",
    )
    guidance_summarise_model: str = Field(
        default="frontier_reason",
        validation_alias="MODEL_GUIDANCE_SUMMARISE",
    )

    embed_model: str = Field(
        default="local_embed",
        validation_alias="MODEL_EMBED",
    )


settings = LLMSettings()
