"""Runtime configuration for DataFlow API."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = Field(
        default="postgresql://dataflow:dataflow@localhost:5432/dataflow",
        validation_alias="DATABASE_URL",
    )
    job_store_backend: str = Field(default="auto", validation_alias="DATAFLOW_JOB_STORE")
    seed_demo_jobs: bool = Field(default=False, validation_alias="DATAFLOW_SEED_DEMO")

    minio_endpoint: str = Field(default="localhost:9000", validation_alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="dataflow", validation_alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="dataflowsecret", validation_alias="MINIO_SECRET_KEY")
    minio_bucket: str = Field(default="dataflow-staging", validation_alias="MINIO_BUCKET")
    minio_secure: bool = Field(default=False, validation_alias="MINIO_SECURE")


settings = Settings()

def get_settings() -> Settings:
    return settings
