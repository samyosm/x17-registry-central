from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="X17_", env_file=".env", extra="ignore")

    database_path: Path
    sqlite_timeout_seconds: float = Field(gt=0)
    log_level: Literal["critical", "error", "warning", "info", "debug"]


class Settings(CommonSettings):
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    api_prefix: str
    api_token: SecretStr
    page_size: int = Field(gt=0)
    max_page_size: int = Field(gt=0)

    @field_validator("api_prefix")
    @classmethod
    def valid_prefix(cls, value: str) -> str:
        if not value.startswith("/") or value.endswith("/"):
            raise ValueError("API prefix must begin with / and have no trailing /.")
        return value


class PollSettings(CommonSettings):
    poll_interval_seconds: float = Field(gt=0)
    trigger_history_path: Path
    trigger_config_path: Path
    trigger_programmed_path: Path
    trigger_source_instance: str = Field(min_length=1)
    logbook_database_path: Path
    logbook_source_instance: str = Field(min_length=1)
    influx_enabled: bool
    influx_url: str | None
    influx_org: str | None
    influx_bucket: str | None
    influx_measurement: str | None = Field(pattern=r"^run_[0-9]+$")
    influx_token: SecretStr | None
    influx_source_instance: str | None
    influx_start_at: AwareDatetime | None
    influx_overlap_seconds: int = Field(ge=0)
    influx_window_seconds: int = Field(gt=0)
    influx_timeout_seconds: float = Field(gt=0)
    influx_backfill_batch_size: int = Field(gt=0)

    @field_validator(
        "influx_url",
        "influx_org",
        "influx_bucket",
        "influx_measurement",
        "influx_token",
        "influx_source_instance",
        "influx_start_at",
        mode="before",
    )
    @classmethod
    def empty_influx_setting(cls, value: object) -> object:
        return None if value == "" else value

    @model_validator(mode="after")
    def validate_influx(self) -> "PollSettings":
        if self.influx_enabled:
            required = (
                self.influx_url,
                self.influx_org,
                self.influx_bucket,
                self.influx_measurement,
                self.influx_token,
                self.influx_source_instance,
                self.influx_start_at,
            )
            if any(not item for item in required):
                raise ValueError(
                    "Enabled Influx polling requires URL, org, bucket, token, "
                    "source instance, and start time."
                )
            if self.influx_token is not None and not self.influx_token.get_secret_value():
                raise ValueError("Enabled Influx polling requires a nonempty token.")
        if self.database_path.resolve() == self.logbook_database_path.resolve():
            raise ValueError("The registry database must differ from the source logbook database.")
        if self.influx_overlap_seconds >= self.influx_window_seconds:
            raise ValueError("Influx overlap must be shorter than its polling window.")
        return self


class BackfillSettings(PollSettings):
    influx_backfill_min_window_seconds: float = Field(gt=0)
    influx_backfill_progress_seconds: float = Field(gt=0)
    influx_backfill_pending_batches: int = Field(gt=0)
    influx_backfill_runs_only: bool
    influx_backfill_sqlite_synchronous: Literal["FULL", "NORMAL", "OFF"]
