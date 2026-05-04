"""Application settings.

YAML at config/settings.yaml is the base. Env vars (and .env) override it.
Resolution order: kwargs > env > .env > YAML.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, BaseModel, Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

# settings.py is at src/trendscope/settings.py — repo root is two levels up.
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS_YAML: Path = PROJECT_ROOT / "config" / "settings.yaml"
DEFAULT_ENV_FILE: Path = PROJECT_ROOT / ".env"


class PathsSettings(BaseModel):
    duckdb: Path
    digests: Path
    universe_yaml: Path

    def absolute(self, root: Path) -> PathsSettings:
        """Anchor relative paths against `root`. Already-absolute paths pass through."""
        def _abs(p: Path) -> Path:
            return p if p.is_absolute() else (root / p).resolve()

        return PathsSettings(
            duckdb=_abs(self.duckdb),
            digests=_abs(self.digests),
            universe_yaml=_abs(self.universe_yaml),
        )


class YFinanceIngestSettings(BaseModel):
    interval: str = "1d"
    auto_adjust: bool = False
    actions: bool = True
    timeout_seconds: int = 30


class IngestRetriesSettings(BaseModel):
    max_attempts: int = 3
    backoff_seconds: int = 2


class IngestSettings(BaseModel):
    default_start: str = "2020-01-01"
    yfinance: YFinanceIngestSettings = Field(default_factory=YFinanceIngestSettings)
    retries: IngestRetriesSettings = Field(default_factory=IngestRetriesSettings)


class TrendSignalSettings(BaseModel):
    fast_ma: int = 50
    slow_ma: int = 200
    adx_period: int = 14


class MomentumSignalSettings(BaseModel):
    horizons_days: list[int] = Field(default_factory=lambda: [5, 21, 63, 252])


class MeanRevSignalSettings(BaseModel):
    rsi_period: int = 14
    zscore_lookback: int = 20


class VolatilitySignalSettings(BaseModel):
    realized_window: int = 20
    percentile_lookback: int = 252


class VolumeSignalSettings(BaseModel):
    avg_window: int = 20
    spike_multiplier: float = 2.0


class BreadthSignalSettings(BaseModel):
    above_ma_window: int = 50


class SignalsSettings(BaseModel):
    trend: TrendSignalSettings = Field(default_factory=TrendSignalSettings)
    momentum: MomentumSignalSettings = Field(default_factory=MomentumSignalSettings)
    meanrev: MeanRevSignalSettings = Field(default_factory=MeanRevSignalSettings)
    volatility: VolatilitySignalSettings = Field(default_factory=VolatilitySignalSettings)
    volume: VolumeSignalSettings = Field(default_factory=VolumeSignalSettings)
    breadth: BreadthSignalSettings = Field(default_factory=BreadthSignalSettings)


class DigestFiltersSettings(BaseModel):
    min_signal_strength: float = 1.5
    max_tickers_per_digest: int = 15


class DigestLLMSettings(BaseModel):
    model: str = "claude-sonnet-4-6"
    max_tokens: int = 4096
    temperature: float = 0.4


class DigestNewsSettings(BaseModel):
    headlines_per_ticker: int = 5


class DigestSettings(BaseModel):
    filters: DigestFiltersSettings = Field(default_factory=DigestFiltersSettings)
    llm: DigestLLMSettings = Field(default_factory=DigestLLMSettings)
    news: DigestNewsSettings = Field(default_factory=DigestNewsSettings)


class LoggingSettings(BaseModel):
    level: str = "INFO"
    format: Literal["console", "json"] = "console"


class DisplaySettings(BaseModel):
    timezone: str = "America/New_York"


class Settings(BaseSettings):
    """Top-level settings — instantiate via `get_settings()` for cached access."""

    model_config = SettingsConfigDict(
        env_file=str(DEFAULT_ENV_FILE),
        env_file_encoding="utf-8",
        env_prefix="TRENDSCOPE_",
        env_nested_delimiter="__",
        extra="ignore",
        yaml_file=str(DEFAULT_SETTINGS_YAML),
        case_sensitive=False,
        populate_by_name=True,
    )

    paths: PathsSettings
    ingest: IngestSettings = Field(default_factory=IngestSettings)
    signals: SignalsSettings = Field(default_factory=SignalsSettings)
    digest: DigestSettings = Field(default_factory=DigestSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    display: DisplaySettings = Field(default_factory=DisplaySettings)

    # Plain ANTHROPIC_API_KEY (no prefix) — also accept TRENDSCOPE_ANTHROPIC_API_KEY.
    anthropic_api_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("ANTHROPIC_API_KEY", "TRENDSCOPE_ANTHROPIC_API_KEY"),
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings accessor. Anchors relative paths against the repo root."""
    s = Settings()
    return s.model_copy(update={"paths": s.paths.absolute(PROJECT_ROOT)})
