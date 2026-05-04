"""Universe loader: tickers and groups from config/universe.yaml."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field

DEFAULT_BENCHMARK = "SPY"


class TickerGroup(BaseModel):
    description: str
    tickers: list[str]


class Universe(BaseModel):
    """The set of tickers being tracked, plus their group memberships and benchmarks."""

    groups: dict[str, TickerGroup]
    sector_etf_map: dict[str, str] = Field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: Path) -> Universe:
        with path.open() as f:
            data = yaml.safe_load(f) or {}
        return cls.model_validate(data)

    @property
    def all_tickers(self) -> list[str]:
        """Deduplicated, sorted union of every ticker across all groups."""
        seen: set[str] = set()
        for group in self.groups.values():
            seen.update(group.tickers)
        return sorted(seen)

    def groups_for(self, ticker: str) -> list[str]:
        """Group names this ticker belongs to, sorted."""
        return sorted(name for name, g in self.groups.items() if ticker in g.tickers)

    def benchmark_for(self, ticker: str) -> str:
        """Sector ETF used for relative-strength signals; falls back to SPY."""
        return self.sector_etf_map.get(ticker, DEFAULT_BENCHMARK)
