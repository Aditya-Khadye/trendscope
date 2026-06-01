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
    def from_yaml(cls, path: Path, *, local_overrides: bool = True) -> Universe:
        """Load the universe from YAML, optionally overlaying a `.local.yaml` sibling.

        With `local_overrides=True` (default), if a sibling file named
        `<stem>.local.yaml` exists next to `path`, its contents are merged on top:
          - `groups`: per-group replace (local fully replaces any same-named group;
            new local-only groups are added)
          - `sector_etf_map`: shallow per-key merge (local entries override base entries)

        The local file is gitignored (see `.gitignore`), so it can hold private
        info like real holdings without ever being pushed.
        """
        with path.open() as f:
            data = yaml.safe_load(f) or {}
        base = cls.model_validate(data)

        if not local_overrides:
            return base

        local_path = path.with_name(f"{path.stem}.local{path.suffix}")
        if not local_path.exists():
            return base

        with local_path.open() as f:
            overrides = yaml.safe_load(f) or {}

        merged_groups = dict(base.groups)
        for name, group_data in (overrides.get("groups") or {}).items():
            merged_groups[name] = TickerGroup.model_validate(group_data)

        merged_sector_map = {
            **base.sector_etf_map,
            **(overrides.get("sector_etf_map") or {}),
        }

        return cls(groups=merged_groups, sector_etf_map=merged_sector_map)

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
