"""Tests for trendscope.universe — base load and local-override merging."""
from __future__ import annotations

from pathlib import Path

from trendscope.universe import Universe

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
BASE_YAML = """
groups:
  mega_cap:
    description: "Mega caps"
    tickers: [AAPL, MSFT]
  holdings:
    description: "Placeholder"
    tickers: [TSLA]

sector_etf_map:
  AAPL: XLK
  MSFT: XLK
"""


def _write(path: Path, text: str) -> Path:
    path.write_text(text)
    return path


# ---------------------------------------------------------------------------
# Base loader
# ---------------------------------------------------------------------------
def test_from_yaml_loads_base(tmp_path: Path) -> None:
    base = _write(tmp_path / "universe.yaml", BASE_YAML)
    u = Universe.from_yaml(base)
    assert u.all_tickers == ["AAPL", "MSFT", "TSLA"]
    assert u.benchmark_for("AAPL") == "XLK"
    assert u.benchmark_for("TSLA") == "SPY"  # default fallback
    assert u.groups_for("TSLA") == ["holdings"]


def test_from_yaml_without_local_file_returns_base(tmp_path: Path) -> None:
    base = _write(tmp_path / "universe.yaml", BASE_YAML)
    # No universe.local.yaml exists — should behave identically to base load.
    u = Universe.from_yaml(base)
    assert "holdings" in u.groups
    assert u.groups["holdings"].tickers == ["TSLA"]


# ---------------------------------------------------------------------------
# Local override behavior
# ---------------------------------------------------------------------------
def test_local_override_replaces_existing_group(tmp_path: Path) -> None:
    base = _write(tmp_path / "universe.yaml", BASE_YAML)
    _write(
        tmp_path / "universe.local.yaml",
        """
groups:
  holdings:
    description: "Real positions"
    tickers: [NVDA, AMZN]
""",
    )
    u = Universe.from_yaml(base)
    # holdings group fully replaced
    assert u.groups["holdings"].tickers == ["NVDA", "AMZN"]
    assert u.groups["holdings"].description == "Real positions"
    # mega_cap untouched
    assert u.groups["mega_cap"].tickers == ["AAPL", "MSFT"]


def test_local_override_adds_new_group(tmp_path: Path) -> None:
    base = _write(tmp_path / "universe.yaml", BASE_YAML)
    _write(
        tmp_path / "universe.local.yaml",
        """
groups:
  watchlist:
    description: "Stuff I'm watching"
    tickers: [COIN, HOOD]
""",
    )
    u = Universe.from_yaml(base)
    assert "watchlist" in u.groups
    assert u.groups["watchlist"].tickers == ["COIN", "HOOD"]
    # Original groups still present
    assert "mega_cap" in u.groups
    assert "holdings" in u.groups


def test_local_override_merges_sector_etf_map(tmp_path: Path) -> None:
    base = _write(tmp_path / "universe.yaml", BASE_YAML)
    _write(
        tmp_path / "universe.local.yaml",
        """
sector_etf_map:
  AAPL: QQQ        # override existing
  COIN: XLF        # add new
""",
    )
    u = Universe.from_yaml(base)
    assert u.benchmark_for("AAPL") == "QQQ"  # overridden
    assert u.benchmark_for("MSFT") == "XLK"  # preserved from base
    assert u.benchmark_for("COIN") == "XLF"  # added
    assert u.benchmark_for("UNKNOWN") == "SPY"  # default fallback still works


def test_local_overrides_disabled(tmp_path: Path) -> None:
    base = _write(tmp_path / "universe.yaml", BASE_YAML)
    _write(
        tmp_path / "universe.local.yaml",
        """
groups:
  holdings:
    description: "Should be ignored"
    tickers: [SHOULD_NOT_APPEAR]
""",
    )
    u = Universe.from_yaml(base, local_overrides=False)
    # Local file present but disabled → base wins
    assert u.groups["holdings"].tickers == ["TSLA"]
