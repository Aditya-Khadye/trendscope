"""Tests for settings loading and the TRENDSCOPE_SETTINGS_PATH override."""
from __future__ import annotations

from pathlib import Path

import pytest

from trendscope.settings import Settings, get_settings


def test_settings_load_from_repo_yaml() -> None:
    get_settings.cache_clear()
    s = get_settings()
    assert s.paths.duckdb.is_absolute()
    assert s.digest.llm.model.startswith("claude-")
    get_settings.cache_clear()


def test_settings_path_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    alt = tmp_path / "alt-settings.yaml"
    alt.write_text(
        """
paths:
  duckdb: "elsewhere/db.duckdb"
  digests: "elsewhere/digests/"
  universe_yaml: "elsewhere/universe.yaml"
digest:
  llm:
    model: "claude-test-override"
"""
    )
    monkeypatch.setenv("TRENDSCOPE_SETTINGS_PATH", str(alt))
    s = Settings()
    assert s.digest.llm.model == "claude-test-override"
    assert s.paths.duckdb == Path("elsewhere/db.duckdb")
