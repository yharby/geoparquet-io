"""Tests for mypy type checking configuration."""

import subprocess
import sys
from pathlib import Path

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).parent.parent


class TestMypyConfiguration:
    """Test that mypy is configured and passing."""

    def test_mypy_command_exists(self):
        """Verify mypy command is available."""
        result = subprocess.run(
            ["uv", "run", "mypy", "--version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "mypy" in result.stdout

    @pytest.mark.slow
    def test_mypy_passes_on_package(self):
        """Verify mypy passes on the main package."""
        result = subprocess.run(
            ["uv", "run", "mypy", "geoparquet_io/"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"mypy failed:\n{result.stdout}\n{result.stderr}"

    def test_mypy_config_in_pyproject(self):
        """Verify mypy configuration exists in pyproject.toml."""
        pyproject = PROJECT_ROOT / "pyproject.toml"
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        assert "mypy" in config.get("tool", {}), "mypy config missing from pyproject.toml"

    def test_mypy_config_has_required_settings(self):
        """Verify mypy config has the expected base settings."""
        pyproject = PROJECT_ROOT / "pyproject.toml"
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        mypy_config = config["tool"]["mypy"]
        assert mypy_config.get("python_version") == "3.10"
        assert mypy_config.get("ignore_missing_imports") is True
