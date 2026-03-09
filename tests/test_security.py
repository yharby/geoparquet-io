"""Tests for security scanning configuration."""

import subprocess
from pathlib import Path

import pytest
import tomllib


class TestSecurityScanning:
    """Test that security tools are configured and passing."""

    def test_bandit_command_exists(self):
        """Verify bandit command is available."""
        result = subprocess.run(
            ["uv", "run", "bandit", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_bandit_passes(self):
        """Verify bandit security scan passes."""
        result = subprocess.run(
            ["uv", "run", "bandit", "-r", "geoparquet_io/", "-c", "pyproject.toml"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Bandit found issues:\n{result.stdout}\n{result.stderr}"

    def test_pip_audit_command_exists(self):
        """Verify pip-audit command is available."""
        result = subprocess.run(
            ["uv", "run", "pip-audit", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    @pytest.mark.network
    def test_pip_audit_passes(self):
        """Verify no known vulnerabilities in dependencies."""
        result = subprocess.run(
            ["uv", "run", "pip-audit"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Vulnerabilities found:\n{result.stdout}\n{result.stderr}"

    def test_bandit_config_in_pyproject(self):
        """Verify bandit configuration exists in pyproject.toml."""
        pyproject = Path("pyproject.toml")
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        assert "bandit" in config.get("tool", {}), "bandit config missing from pyproject.toml"

    def test_bandit_config_has_exclude_dirs(self):
        """Verify bandit config excludes test and venv directories."""
        pyproject = Path("pyproject.toml")
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        bandit_config = config["tool"]["bandit"]
        assert "exclude_dirs" in bandit_config, "bandit config missing exclude_dirs"
        exclude_dirs = bandit_config["exclude_dirs"]
        assert "tests" in exclude_dirs, "tests dir not excluded from bandit scan"
        assert ".venv" in exclude_dirs, ".venv dir not excluded from bandit scan"

    def test_bandit_and_pip_audit_in_dev_deps(self):
        """Verify bandit and pip-audit are listed as dev dependencies."""
        pyproject = Path("pyproject.toml")
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        dev_deps = config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        dep_names = [dep.split(">=")[0].split("==")[0].strip().lower() for dep in dev_deps]
        assert "bandit" in dep_names, "bandit not found in dev extras"
        assert "pip-audit" in dep_names, "pip-audit not found in dev extras"
