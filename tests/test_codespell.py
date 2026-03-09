"""Tests for codespell configuration."""

import subprocess


class TestCodespell:
    """Test that codespell is configured and passing."""

    def test_codespell_command_exists(self):
        """Verify codespell command is available."""
        result = subprocess.run(
            ["uv", "run", "codespell", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_codespell_passes(self):
        """Verify no typos in codebase."""
        result = subprocess.run(
            ["uv", "run", "codespell"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Typos found:\n{result.stdout}\n{result.stderr}"

    def test_codespell_config_in_pyproject(self):
        """Verify codespell configuration exists in pyproject.toml."""
        from pathlib import Path

        import tomllib

        pyproject = Path("pyproject.toml")
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        assert "codespell" in config.get("tool", {}), "codespell config missing from pyproject.toml"

    def test_ignore_list_includes_geospatial_terms(self):
        """Verify common geospatial terms are in ignore list."""
        from pathlib import Path

        import tomllib

        pyproject = Path("pyproject.toml")
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        ignore_list = config.get("tool", {}).get("codespell", {}).get("ignore-words-list", "")
        # crs is a common geospatial abbreviation
        assert "crs" in ignore_list, "crs should be in ignore list"
