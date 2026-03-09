"""Tests for CLAUDE.md validation script.

Tests for scripts/validate_claude_md.py which validates that CLAUDE.md
stays in sync with the actual codebase.
"""

import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "validate_claude_md.py"


def _run_validator(*extra_args: str) -> subprocess.CompletedProcess:
    """Run the validation script and return the CompletedProcess."""
    return subprocess.run(
        ["uv", "run", "python", str(SCRIPT_PATH), *extra_args],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestValidateClaudeMdIntegration:
    """Integration tests that run the full validation script."""

    def test_script_exists(self):
        """Verify validation script exists."""
        assert SCRIPT_PATH.exists(), f"Script not found at {SCRIPT_PATH}"

    def test_script_runs_successfully(self):
        """Verify script runs and passes on current CLAUDE.md."""
        result = _run_validator()
        assert result.returncode == 0, (
            f"Validation failed (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_script_accepts_custom_path(self, tmp_path):
        """Verify script accepts a custom CLAUDE.md path via --claude-md."""
        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
        )
        result = _run_validator("--claude-md", str(fake))
        # Should at least not crash with exit code 2
        assert result.returncode in (0, 1)

    def test_missing_claude_md(self, tmp_path):
        """Script returns exit code 2 when CLAUDE.md is missing."""
        missing = tmp_path / "nonexistent.md"
        result = _run_validator("--claude-md", str(missing))
        assert result.returncode == 2

    def test_catches_invalid_command(self, tmp_path):
        """Detects references to non-existent CLI commands."""
        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
            "\nRun `gpio nonexistent-command` to do stuff.\n"
        )
        result = _run_validator("--claude-md", str(fake))
        assert result.returncode == 1
        assert "nonexistent-command" in result.stdout

    def test_catches_invalid_path(self, tmp_path):
        """Detects references to non-existent file paths."""
        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
            "\nCheck `geoparquet_io/core/does_not_exist.py` for details.\n"
        )
        result = _run_validator("--claude-md", str(fake))
        assert result.returncode == 1
        assert "does_not_exist" in result.stdout

    def test_catches_invalid_marker(self, tmp_path):
        """Detects references to undefined pytest markers."""
        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
            "\nUse `@pytest.mark.nonexistent` for special tests.\n"
        )
        result = _run_validator("--claude-md", str(fake))
        assert result.returncode == 1
        assert "nonexistent" in result.stdout


# ---------------------------------------------------------------------------
# Unit tests: CLI command extraction
# ---------------------------------------------------------------------------


class TestCliCommandValidation:
    """Unit tests for CLI command extraction and validation."""

    def test_extracts_gpio_commands_from_backticks(self):
        """Extracts gpio command groups from backticked references."""
        from scripts.validate_claude_md import extract_gpio_commands

        content = (
            "Run `gpio add bbox` to add bounding boxes.\n"
            "Use `gpio convert crs` for conversion.\n"
            "Try `gpio inspect meta` for metadata.\n"
        )
        commands = extract_gpio_commands(content)
        assert "add" in commands
        assert "convert" in commands
        assert "inspect" in commands

    def test_extracts_commands_from_code_blocks(self):
        """Extracts gpio commands from fenced code blocks."""
        from scripts.validate_claude_md import extract_gpio_commands

        content = "```bash\ngpio add           # Enhance files\ngpio convert       # Convert\n```\n"
        commands = extract_gpio_commands(content)
        assert "add" in commands
        assert "convert" in commands

    def test_gets_actual_cli_commands(self):
        """Introspects the real CLI to get command names."""
        from scripts.validate_claude_md import get_actual_commands

        commands = get_actual_commands()
        assert "add" in commands
        assert "convert" in commands
        assert "inspect" in commands
        assert "extract" in commands
        assert "sort" in commands
        assert "check" in commands
        assert "partition" in commands
        assert "publish" in commands
        assert "benchmark" in commands

    def test_validate_cli_commands_passes_for_valid(self):
        """No errors when all referenced commands exist."""
        from scripts.validate_claude_md import validate_cli_commands

        content = "```bash\ngpio add\ngpio convert\ngpio inspect\n```\n"
        errors = validate_cli_commands(content)
        assert errors == []

    def test_validate_cli_commands_fails_for_invalid(self):
        """Reports error for a non-existent command."""
        from scripts.validate_claude_md import validate_cli_commands

        content = "Run `gpio frobnicate` to do stuff.\n"
        errors = validate_cli_commands(content)
        assert len(errors) == 1
        assert "frobnicate" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: file path extraction
# ---------------------------------------------------------------------------


class TestFilePathValidation:
    """Unit tests for file path extraction and validation."""

    def test_extracts_python_file_paths(self):
        """Extracts file paths that look like Python files."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Check `core/common.py` and `cli/main.py`."
        paths = extract_file_paths(content)
        assert any("core/common.py" in p for p in paths)
        assert any("cli/main.py" in p for p in paths)

    def test_extracts_qualified_paths(self):
        """Extracts fully-qualified paths within geoparquet_io."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Entry point in `geoparquet_io/cli/main.py`."
        paths = extract_file_paths(content)
        assert "geoparquet_io/cli/main.py" in paths

    def test_skips_generic_patterns(self):
        """Does not treat wildcard patterns like `add_*.py` as concrete paths."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Files matching `add_*.py` exist in core."
        paths = extract_file_paths(content)
        assert not any("add_*.py" in p for p in paths)

    def test_skips_template_paths(self):
        """Does not treat template paths like `core/<feature>.py` as concrete paths."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Core logic in `core/<feature>.py` with `*_table()` function."
        paths = extract_file_paths(content)
        assert not any("<feature>" in p for p in paths)

    def test_validate_file_paths_passes_for_valid(self):
        """No errors for paths that exist."""
        from scripts.validate_claude_md import validate_file_paths

        content = "See `geoparquet_io/cli/main.py` for details."
        errors = validate_file_paths(content, PROJECT_ROOT)
        assert errors == []

    def test_validate_file_paths_fails_for_invalid(self):
        """Reports error for non-existent file paths."""
        from scripts.validate_claude_md import validate_file_paths

        content = "See `geoparquet_io/core/does_not_exist.py` for details."
        errors = validate_file_paths(content, PROJECT_ROOT)
        assert len(errors) == 1
        assert "does_not_exist" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: test marker extraction
# ---------------------------------------------------------------------------


class TestTestMarkerValidation:
    """Unit tests for pytest marker extraction and validation."""

    def test_extracts_markers_from_decorator_syntax(self):
        """Extracts marker names from @pytest.mark.X references."""
        from scripts.validate_claude_md import extract_test_markers

        content = "Use `@pytest.mark.slow` for slow tests."
        markers = extract_test_markers(content)
        assert "slow" in markers

    def test_extracts_markers_from_m_flag(self):
        """Extracts marker names from -m flag references."""
        from scripts.validate_claude_md import extract_test_markers

        content = '`-m "not slow and not network"`'
        markers = extract_test_markers(content)
        assert "slow" in markers
        assert "network" in markers

    def test_validate_markers_passes_for_valid(self):
        """No errors when all referenced markers are defined."""
        from scripts.validate_claude_md import validate_test_markers

        content = "Use `@pytest.mark.slow` and `@pytest.mark.network`."
        errors = validate_test_markers(content, PROJECT_ROOT / "pyproject.toml")
        assert errors == []

    def test_validate_markers_fails_for_undefined(self):
        """Reports error for undefined markers."""
        from scripts.validate_claude_md import validate_test_markers

        content = "Use `@pytest.mark.flaky` for flaky tests."
        errors = validate_test_markers(content, PROJECT_ROOT / "pyproject.toml")
        assert len(errors) == 1
        assert "flaky" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: import validation
# ---------------------------------------------------------------------------


class TestImportValidation:
    """Unit tests for import example validation."""

    def test_extracts_imports(self):
        """Extracts import statements from backticked code blocks."""
        from scripts.validate_claude_md import extract_imports

        content = "```python\nfrom geoparquet_io.core.common import get_duckdb_connection\n```\n"
        imports = extract_imports(content)
        assert len(imports) >= 1
        assert any(
            imp[0] == "geoparquet_io.core.common" and "get_duckdb_connection" in imp[1]
            for imp in imports
        )

    def test_validate_imports_passes_for_valid(self):
        """No errors when imports reference real modules and names."""
        from scripts.validate_claude_md import validate_imports

        content = "```python\nfrom geoparquet_io.core.common import get_duckdb_connection\n```\n"
        errors = validate_imports(content, PROJECT_ROOT)
        assert errors == []

    def test_validate_imports_detects_missing_module(self):
        """Reports error when module does not exist."""
        from scripts.validate_claude_md import validate_imports

        content = "```python\nfrom geoparquet_io.core.nonexistent_module import foo\n```\n"
        errors = validate_imports(content, PROJECT_ROOT)
        assert len(errors) >= 1
        assert "nonexistent_module" in errors[0]

    def test_validate_imports_detects_missing_name(self):
        """Reports error when imported name does not exist in the module."""
        from scripts.validate_claude_md import validate_imports

        content = "```python\nfrom geoparquet_io.core.common import nonexistent_function\n```\n"
        errors = validate_imports(content, PROJECT_ROOT)
        assert len(errors) >= 1
        assert "nonexistent_function" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: required sections
# ---------------------------------------------------------------------------


class TestRequiredSections:
    """Unit tests for required section validation."""

    def test_passes_with_all_sections(self):
        """No errors when all required sections are present."""
        from scripts.validate_claude_md import validate_required_sections

        content = (
            "## Project Overview\n"
            "Some text.\n"
            "## Architecture & Key Files\n"
            "More text.\n"
            "## Testing with uv\n"
            "Test info.\n"
            "## Git Workflow\n"
            "Git info.\n"
        )
        errors = validate_required_sections(content)
        assert errors == []

    def test_reports_missing_section(self):
        """Reports error for missing required sections."""
        from scripts.validate_claude_md import validate_required_sections

        content = "## Some Other Section\nContent.\n"
        errors = validate_required_sections(content)
        assert len(errors) >= 1

    def test_matches_partial_section_names(self):
        """Section matching is substring-based (e.g. 'Architecture' matches '## Architecture & Key Files')."""
        from scripts.validate_claude_md import validate_required_sections

        content = (
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
        )
        errors = validate_required_sections(content)
        assert errors == []
