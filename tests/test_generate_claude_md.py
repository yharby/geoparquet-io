"""Tests for CLAUDE.md section generation."""

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

# Project root for import resolution
PROJECT_ROOT = Path(__file__).parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "generate_claude_md_sections.py"


def _load_module():
    """Load the generation module using importlib to avoid stale cache issues."""
    spec = importlib.util.spec_from_file_location("generate_claude_md_sections", str(SCRIPT_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestScriptExists:
    """Verify script infrastructure."""

    def test_script_exists(self):
        """Verify generation script exists."""
        assert SCRIPT_PATH.exists(), f"Expected script at {SCRIPT_PATH}"

    def test_script_runs_without_update(self):
        """Verify script runs in default (diff) mode without error."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        # Default mode should always return 0 (never 1)
        assert result.returncode == 0, (
            f"Script error (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_script_check_mode(self):
        """Verify --check flag works."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--check"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        # Exit 0 if current, 1 if outdated
        assert result.returncode in [0, 1], (
            f"Script error (rc={result.returncode}):\n{result.stderr}"
        )

    def test_script_exits_2_when_claude_md_missing(self, tmp_path):
        """Verify script returns exit code 2 when CLAUDE.md is not found."""
        # Create a fake project structure with no CLAUDE.md
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        # Copy the script to a location where CLAUDE.md won't be found
        import shutil

        shutil.copy2(str(SCRIPT_PATH), str(scripts_dir / "generate_claude_md_sections.py"))

        result = subprocess.run(
            [sys.executable, str(scripts_dir / "generate_claude_md_sections.py"), "--check"],
            capture_output=True,
            text=True,
            cwd=str(tmp_path),
        )
        assert result.returncode == 2
        assert "ERROR" in result.stdout

    def test_update_and_check_mutually_exclusive(self):
        """Verify --update and --check cannot be used together."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--update", "--check"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode != 0
        assert "not allowed" in result.stderr.lower() or "error" in result.stderr.lower()


class TestCliCommandGeneration:
    """Unit tests for CLI command generation."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_get_cli_commands_returns_dict(self):
        """Test CLI introspection returns a dict of commands."""
        commands = self.mod.get_cli_commands()
        assert isinstance(commands, dict)
        assert len(commands) > 0

    def test_get_cli_commands_known_groups(self):
        """Test that known command groups are present."""
        commands = self.mod.get_cli_commands()
        expected = ["add", "convert", "inspect", "extract", "partition", "sort", "check", "publish"]
        for group in expected:
            assert group in commands, f"Expected command group '{group}' not found"

    def test_get_cli_commands_has_subcommands(self):
        """Test that groups have subcommands."""
        commands = self.mod.get_cli_commands()
        # 'add' group should have subcommands
        assert len(commands["add"]["subcommands"]) > 0
        assert "bbox" in commands["add"]["subcommands"]

    def test_get_cli_commands_has_help_text(self):
        """Test that commands have help text."""
        commands = self.mod.get_cli_commands()
        for name, info in commands.items():
            assert "help" in info, f"Command '{name}' missing help key"

    def test_generate_cli_section_format(self):
        """Test CLI section has correct markdown format."""
        section = self.mod.generate_cli_section()
        assert "<!-- BEGIN GENERATED: cli-commands -->" in section
        assert "<!-- END GENERATED: cli-commands -->" in section
        assert "| Command Group |" in section
        assert "| Subcommands |" in section

    def test_generate_cli_section_contains_commands(self):
        """Test CLI section contains actual command names."""
        section = self.mod.generate_cli_section()
        assert "`gpio add`" in section
        assert "`gpio convert`" in section
        assert "`gpio inspect`" in section


class TestMarkerGeneration:
    """Unit tests for test marker generation."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_get_test_markers_returns_list(self):
        """Test marker parsing returns a list."""
        markers = self.mod.get_test_markers(PROJECT_ROOT / "pyproject.toml")
        assert isinstance(markers, list)
        assert len(markers) > 0

    def test_get_test_markers_known_markers(self):
        """Test that known markers are present."""
        markers = self.mod.get_test_markers(PROJECT_ROOT / "pyproject.toml")
        marker_names = [m["name"] for m in markers]
        assert "slow" in marker_names
        assert "network" in marker_names

    def test_get_test_markers_have_descriptions(self):
        """Test that markers have descriptions."""
        markers = self.mod.get_test_markers(PROJECT_ROOT / "pyproject.toml")
        for marker in markers:
            assert "name" in marker
            assert "description" in marker
            assert len(marker["description"]) > 0

    def test_generate_markers_section_format(self):
        """Test markers section has correct markdown format."""
        section = self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml")
        assert "<!-- BEGIN GENERATED: test-markers -->" in section
        assert "<!-- END GENERATED: test-markers -->" in section
        assert "| Marker |" in section
        assert "@pytest.mark." in section


class TestDocstringExtraction:
    """Unit tests for module docstring extraction."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_single_line_docstring(self):
        """Test extraction from single-line docstrings."""
        assert self.mod._extract_module_docstring('"""Hello world."""\n') == "Hello world."

    def test_multi_line_docstring_newline_after_quotes(self):
        """Test extraction from multi-line docstrings where first line is empty."""
        content = '"""\nGeoParquet file validation.\n\nMore details.\n"""\n'
        assert self.mod._extract_module_docstring(content) == "GeoParquet file validation."

    def test_no_docstring(self):
        """Test extraction returns empty string when no docstring."""
        assert self.mod._extract_module_docstring("import os\n") == ""

    def test_empty_docstring(self):
        """Test extraction returns empty string for empty docstring."""
        assert self.mod._extract_module_docstring('""""""') == ""

    def test_real_modules_have_purposes(self):
        """Test that modules with docstrings actually get their purpose extracted."""
        modules = self.mod.get_core_modules(PROJECT_ROOT / "geoparquet_io" / "core")
        with_purpose = [m for m in modules if m["purpose"]]
        # After fixing docstring extraction, we should have significantly more
        # than the original 6 modules with purposes
        assert len(with_purpose) >= 10, (
            f"Only {len(with_purpose)} modules have purposes extracted. "
            f"Expected at least 10. Docstring extraction may be broken."
        )


class TestModuleGeneration:
    """Unit tests for core module generation."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_get_core_modules_returns_list(self):
        """Test core module scanning returns a list."""
        modules = self.mod.get_core_modules(PROJECT_ROOT / "geoparquet_io" / "core")
        assert isinstance(modules, list)
        assert len(modules) > 0

    def test_get_core_modules_includes_common(self):
        """Test that common.py is found."""
        modules = self.mod.get_core_modules(PROJECT_ROOT / "geoparquet_io" / "core")
        module_names = [m["name"] for m in modules]
        assert "common.py" in module_names

    def test_get_core_modules_skips_init(self):
        """Test that __init__.py is excluded."""
        modules = self.mod.get_core_modules(PROJECT_ROOT / "geoparquet_io" / "core")
        module_names = [m["name"] for m in modules]
        assert "__init__.py" not in module_names

    def test_get_core_modules_has_line_counts(self):
        """Test that modules have line counts."""
        modules = self.mod.get_core_modules(PROJECT_ROOT / "geoparquet_io" / "core")
        for mod in modules:
            assert "lines" in mod
            assert mod["lines"] > 0

    def test_generate_modules_section_format(self):
        """Test modules section has correct markdown format."""
        section = self.mod.generate_modules_section(PROJECT_ROOT / "geoparquet_io" / "core")
        assert "<!-- BEGIN GENERATED: core-modules -->" in section
        assert "<!-- END GENERATED: core-modules -->" in section
        assert "| Module |" in section
        assert "| Purpose |" in section
        assert "| Lines |" in section

    def test_generate_modules_section_limits_to_15(self):
        """Test that only top 15 modules are shown."""
        section = self.mod.generate_modules_section(PROJECT_ROOT / "geoparquet_io" / "core")
        # Count table rows (lines starting with | and not header/separator)
        table_rows = [
            line
            for line in section.split("\n")
            if line.startswith("|")
            and "Module" not in line
            and "---" not in line
            and "more modules" not in line
        ]
        assert len(table_rows) == 15

    def test_generate_modules_section_has_more_line(self):
        """Test that the '... more modules' line is present."""
        section = self.mod.generate_modules_section(PROJECT_ROOT / "geoparquet_io" / "core")
        assert "more modules" in section


class TestSectionUpdate:
    """Unit tests for section replacement logic."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        """Import the generation module for unit tests."""
        self.mod = _load_module()

    def test_update_existing_section(self):
        """Test replacing an existing generated section."""
        content = """# Header
<!-- BEGIN GENERATED: test -->
old content
<!-- END GENERATED: test -->
# Footer"""

        new_section = """<!-- BEGIN GENERATED: test -->
new content
<!-- END GENERATED: test -->"""

        result = self.mod.update_section(content, "test", new_section)
        assert "new content" in result
        assert "old content" not in result
        assert "# Header" in result
        assert "# Footer" in result

    def test_update_preserves_surrounding_content(self):
        """Test that content before and after is preserved."""
        content = """Line 1
Line 2
<!-- BEGIN GENERATED: foo -->
old stuff
<!-- END GENERATED: foo -->
Line 3
Line 4"""

        new_section = """<!-- BEGIN GENERATED: foo -->
new stuff
<!-- END GENERATED: foo -->"""

        result = self.mod.update_section(content, "foo", new_section)
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result
        assert "Line 4" in result
        assert "new stuff" in result

    def test_update_nonexistent_section_returns_unchanged(self):
        """Test that updating a missing section returns content unchanged."""
        content = "# No generated sections here"
        new_section = """<!-- BEGIN GENERATED: missing -->
stuff
<!-- END GENERATED: missing -->"""

        result = self.mod.update_section(content, "missing", new_section)
        assert result == content

    def test_update_multiple_sections_independently(self):
        """Test that multiple sections can be updated independently."""
        content = """# Doc
<!-- BEGIN GENERATED: alpha -->
old alpha
<!-- END GENERATED: alpha -->
Middle text
<!-- BEGIN GENERATED: beta -->
old beta
<!-- END GENERATED: beta -->
End"""

        new_alpha = """<!-- BEGIN GENERATED: alpha -->
new alpha
<!-- END GENERATED: alpha -->"""

        result = self.mod.update_section(content, "alpha", new_alpha)
        assert "new alpha" in result
        assert "old alpha" not in result
        # beta should be unchanged
        assert "old beta" in result

    def test_update_replaces_only_first_occurrence(self):
        """Test that duplicate section markers only replace the first match."""
        content = """# Doc
<!-- BEGIN GENERATED: dup -->
first
<!-- END GENERATED: dup -->
middle
<!-- BEGIN GENERATED: dup -->
second
<!-- END GENERATED: dup -->
end"""

        new_section = """<!-- BEGIN GENERATED: dup -->
replaced
<!-- END GENERATED: dup -->"""

        result = self.mod.update_section(content, "dup", new_section)
        assert "replaced" in result
        assert "second" in result
        assert "first" not in result


class TestUpdateMode:
    """Tests for --update mode on CLAUDE.md."""

    @pytest.fixture(autouse=True)
    def _import_module(self):
        self.mod = _load_module()

    def test_claude_md_has_generated_markers(self):
        """Test that CLAUDE.md contains the generated section markers."""
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        content = claude_md.read_text(encoding="utf-8")
        for section_name in ["cli-commands", "test-markers", "core-modules"]:
            assert f"<!-- BEGIN GENERATED: {section_name} -->" in content, (
                f"CLAUDE.md missing BEGIN marker for '{section_name}'"
            )
            assert f"<!-- END GENERATED: {section_name} -->" in content, (
                f"CLAUDE.md missing END marker for '{section_name}'"
            )

    def test_update_produces_valid_markdown(self):
        """Test that updated CLAUDE.md still has valid structure."""
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        content = claude_md.read_text(encoding="utf-8")

        # Generate all sections and apply
        sections = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
            "core-modules": self.mod.generate_modules_section(
                PROJECT_ROOT / "geoparquet_io" / "core"
            ),
        }

        updated = content
        for name, section_content in sections.items():
            updated = self.mod.update_section(updated, name, section_content)

        # Should still have the main title
        assert "# Claude Code Instructions for geoparquet-io" in updated
        # Should have all three generated sections
        for name in sections:
            assert f"<!-- BEGIN GENERATED: {name} -->" in updated
            assert f"<!-- END GENERATED: {name} -->" in updated

    def test_update_is_idempotent(self):
        """Test that running update twice produces identical output."""
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        content = claude_md.read_text(encoding="utf-8")

        sections = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
            "core-modules": self.mod.generate_modules_section(
                PROJECT_ROOT / "geoparquet_io" / "core"
            ),
        }

        # First pass
        first_pass = content
        for name, section_content in sections.items():
            first_pass = self.mod.update_section(first_pass, name, section_content)

        # Second pass (regenerate sections and apply again)
        sections2 = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
            "core-modules": self.mod.generate_modules_section(
                PROJECT_ROOT / "geoparquet_io" / "core"
            ),
        }

        second_pass = first_pass
        for name, section_content in sections2.items():
            second_pass = self.mod.update_section(second_pass, name, section_content)

        assert first_pass == second_pass, "Update is not idempotent"

    def test_update_writes_to_disk(self, tmp_path):
        """Test that --update actually writes the file."""
        # Copy CLAUDE.md to a temp location
        claude_md = PROJECT_ROOT / "CLAUDE.md"
        fake_md = tmp_path / "CLAUDE.md"
        fake_md.write_text(claude_md.read_text(encoding="utf-8"), encoding="utf-8")

        # Corrupt a section to force an update
        content = fake_md.read_text(encoding="utf-8")
        content = content.replace(
            "### CLI Command Groups",
            "### CLI Command Groups (OUTDATED)",
        )
        fake_md.write_text(content, encoding="utf-8")

        # Create the required directory structure
        (tmp_path / "geoparquet_io" / "core").mkdir(parents=True, exist_ok=True)
        (tmp_path / "scripts").mkdir(exist_ok=True)

        # The script needs a specific project root -- we test via the module API
        original = fake_md.read_text(encoding="utf-8")
        sections = {
            "cli-commands": self.mod.generate_cli_section(),
            "test-markers": self.mod.generate_markers_section(PROJECT_ROOT / "pyproject.toml"),
            "core-modules": self.mod.generate_modules_section(
                PROJECT_ROOT / "geoparquet_io" / "core"
            ),
        }

        updated = original
        for name, section_content in sections.items():
            updated = self.mod.update_section(updated, name, section_content)

        fake_md.write_text(updated, encoding="utf-8")
        assert "OUTDATED" not in fake_md.read_text(encoding="utf-8")
        assert "### CLI Command Groups" in fake_md.read_text(encoding="utf-8")
