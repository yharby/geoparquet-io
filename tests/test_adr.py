"""Tests for ADR directory structure and content quality."""

import re
from pathlib import Path

import pytest


@pytest.fixture
def adr_dir():
    """Return path to ADR directory, relative to repo root."""
    path = Path(__file__).parent.parent / "context" / "shared" / "adr"
    assert path.exists(), f"ADR directory not found at {path}"
    return path


@pytest.fixture
def adr_files(adr_dir):
    """Return sorted list of ADR files (numbered 0*.md)."""
    return sorted(adr_dir.glob("0*.md"))


class TestADRDirectory:
    """Test ADR directory is properly structured."""

    def test_adr_directory_exists(self, adr_dir):
        """Verify ADR directory exists and is a directory."""
        assert adr_dir.is_dir()

    def test_readme_exists(self, adr_dir):
        """Verify README exists and is non-empty."""
        readme = adr_dir / "README.md"
        assert readme.exists(), "ADR README missing"
        assert readme.stat().st_size > 0, "ADR README is empty"

    def test_template_exists(self, adr_dir):
        """Verify template exists and is non-empty."""
        template = adr_dir / "template.md"
        assert template.exists(), "ADR template missing"
        assert template.stat().st_size > 0, "ADR template is empty"

    def test_at_least_5_adrs_exist(self, adr_files):
        """Verify all 5 initial ADRs are documented."""
        assert len(adr_files) >= 5, f"Expected at least 5 ADRs, found {len(adr_files)}"


class TestADRContent:
    """Test ADR content quality and completeness."""

    def test_adrs_have_required_sections(self, adr_files):
        """Verify each ADR has all required sections from the template."""
        required_sections = [
            "## Status",
            "## Context",
            "## Decision",
            "## Consequences",
            "## Alternatives Considered",
            "## References",
        ]

        for adr_file in adr_files:
            content = adr_file.read_text()
            for section in required_sections:
                assert section in content, f"{adr_file.name} missing '{section}'"

    def test_adrs_have_consequence_subsections(self, adr_files):
        """Verify each ADR has Positive/Negative subsections under Consequences."""
        required_subsections = ["### Positive", "### Negative"]

        for adr_file in adr_files:
            content = adr_file.read_text()
            for subsection in required_subsections:
                assert subsection in content, (
                    f"{adr_file.name} missing '{subsection}' under Consequences"
                )

    def test_adrs_have_valid_status(self, adr_files):
        """Verify each ADR has a valid status value."""
        valid_statuses = {"Proposed", "Accepted", "Deprecated"}

        for adr_file in adr_files:
            content = adr_file.read_text()
            status_match = re.search(r"## Status\s*\n\s*(.+?)(?:\n\n|\n##)", content)
            assert status_match, f"{adr_file.name} has no status value after '## Status'"
            status_text = status_match.group(1).strip()
            if status_text.startswith("Superseded by"):
                continue
            assert status_text in valid_statuses, (
                f"{adr_file.name} has invalid status '{status_text}', "
                f"expected one of: {valid_statuses}"
            )

    def test_adrs_have_title_in_heading(self, adr_files):
        """Verify each ADR has a proper title heading."""
        for adr_file in adr_files:
            content = adr_file.read_text()
            first_line = content.strip().split("\n")[0]
            assert first_line.startswith("# ADR-"), (
                f"{adr_file.name} first line should start with '# ADR-', got: {first_line}"
            )

    def test_adrs_title_matches_filename_number(self, adr_files):
        """Verify ADR title number matches filename number."""
        for adr_file in adr_files:
            content = adr_file.read_text()
            first_line = content.strip().split("\n")[0]
            file_number = adr_file.name.split("-")[0]
            title_match = re.search(r"# ADR-(\d+)", first_line)
            assert title_match, f"{adr_file.name} title does not contain ADR number"
            title_number = title_match.group(1)
            assert file_number == title_number, (
                f"{adr_file.name}: filename number {file_number} != title number {title_number}"
            )


class TestADRNumbering:
    """Test ADR numbering and sequencing."""

    def test_adrs_are_numbered_sequentially(self, adr_files):
        """Verify ADRs are numbered sequentially starting from 0001."""
        for i, adr in enumerate(adr_files, start=1):
            expected_prefix = f"{i:04d}-"
            assert adr.name.startswith(expected_prefix), (
                f"Expected {adr.name} to start with {expected_prefix}"
            )


class TestADRReadmeIndex:
    """Test README index consistency with actual files."""

    def test_readme_index_links_to_all_files(self, adr_dir, adr_files):
        """Verify README index contains links to all ADR files."""
        readme = adr_dir / "README.md"
        content = readme.read_text()

        for adr_file in adr_files:
            assert adr_file.name in content, f"README missing link to {adr_file.name}"

    def test_readme_index_has_no_dead_links(self, adr_dir):
        """Verify README does not reference ADR files that do not exist."""
        readme = adr_dir / "README.md"
        content = readme.read_text()

        linked_files = re.findall(r"\((\d{4}-[^)]+\.md)\)", content)
        for linked_file in linked_files:
            assert (adr_dir / linked_file).exists(), (
                f"README links to {linked_file} which does not exist"
            )

    def test_readme_has_adr_index_table(self, adr_dir):
        """Verify README has a properly formatted ADR index table."""
        readme = adr_dir / "README.md"
        content = readme.read_text()

        assert "## ADR Index" in content, "README missing '## ADR Index' section"
        assert "| ADR |" in content, "README missing ADR index table header"


class TestADRTemplate:
    """Test template has all required sections."""

    def test_template_has_required_sections(self, adr_dir):
        """Verify template contains all sections that ADRs must have."""
        template = adr_dir / "template.md"
        content = template.read_text()

        required_sections = [
            "## Status",
            "## Context",
            "## Decision",
            "## Consequences",
            "## Alternatives Considered",
            "## References",
        ]

        for section in required_sections:
            assert section in content, f"Template missing '{section}'"
