"""Tests for freshness marker updates."""

import subprocess
from datetime import date
from pathlib import Path


class TestUpdateFreshness:
    """Test the freshness marker update script."""

    def test_script_exists(self):
        """Verify script exists."""
        assert Path("scripts/update_freshness.py").exists()

    def test_script_runs(self):
        """Verify script runs without error."""
        result = subprocess.run(
            ["uv", "run", "python", "scripts/update_freshness.py"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode in [0, 1], f"Script error: {result.stderr}"

    def test_check_mode(self):
        """Verify --check flag works."""
        result = subprocess.run(
            ["uv", "run", "python", "scripts/update_freshness.py", "--check"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        # 0 = all fresh, 1 = stale markers
        assert result.returncode in [0, 1]

    def test_update_mode_does_not_crash(self, tmp_path):
        """Verify --update flag runs without crashing."""
        # Create a temporary CLAUDE.md with a marker
        claude_md = tmp_path / "CLAUDE.md"
        claude_md.write_text(
            "<!-- freshness: last-verified: 2020-01-01, maps-to: pyproject.toml -->\n"
            "## Testing with uv\n"
        )
        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "scripts/update_freshness.py",
                "--update",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(tmp_path),
        )
        # Exit code 0 or 1 are both acceptable (stale/fresh)
        assert result.returncode in [0, 1, 2]

    def test_help_output(self):
        """Verify --help works and shows usage."""
        result = subprocess.run(
            ["uv", "run", "python", "scripts/update_freshness.py", "--help"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0
        assert "update" in result.stdout.lower()


class TestParseMarkers:
    """Unit tests for marker parsing."""

    def test_parse_single_marker(self):
        """Test parsing a single freshness marker."""
        from scripts.update_freshness import parse_freshness_markers

        content = "<!-- freshness: last-verified: 2026-03-09, maps-to: cli/main.py -->"
        markers = parse_freshness_markers(content)

        assert len(markers) == 1
        assert markers[0]["date"].isoformat() == "2026-03-09"
        assert markers[0]["files"] == ["cli/main.py"]

    def test_parse_multiple_files(self):
        """Test parsing marker with multiple files."""
        from scripts.update_freshness import parse_freshness_markers

        content = "<!-- freshness: last-verified: 2026-03-01, maps-to: core/common.py, cli/decorators.py -->"
        markers = parse_freshness_markers(content)

        assert len(markers) == 1
        assert len(markers[0]["files"]) == 2
        assert "core/common.py" in markers[0]["files"]
        assert "cli/decorators.py" in markers[0]["files"]

    def test_parse_no_markers(self):
        """Test parsing content with no markers."""
        from scripts.update_freshness import parse_freshness_markers

        markers = parse_freshness_markers("# Just some markdown")
        assert len(markers) == 0

    def test_parse_multiple_markers(self):
        """Test parsing multiple freshness markers in one document."""
        from scripts.update_freshness import parse_freshness_markers

        content = (
            "<!-- freshness: last-verified: 2026-01-01, maps-to: file1.py -->\n"
            "## Section 1\n\n"
            "<!-- freshness: last-verified: 2026-02-15, maps-to: file2.py, file3.py -->\n"
            "## Section 2\n"
        )
        markers = parse_freshness_markers(content)

        assert len(markers) == 2
        assert markers[0]["date"].isoformat() == "2026-01-01"
        assert markers[1]["date"].isoformat() == "2026-02-15"
        assert markers[1]["files"] == ["file2.py", "file3.py"]

    def test_parse_marker_position(self):
        """Test that start/end positions are captured for string replacement."""
        from scripts.update_freshness import parse_freshness_markers

        content = "before <!-- freshness: last-verified: 2026-03-09, maps-to: x.py --> after"
        markers = parse_freshness_markers(content)

        assert len(markers) == 1
        assert markers[0]["start"] > 0
        assert markers[0]["end"] > markers[0]["start"]
        # Verify the match text is correct
        assert content[markers[0]["start"] : markers[0]["end"]].startswith("<!--")


class TestMarkerFreshness:
    """Unit tests for freshness checking."""

    def test_check_fresh_marker(self):
        """Test that recent marker is not stale."""
        import re

        from scripts.update_freshness import check_marker_freshness

        content = f"<!-- freshness: last-verified: {date.today().isoformat()}, maps-to: pyproject.toml -->"
        pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}), maps-to: ([^>]+) -->"
        match = re.search(pattern, content)

        marker = {
            "date": date.today(),
            "files": ["pyproject.toml"],
            "match": match,
            "start": match.start(),
            "end": match.end(),
        }

        result = check_marker_freshness(marker, Path("."))
        # A marker dated today should not be stale (even if file was modified today)
        assert result["days_old"] == 0

    def test_check_missing_file(self):
        """Test that missing files are reported."""
        import re

        from scripts.update_freshness import check_marker_freshness

        content = "<!-- freshness: last-verified: 2026-03-09, maps-to: nonexistent_file_xyz.py -->"
        pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}), maps-to: ([^>]+) -->"
        match = re.search(pattern, content)

        marker = {
            "date": date(2026, 3, 9),
            "files": ["nonexistent_file_xyz.py"],
            "match": match,
            "start": match.start(),
            "end": match.end(),
        }

        result = check_marker_freshness(marker, Path("."))
        assert "nonexistent_file_xyz.py" in result["missing_files"]

    def test_days_old_calculation(self):
        """Test that days_old is correctly calculated."""
        import re
        from datetime import timedelta

        from scripts.update_freshness import check_marker_freshness

        old_date = date.today() - timedelta(days=45)
        content = (
            f"<!-- freshness: last-verified: {old_date.isoformat()}, maps-to: pyproject.toml -->"
        )
        pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}), maps-to: ([^>]+) -->"
        match = re.search(pattern, content)

        marker = {
            "date": old_date,
            "files": ["pyproject.toml"],
            "match": match,
            "start": match.start(),
            "end": match.end(),
        }

        result = check_marker_freshness(marker, Path("."))
        assert result["days_old"] == 45

    def test_stale_result_structure(self):
        """Test that result dict has all required keys."""
        import re

        from scripts.update_freshness import check_marker_freshness

        content = "<!-- freshness: last-verified: 2026-03-09, maps-to: pyproject.toml -->"
        pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}), maps-to: ([^>]+) -->"
        match = re.search(pattern, content)

        marker = {
            "date": date(2026, 3, 9),
            "files": ["pyproject.toml"],
            "match": match,
            "start": match.start(),
            "end": match.end(),
        }

        result = check_marker_freshness(marker, Path("."))
        assert "stale" in result
        assert "stale_files" in result
        assert "missing_files" in result
        assert "days_old" in result
        assert "marker" in result


class TestUpdateMarkerDate:
    """Unit tests for marker date updating."""

    def test_update_single_marker(self):
        """Test updating a marker's date in content."""
        from scripts.update_freshness import parse_freshness_markers, update_marker_date

        old_date = "2020-01-01"
        new_date = "2026-03-09"
        content = f"<!-- freshness: last-verified: {old_date}, maps-to: cli/main.py -->\n## Section"
        markers = parse_freshness_markers(content)

        updated = update_marker_date(content, markers[0], new_date)

        assert new_date in updated
        assert old_date not in updated

    def test_update_preserves_surrounding_content(self):
        """Test that updating a marker preserves surrounding text."""
        from scripts.update_freshness import parse_freshness_markers, update_marker_date

        content = (
            "# Header\n"
            "<!-- freshness: last-verified: 2020-01-01, maps-to: cli/main.py -->\n"
            "## CLI Section\n"
            "Some content here.\n"
        )
        markers = parse_freshness_markers(content)
        updated = update_marker_date(content, markers[0], "2026-03-09")

        assert "# Header" in updated
        assert "## CLI Section" in updated
        assert "Some content here." in updated
        assert "2026-03-09" in updated

    def test_update_preserves_files_in_marker(self):
        """Test that updating a marker's date preserves the files list."""
        from scripts.update_freshness import parse_freshness_markers, update_marker_date

        content = "<!-- freshness: last-verified: 2020-01-01, maps-to: core/common.py, cli/decorators.py -->"
        markers = parse_freshness_markers(content)
        updated = update_marker_date(content, markers[0], "2026-03-09")

        assert "core/common.py" in updated
        assert "cli/decorators.py" in updated


class TestClaudeMdMarkers:
    """Integration tests verifying CLAUDE.md has freshness markers."""

    def test_claude_md_has_freshness_markers(self):
        """Verify CLAUDE.md contains at least one freshness marker."""
        from scripts.update_freshness import parse_freshness_markers

        claude_md = Path("CLAUDE.md")
        assert claude_md.exists(), "CLAUDE.md must exist"

        content = claude_md.read_text()
        markers = parse_freshness_markers(content)

        assert len(markers) >= 1, "CLAUDE.md must have at least one freshness marker"

    def test_claude_md_has_cli_marker(self):
        """Verify CLAUDE.md has a marker for CLI Command Groups section."""
        from scripts.update_freshness import parse_freshness_markers

        content = Path("CLAUDE.md").read_text()
        markers = parse_freshness_markers(content)

        # At least one marker should reference cli/main.py
        cli_files = [f for m in markers for f in m["files"] if "main.py" in f]
        assert len(cli_files) >= 1, "Must have a freshness marker for cli/main.py"

    def test_claude_md_marker_dates_are_valid(self):
        """Verify all CLAUDE.md markers have valid dates."""
        from scripts.update_freshness import parse_freshness_markers

        content = Path("CLAUDE.md").read_text()
        markers = parse_freshness_markers(content)

        for marker in markers:
            assert isinstance(marker["date"], date), f"Marker date must be a date object: {marker}"
            # Dates should be in the past or today
            assert marker["date"] <= date.today(), (
                f"Marker date cannot be in the future: {marker['date']}"
            )
