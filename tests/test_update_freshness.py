"""Tests for freshness marker updates."""

from __future__ import annotations

import re
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pytest

# Resolve project root once so tests are not cwd-dependent.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_root():
    """Return the absolute project root regardless of working directory."""
    return PROJECT_ROOT


@pytest.fixture()
def _make_marker():
    """Factory fixture to build a marker dict from a content string."""

    def _build(content: str):
        pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}), maps-to: ([^>]+) -->"
        match = re.search(pattern, content)
        assert match, f"No marker found in: {content}"
        from scripts.update_freshness import parse_freshness_markers

        return parse_freshness_markers(content)[0]

    return _build


# ---------------------------------------------------------------------------
# TestUpdateFreshness (integration / subprocess tests)
# ---------------------------------------------------------------------------


class TestUpdateFreshness:
    """Integration tests that run the script as a subprocess."""

    def test_script_exists(self, project_root):
        """Verify script exists."""
        assert (project_root / "scripts" / "update_freshness.py").exists()

    def test_script_runs(self, project_root):
        """Verify script runs without error."""
        result = subprocess.run(
            ["uv", "run", "python", "scripts/update_freshness.py"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(project_root),
        )
        assert result.returncode in (0, 1), f"Script error: {result.stderr}"

    def test_check_mode(self, project_root):
        """Verify --check flag works."""
        result = subprocess.run(
            ["uv", "run", "python", "scripts/update_freshness.py", "--check"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(project_root),
        )
        # 0 = all fresh, 1 = stale markers
        assert result.returncode in (0, 1)

    def test_help_output(self, project_root):
        """Verify --help works and shows usage."""
        result = subprocess.run(
            ["uv", "run", "python", "scripts/update_freshness.py", "--help"],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(project_root),
        )
        assert result.returncode == 0
        assert "update" in result.stdout.lower()


# ---------------------------------------------------------------------------
# TestParseMarkers
# ---------------------------------------------------------------------------


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

    def test_parse_ignores_malformed_markers(self):
        """Markers with bad dates or missing fields are skipped."""
        from scripts.update_freshness import parse_freshness_markers

        bad = "<!-- freshness: last-verified: not-a-date, maps-to: x.py -->"
        markers = parse_freshness_markers(bad)
        assert len(markers) == 0

    def test_parse_whitespace_around_files(self):
        """Whitespace around file names in maps-to is stripped."""
        from scripts.update_freshness import parse_freshness_markers

        content = "<!-- freshness: last-verified: 2026-01-01, maps-to:  a.py ,  b.py  -->"
        markers = parse_freshness_markers(content)
        assert markers[0]["files"] == ["a.py", "b.py"]


# ---------------------------------------------------------------------------
# TestGetFileLastModified
# ---------------------------------------------------------------------------


class TestGetFileLastModified:
    """Unit tests for get_file_last_modified."""

    def test_returns_none_for_nonexistent_file(self, project_root):
        """Missing file returns None without raising."""
        from scripts.update_freshness import get_file_last_modified

        result = get_file_last_modified(Path("totally_nonexistent_xyz.py"), project_root)
        assert result is None

    def test_returns_datetime_for_tracked_file(self, project_root):
        """A git-tracked file returns a datetime."""
        from scripts.update_freshness import get_file_last_modified

        result = get_file_last_modified(Path("pyproject.toml"), project_root)
        assert result is not None
        assert hasattr(result, "date")

    def test_handles_absolute_path(self, project_root):
        """Absolute paths are used as-is, not joined again."""
        from scripts.update_freshness import get_file_last_modified

        abs_path = project_root / "pyproject.toml"
        result = get_file_last_modified(abs_path, project_root)
        assert result is not None

    def test_handles_subprocess_error(self, project_root, tmp_path):
        """SubprocessError is caught gracefully."""
        from scripts.update_freshness import get_file_last_modified

        # Create a file in tmp_path that exists but is not in a git repo
        dummy = tmp_path / "dummy.py"
        dummy.write_text("x = 1\n")

        result = get_file_last_modified(dummy, tmp_path)
        # Either None (git error) or a valid datetime are acceptable
        # The key thing is no exception is raised
        assert result is None or hasattr(result, "date")


# ---------------------------------------------------------------------------
# TestMarkerFreshness
# ---------------------------------------------------------------------------


class TestMarkerFreshness:
    """Unit tests for freshness checking."""

    def test_check_fresh_marker(self, project_root, _make_marker):
        """Test that a marker dated today is not stale."""
        from scripts.update_freshness import check_marker_freshness

        today = date.today().isoformat()
        content = f"<!-- freshness: last-verified: {today}, maps-to: pyproject.toml -->"
        marker = _make_marker(content)

        result = check_marker_freshness(marker, project_root)
        assert result["days_old"] == 0

    def test_check_missing_file(self, project_root, _make_marker):
        """Test that missing files are reported."""
        from scripts.update_freshness import check_marker_freshness

        content = "<!-- freshness: last-verified: 2026-03-09, maps-to: nonexistent_file_xyz.py -->"
        marker = _make_marker(content)

        result = check_marker_freshness(marker, project_root)
        assert "nonexistent_file_xyz.py" in result["missing_files"]

    def test_days_old_calculation(self, project_root, _make_marker):
        """Test that days_old is correctly calculated."""
        from scripts.update_freshness import check_marker_freshness

        old_date = date.today() - timedelta(days=45)
        content = (
            f"<!-- freshness: last-verified: {old_date.isoformat()}, maps-to: pyproject.toml -->"
        )
        marker = _make_marker(content)

        result = check_marker_freshness(marker, project_root)
        assert result["days_old"] == 45

    def test_stale_result_structure(self, project_root, _make_marker):
        """Test that result dict has all required keys."""
        from scripts.update_freshness import check_marker_freshness

        content = "<!-- freshness: last-verified: 2026-03-09, maps-to: pyproject.toml -->"
        marker = _make_marker(content)

        result = check_marker_freshness(marker, project_root)
        assert "stale" in result
        assert "stale_files" in result
        assert "missing_files" in result
        assert "days_old" in result
        assert "marker" in result

    def test_stale_marker_detected(self, project_root, _make_marker):
        """A marker with a very old date for a tracked file should be stale."""
        from scripts.update_freshness import check_marker_freshness

        content = "<!-- freshness: last-verified: 2020-01-01, maps-to: pyproject.toml -->"
        marker = _make_marker(content)

        result = check_marker_freshness(marker, project_root)
        assert result["stale"] is True
        assert "pyproject.toml" in result["stale_files"]

    def test_geoparquet_io_fallback_path(self, project_root, _make_marker):
        """Files referenced without geoparquet_io/ prefix are found via fallback."""
        from scripts.update_freshness import check_marker_freshness

        # cli/main.py doesn't exist at root, but geoparquet_io/cli/main.py does
        content = "<!-- freshness: last-verified: 2020-01-01, maps-to: cli/main.py -->"
        marker = _make_marker(content)

        result = check_marker_freshness(marker, project_root)
        # Should NOT be in missing_files because fallback to geoparquet_io/ works
        assert "cli/main.py" not in result["missing_files"]


# ---------------------------------------------------------------------------
# TestUpdateMarkerDate
# ---------------------------------------------------------------------------


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

    def test_update_multiple_markers_reverse(self):
        """Updating multiple markers in reverse order preserves positions."""
        from scripts.update_freshness import parse_freshness_markers, update_marker_date

        content = (
            "<!-- freshness: last-verified: 2020-01-01, maps-to: a.py -->\n"
            "## Section A\n"
            "<!-- freshness: last-verified: 2020-06-15, maps-to: b.py -->\n"
            "## Section B\n"
        )
        markers = parse_freshness_markers(content)
        assert len(markers) == 2

        new_date = "2026-03-09"
        updated = content
        for marker in reversed(markers):
            updated = update_marker_date(updated, marker, new_date)

        assert "2020-01-01" not in updated
        assert "2020-06-15" not in updated
        assert updated.count(new_date) == 2
        assert "## Section A" in updated
        assert "## Section B" in updated


# ---------------------------------------------------------------------------
# TestMain (unit tests for main() via monkeypatch)
# ---------------------------------------------------------------------------


class TestMain:
    """Unit tests for the main() entry point."""

    def test_missing_claude_md_returns_2(self, tmp_path, monkeypatch):
        """Script returns 2 when CLAUDE.md is missing."""
        from scripts import update_freshness

        # Point project_root to an empty tmp dir by patching __file__
        fake_script = tmp_path / "scripts" / "update_freshness.py"
        fake_script.parent.mkdir(parents=True)
        fake_script.write_text("")

        monkeypatch.setattr(update_freshness, "__file__", str(fake_script))
        monkeypatch.setattr("sys.argv", ["update_freshness.py"])

        assert update_freshness.main() == 2

    def test_no_markers_returns_0(self, tmp_path, monkeypatch):
        """Script returns 0 when CLAUDE.md has no markers."""
        from scripts import update_freshness

        fake_script = tmp_path / "scripts" / "update_freshness.py"
        fake_script.parent.mkdir(parents=True)
        fake_script.write_text("")

        claude_md = tmp_path / "CLAUDE.md"
        claude_md.write_text("# Just a heading, no markers\n")

        monkeypatch.setattr(update_freshness, "__file__", str(fake_script))
        monkeypatch.setattr("sys.argv", ["update_freshness.py"])

        assert update_freshness.main() == 0

    def test_check_returns_1_when_stale(self, tmp_path, monkeypatch):
        """--check returns 1 when markers are stale."""
        from scripts import update_freshness

        fake_script = tmp_path / "scripts" / "update_freshness.py"
        fake_script.parent.mkdir(parents=True)
        fake_script.write_text("")

        claude_md = tmp_path / "CLAUDE.md"
        claude_md.write_text(
            "<!-- freshness: last-verified: 2020-01-01, maps-to: dummy.py -->\n## Section\n"
        )
        # Create the dummy file so it's not "missing"
        (tmp_path / "dummy.py").write_text("x = 1\n")

        monkeypatch.setattr(update_freshness, "__file__", str(fake_script))
        monkeypatch.setattr("sys.argv", ["update_freshness.py", "--check"])

        # Mock get_file_last_modified to return a recent date
        from datetime import datetime

        monkeypatch.setattr(
            update_freshness,
            "get_file_last_modified",
            lambda *_args, **_kw: datetime(2026, 3, 1),
        )

        assert update_freshness.main() == 1

    def test_update_writes_file(self, tmp_path, monkeypatch):
        """--update rewrites CLAUDE.md with new timestamps."""
        from scripts import update_freshness

        fake_script = tmp_path / "scripts" / "update_freshness.py"
        fake_script.parent.mkdir(parents=True)
        fake_script.write_text("")

        claude_md = tmp_path / "CLAUDE.md"
        claude_md.write_text(
            "<!-- freshness: last-verified: 2020-01-01, maps-to: dummy.py -->\n## Section\n"
        )
        (tmp_path / "dummy.py").write_text("x = 1\n")

        monkeypatch.setattr(update_freshness, "__file__", str(fake_script))
        monkeypatch.setattr("sys.argv", ["update_freshness.py", "--update"])

        from datetime import datetime

        monkeypatch.setattr(
            update_freshness,
            "get_file_last_modified",
            lambda *_args, **_kw: datetime(2026, 3, 1),
        )

        update_freshness.main()

        updated = claude_md.read_text()
        assert "2020-01-01" not in updated
        # Should contain today's date
        assert datetime.now().strftime("%Y-%m-%d") in updated

    def test_warn_days_threshold(self, tmp_path, monkeypatch):
        """--warn-days triggers warning for old-but-not-stale markers."""
        from scripts import update_freshness

        fake_script = tmp_path / "scripts" / "update_freshness.py"
        fake_script.parent.mkdir(parents=True)
        fake_script.write_text("")

        old_date = (date.today() - timedelta(days=40)).isoformat()
        claude_md = tmp_path / "CLAUDE.md"
        claude_md.write_text(
            f"<!-- freshness: last-verified: {old_date}, maps-to: dummy.py -->\n## Section\n"
        )
        (tmp_path / "dummy.py").write_text("x = 1\n")

        monkeypatch.setattr(update_freshness, "__file__", str(fake_script))
        monkeypatch.setattr("sys.argv", ["update_freshness.py", "--warn-days", "30"])
        # Return None so marker is not stale, only old
        monkeypatch.setattr(
            update_freshness,
            "get_file_last_modified",
            lambda *_args, **_kw: None,
        )

        rc = update_freshness.main()
        assert rc == 0  # warnings don't cause non-zero exit


# ---------------------------------------------------------------------------
# TestClaudeMdMarkers (integration: real CLAUDE.md)
# ---------------------------------------------------------------------------


class TestClaudeMdMarkers:
    """Integration tests verifying CLAUDE.md has freshness markers."""

    def test_claude_md_has_freshness_markers(self, project_root):
        """Verify CLAUDE.md contains at least one freshness marker."""
        from scripts.update_freshness import parse_freshness_markers

        claude_md = project_root / "CLAUDE.md"
        assert claude_md.exists(), "CLAUDE.md must exist"

        content = claude_md.read_text()
        markers = parse_freshness_markers(content)

        assert len(markers) >= 1, "CLAUDE.md must have at least one freshness marker"

    def test_claude_md_has_cli_marker(self, project_root):
        """Verify CLAUDE.md has a marker for CLI Command Groups section."""
        from scripts.update_freshness import parse_freshness_markers

        content = (project_root / "CLAUDE.md").read_text()
        markers = parse_freshness_markers(content)

        # At least one marker should reference cli/main.py
        cli_files = [f for m in markers for f in m["files"] if "main.py" in f]
        assert len(cli_files) >= 1, "Must have a freshness marker for cli/main.py"

    def test_claude_md_marker_dates_are_valid(self, project_root):
        """Verify all CLAUDE.md markers have valid dates."""
        from scripts.update_freshness import parse_freshness_markers

        content = (project_root / "CLAUDE.md").read_text()
        markers = parse_freshness_markers(content)

        for marker in markers:
            assert isinstance(marker["date"], date), f"Marker date must be a date object: {marker}"
            # Dates should be in the past or today
            assert marker["date"] <= date.today(), (
                f"Marker date cannot be in the future: {marker['date']}"
            )

    def test_claude_md_marker_files_exist(self, project_root):
        """Verify all files referenced in markers actually exist."""
        from scripts.update_freshness import parse_freshness_markers

        content = (project_root / "CLAUDE.md").read_text()
        markers = parse_freshness_markers(content)

        for marker in markers:
            for file_path in marker["files"]:
                full = project_root / file_path
                assert full.exists(), (
                    f"File referenced in freshness marker does not exist: {file_path}"
                )
