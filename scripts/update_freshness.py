#!/usr/bin/env python3
"""Update freshness markers in CLAUDE.md when mapped files change.

Freshness markers track when documentation sections were last verified
against their source files. This script updates timestamps when the
mapped source files have been modified more recently than the marker.

Usage:
    python scripts/update_freshness.py          # Check mode (report stale)
    python scripts/update_freshness.py --update # Update timestamps
    python scripts/update_freshness.py --check  # Exit 1 if stale (for CI)
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Staleness threshold in days
STALENESS_THRESHOLD_DAYS = 30

logger = logging.getLogger(__name__)


def parse_freshness_markers(content: str) -> list[dict]:
    """Extract freshness markers from CLAUDE.md content."""
    pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}), maps-to: ([^>]+) -->"
    markers = []
    for match in re.finditer(pattern, content):
        date_str = match.group(1)
        files = [f.strip() for f in match.group(2).split(",")]
        markers.append(
            {
                "date": datetime.strptime(date_str, "%Y-%m-%d").date(),
                "files": files,
                "match": match,
                "start": match.start(),
                "end": match.end(),
            }
        )
    return markers


def get_file_last_modified(file_path: Path, project_root: Path) -> datetime | None:
    """Get the last modification date of a file from git.

    Args:
        file_path: Absolute or relative path to the file. If relative,
            it is resolved against *project_root*.
        project_root: The git repository root used as ``cwd`` for git.
    """
    resolved = file_path if file_path.is_absolute() else project_root / file_path
    if not resolved.exists():
        return None

    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ci", str(resolved)],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            # Parse git date format: "2026-03-09 10:30:00 -0500"
            date_str = result.stdout.strip().split()[0]
            return datetime.strptime(date_str, "%Y-%m-%d")
    except (subprocess.SubprocessError, OSError) as exc:
        logger.debug("Could not get git date for %s: %s", file_path, exc)
    return None


def check_marker_freshness(marker: dict, project_root: Path) -> dict:
    """Check if a marker's mapped files have been modified since last verification."""
    result = {
        "marker": marker,
        "stale": False,
        "stale_files": [],
        "missing_files": [],
        "days_old": (datetime.now().date() - marker["date"]).days,
    }

    for file_path in marker["files"]:
        # Handle paths that might be relative to geoparquet_io/
        full_path = project_root / file_path
        if not full_path.exists():
            full_path = project_root / "geoparquet_io" / file_path

        if not full_path.exists():
            result["missing_files"].append(file_path)
            continue

        last_modified = get_file_last_modified(full_path, project_root)
        if last_modified and last_modified.date() > marker["date"]:
            result["stale"] = True
            result["stale_files"].append(file_path)

    return result


def update_marker_date(content: str, marker: dict, new_date: str) -> str:
    """Update a marker's date in the content."""
    old_text = marker["match"].group(0)
    new_text = re.sub(
        r"last-verified: \d{4}-\d{2}-\d{2}",
        f"last-verified: {new_date}",
        old_text,
    )
    return content[: marker["start"]] + new_text + content[marker["end"] :]


def main() -> int:
    parser = argparse.ArgumentParser(description="Update CLAUDE.md freshness markers")
    parser.add_argument("--update", action="store_true", help="Update stale markers")
    parser.add_argument("--check", action="store_true", help="Exit 1 if any markers are stale")
    parser.add_argument(
        "--warn-days",
        type=int,
        default=STALENESS_THRESHOLD_DAYS,
        help=f"Warn if section is older than N days (default: {STALENESS_THRESHOLD_DAYS})",
    )
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    claude_md = project_root / "CLAUDE.md"

    if not claude_md.exists():
        print("ERROR: CLAUDE.md not found")
        return 2

    content = claude_md.read_text()
    markers = parse_freshness_markers(content)

    if not markers:
        print("No freshness markers found in CLAUDE.md")
        return 0

    today = datetime.now().strftime("%Y-%m-%d")
    stale_count = 0
    warning_count = 0
    updated_content = content

    # Process in reverse order to preserve string positions when updating
    for marker in reversed(markers):
        result = check_marker_freshness(marker, project_root)

        if result["missing_files"]:
            print(f"WARNING: Missing files in marker: {', '.join(result['missing_files'])}")

        if result["stale"]:
            stale_count += 1
            files_str = ", ".join(result["stale_files"])
            print(f"STALE: Section verified {marker['date']} has newer files: {files_str}")

            if args.update:
                updated_content = update_marker_date(updated_content, marker, today)
                print(f"  -> Updated to {today}")

        elif result["days_old"] > args.warn_days:
            warning_count += 1
            print(
                f"WARNING: Section not verified in {result['days_old']} days "
                f"(threshold: {args.warn_days})"
            )

    if args.update and updated_content != content:
        claude_md.write_text(updated_content)
        print(f"\nUpdated {stale_count} stale marker(s)")

    if stale_count > 0:
        print(f"\nFound {stale_count} stale marker(s)")
        if args.check:
            return 1
    elif warning_count > 0:
        print(f"\nFound {warning_count} marker(s) approaching staleness threshold")
    else:
        print("\nAll freshness markers are current!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
