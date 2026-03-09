#!/usr/bin/env python3
"""Generate CLAUDE.md sections from code introspection.

This script auto-generates documentation sections to prevent drift:
1. CLI command table from Click introspection
2. Test markers from pyproject.toml
3. Core modules index from filesystem

Usage:
    python scripts/generate_claude_md_sections.py          # Default: show diff
    python scripts/generate_claude_md_sections.py --update  # Update CLAUDE.md in place
    python scripts/generate_claude_md_sections.py --check   # Exit 1 if outdated
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import click

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]


def get_cli_commands() -> dict[str, dict]:
    """Introspect Click CLI to get all commands and their structure.

    Returns:
        Dict mapping command name to info dict with 'subcommands' and 'help' keys.
    """
    from geoparquet_io.cli.main import cli

    result = {}
    for name, cmd in sorted(cli.commands.items()):
        if isinstance(cmd, click.Group):
            subcommands = sorted(cmd.commands.keys())
            result[name] = {
                "subcommands": subcommands,
                "help": cmd.help or "",
            }
        else:
            result[name] = {
                "subcommands": [],
                "help": cmd.help or "",
            }
    return result


def generate_cli_section() -> str:
    """Generate the CLI commands markdown section."""
    commands = get_cli_commands()

    lines = [
        "<!-- BEGIN GENERATED: cli-commands -->",
        "### CLI Command Groups",
        "",
        "| Command Group | Subcommands | Description |",
        "|---------------|-------------|-------------|",
    ]

    for name, info in sorted(commands.items()):
        subcommands = ", ".join(info["subcommands"]) if info["subcommands"] else ""
        # Truncate description to first sentence
        help_text = info["help"].strip()
        desc = help_text.split(".")[0].strip() if help_text else ""
        lines.append(f"| `gpio {name}` | {subcommands} | {desc} |")

    lines.append("<!-- END GENERATED: cli-commands -->")
    return "\n".join(lines)


def get_test_markers(pyproject_path: Path) -> list[dict]:
    """Parse test markers from pyproject.toml.

    Args:
        pyproject_path: Path to pyproject.toml file.

    Returns:
        List of dicts with 'name' and 'description' keys.
    """
    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)

    markers_raw = config.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("markers", [])
    markers = []
    for marker in markers_raw:
        if ":" in marker:
            name, desc = marker.split(":", 1)
            markers.append({"name": name.strip(), "description": desc.strip()})
    return markers


def generate_markers_section(pyproject_path: Path) -> str:
    """Generate the test markers markdown section."""
    markers = get_test_markers(pyproject_path)

    lines = [
        "<!-- BEGIN GENERATED: test-markers -->",
        "### Test Markers",
        "",
        "| Marker | Description |",
        "|--------|-------------|",
    ]

    for marker in markers:
        lines.append(f"| `@pytest.mark.{marker['name']}` | {marker['description']} |")

    lines.append("<!-- END GENERATED: test-markers -->")
    return "\n".join(lines)


def _extract_module_docstring(content: str) -> str:
    """Extract the first meaningful line from a module docstring.

    Handles both single-line (``\"\"\"Desc.\"\"\"``) and multi-line
    (``\"\"\"\\nDesc.\\n\"\"\"``) docstring formats.
    """
    if not content.startswith('"""'):
        return ""
    end = content.find('"""', 3)
    if end <= 0:
        return ""
    raw = content[3:end]
    for line in raw.split("\n"):
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def get_core_modules(core_path: Path) -> list[dict]:
    """Scan core directory for Python modules.

    Args:
        core_path: Path to geoparquet_io/core/ directory.

    Returns:
        List of dicts with 'name', 'lines', and 'purpose' keys.
    """
    modules = []
    for py_file in sorted(core_path.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        content = py_file.read_text(encoding="utf-8")
        line_count = len(content.splitlines())
        docstring = _extract_module_docstring(content)
        modules.append(
            {
                "name": py_file.name,
                "lines": line_count,
                "purpose": docstring,
            }
        )
    return modules


def generate_modules_section(core_path: Path) -> str:
    """Generate the core modules markdown section."""
    modules = get_core_modules(core_path)

    lines = [
        "<!-- BEGIN GENERATED: core-modules -->",
        "### Core Modules",
        "",
        "| Module | Purpose | Lines |",
        "|--------|---------|-------|",
    ]

    # Show top 15 by line count
    modules_sorted = sorted(modules, key=lambda x: x["lines"], reverse=True)[:15]
    for mod in modules_sorted:
        purpose = mod["purpose"][:50] + "..." if len(mod["purpose"]) > 50 else mod["purpose"]
        lines.append(f"| `{mod['name']}` | {purpose} | {mod['lines']} |")

    remaining = len(modules) - 15
    if remaining > 0:
        lines.append(f"| ... | *{remaining} more modules* | |")
    lines.append("<!-- END GENERATED: core-modules -->")
    return "\n".join(lines)


def update_section(content: str, section_name: str, new_content: str) -> str:
    """Replace a generated section in the content.

    Args:
        content: Full file content.
        section_name: Name used in BEGIN/END GENERATED markers.
        new_content: New section content including markers.

    Returns:
        Updated content with section replaced, or unchanged if section not found.
    """
    pattern = (
        rf"<!-- BEGIN GENERATED: {re.escape(section_name)} -->"
        r".*?"
        rf"<!-- END GENERATED: {re.escape(section_name)} -->"
    )

    if re.search(pattern, content, re.DOTALL):
        return re.sub(pattern, new_content, content, count=1, flags=re.DOTALL)
    else:
        # Section doesn't exist yet - return unchanged
        return content


def main() -> int:
    """Main entry point.

    Returns:
        Exit code: 0 on success/up-to-date, 1 if outdated (--check), 2 on error.
    """
    parser = argparse.ArgumentParser(description="Generate CLAUDE.md sections from code")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--update", action="store_true", help="Update CLAUDE.md in place")
    group.add_argument(
        "--check", action="store_true", help="Check if sections are current (exit 1 if not)"
    )
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    claude_md = project_root / "CLAUDE.md"
    pyproject = project_root / "pyproject.toml"
    core_path = project_root / "geoparquet_io" / "core"

    if not claude_md.exists():
        print("ERROR: CLAUDE.md not found")
        return 2

    if not pyproject.exists():
        print("ERROR: pyproject.toml not found")
        return 2

    if not core_path.is_dir():
        print(f"ERROR: core directory not found at {core_path}")
        return 2

    original = claude_md.read_text(encoding="utf-8")
    updated = original

    # Generate each section
    sections = {
        "cli-commands": generate_cli_section(),
        "test-markers": generate_markers_section(pyproject),
        "core-modules": generate_modules_section(core_path),
    }

    for name, content in sections.items():
        updated = update_section(updated, name, content)

    if updated == original:
        print("CLAUDE.md sections are up to date!")
        return 0

    if args.update:
        claude_md.write_text(updated, encoding="utf-8")
        print("CLAUDE.md updated successfully!")
        return 0
    elif args.check:
        print("CLAUDE.md sections are out of date. Run with --update to fix.")
        return 1
    else:
        # Default: show what would change
        print("CLAUDE.md sections would change:\n")
        for name in sections:
            if f"<!-- BEGIN GENERATED: {name} -->" not in original:
                print(f"  Would ADD section: {name}")
            else:
                pattern = (
                    rf"<!-- BEGIN GENERATED: {re.escape(name)} -->"
                    r".*?"
                    rf"<!-- END GENERATED: {re.escape(name)} -->"
                )
                old_match = re.search(pattern, original, re.DOTALL)
                new_match = re.search(pattern, updated, re.DOTALL)
                if old_match and new_match and old_match.group() != new_match.group():
                    print(f"  Would UPDATE section: {name}")
        print("\nRun with --update to apply changes.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
