#!/usr/bin/env python3
"""Generate SKILL.md sections from CLI introspection.

This script auto-generates documentation sections to prevent drift in the
Claude Code skill file. It extracts actual commands, subcommands, and options
from the Click CLI to ensure accuracy.

Usage:
    python scripts/generate_skill_commands.py          # Default: show diff
    python scripts/generate_skill_commands.py --update  # Update SKILL.md in place
    python scripts/generate_skill_commands.py --check   # Exit 1 if outdated
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import click


def get_cli_structure() -> dict[str, dict]:
    """Introspect Click CLI to get all commands, subcommands, and key options.

    Returns:
        Dict mapping command group name to info dict with structure.
    """
    from geoparquet_io.cli.main import cli

    result = {}
    for name, cmd in sorted(cli.commands.items()):
        if isinstance(cmd, click.Group):
            subcommands = {}
            for subname, subcmd in sorted(cmd.commands.items()):
                # Get key options (skip common ones like --help)
                options = []
                for param in subcmd.params:
                    if isinstance(param, click.Option) and param.name not in ("help", "verbose"):
                        opt_str = "/".join(param.opts)
                        if param.default is not None and param.default != ():
                            options.append(f"{opt_str} (default: {param.default})")
                        else:
                            options.append(opt_str)
                subcommands[subname] = {
                    "help": (subcmd.help or "").split("\n")[0].strip(),
                    "options": options[:5],  # Limit to top 5 options
                }
            result[name] = {
                "help": (cmd.help or "").split("\n")[0].strip(),
                "subcommands": subcommands,
            }
        else:
            options = []
            for param in cmd.params:
                if isinstance(param, click.Option) and param.name not in ("help", "verbose"):
                    opt_str = "/".join(param.opts)
                    options.append(opt_str)
            result[name] = {
                "help": (cmd.help or "").split("\n")[0].strip(),
                "subcommands": {},
                "options": options[:5],
            }
    return result


def generate_commands_table() -> str:
    """Generate the CLI commands table for SKILL.md."""
    structure = get_cli_structure()

    lines = [
        "<!-- BEGIN GENERATED: skill-commands -->",
        "### Command Reference",
        "",
        "| Command | Subcommands | Description |",
        "|---------|-------------|-------------|",
    ]

    for name, info in sorted(structure.items()):
        subs = ", ".join(sorted(info.get("subcommands", {}).keys()))
        desc = info["help"][:60] + "..." if len(info["help"]) > 60 else info["help"]
        lines.append(f"| `gpio {name}` | {subs} | {desc} |")

    lines.append("<!-- END GENERATED: skill-commands -->")
    return "\n".join(lines)


def generate_compression_options() -> str:
    """Generate compression options section from actual CLI defaults."""
    from geoparquet_io.cli.main import cli

    # Find a command that uses compression options
    compression = "zstd"
    compression_level = 15

    # Check convert geoparquet command for compression defaults
    if "convert" in cli.commands:
        convert_group = cli.commands["convert"]
        if isinstance(convert_group, click.Group) and "geoparquet" in convert_group.commands:
            geoparquet_cmd = convert_group.commands["geoparquet"]
            for param in geoparquet_cmd.params:
                if param.name == "compression":
                    val = param.default
                    # Handle enum values and sentinel objects
                    if hasattr(val, "value"):
                        compression = str(val.value).lower()
                    elif val is not None and "Sentinel" not in str(type(val)):
                        compression = str(val).lower()
                elif param.name == "compression_level":
                    val = param.default
                    # Skip sentinel/unset values
                    if val is not None and "Sentinel" not in str(type(val)):
                        compression_level = val

    lines = [
        "<!-- BEGIN GENERATED: compression-options -->",
        "### Compression Options",
        "",
        "Most write commands accept these options:",
        "",
        "| Option | Values | Default |",
        "|--------|--------|---------|",
        f"| `--compression` | zstd, snappy, gzip, lz4, brotli, none | {compression} |",
        f"| `--compression-level` | 1-22 (for zstd) | {compression_level} |",
        "| `--row-group-size` | Number of rows per group | varies by command |",
        "<!-- END GENERATED: compression-options -->",
    ]
    return "\n".join(lines)


def generate_inspection_commands() -> str:
    """Generate inspection commands section."""
    from geoparquet_io.cli.main import cli

    lines = [
        "<!-- BEGIN GENERATED: inspect-commands -->",
        "### Inspection Commands",
        "",
        "```bash",
    ]

    if "inspect" in cli.commands:
        inspect_group = cli.commands["inspect"]
        if isinstance(inspect_group, click.Group):
            for subname, subcmd in sorted(inspect_group.commands.items()):
                help_line = (subcmd.help or "").split("\n")[0].strip()
                lines.append(f"gpio inspect {subname} <file>  # {help_line[:40]}")

    lines.extend(
        [
            "```",
            "<!-- END GENERATED: inspect-commands -->",
        ]
    )
    return "\n".join(lines)


def generate_check_commands() -> str:
    """Generate check commands section."""
    from geoparquet_io.cli.main import cli

    lines = [
        "<!-- BEGIN GENERATED: check-commands -->",
        "### Validation Commands",
        "",
        "```bash",
        "# Run all checks",
        "gpio check all <file>",
        "",
        "# Individual checks",
    ]

    if "check" in cli.commands:
        check_group = cli.commands["check"]
        if isinstance(check_group, click.Group):
            for subname in sorted(check_group.commands.keys()):
                if subname != "all":
                    lines.append(f"gpio check {subname} <file>")

    lines.extend(
        [
            "",
            "# Auto-fix issues",
            "gpio check all <file> --fix --output <fixed_file>",
            "```",
            "<!-- END GENERATED: check-commands -->",
        ]
    )
    return "\n".join(lines)


def update_section(content: str, section_name: str, new_content: str) -> str:
    """Replace a generated section in the content."""
    pattern = (
        rf"<!-- BEGIN GENERATED: {re.escape(section_name)} -->"
        r".*?"
        rf"<!-- END GENERATED: {re.escape(section_name)} -->"
    )

    if re.search(pattern, content, re.DOTALL):
        return re.sub(pattern, new_content, content, count=1, flags=re.DOTALL)
    else:
        return content


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate SKILL.md sections from CLI")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--update", action="store_true", help="Update SKILL.md in place")
    group.add_argument(
        "--check", action="store_true", help="Check if sections are current (exit 1 if not)"
    )
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    skill_md = project_root / ".claude" / "SKILL.md"

    if not skill_md.exists():
        print("ERROR: .claude/SKILL.md not found")
        return 2

    original = skill_md.read_text(encoding="utf-8")
    updated = original

    # Generate each section
    sections = {
        "skill-commands": generate_commands_table(),
        "compression-options": generate_compression_options(),
        "inspect-commands": generate_inspection_commands(),
        "check-commands": generate_check_commands(),
    }

    for name, content in sections.items():
        updated = update_section(updated, name, content)

    if updated == original:
        print("SKILL.md sections are up to date!")
        return 0

    if args.update:
        skill_md.write_text(updated, encoding="utf-8")
        print("SKILL.md updated successfully!")
        return 0
    elif args.check:
        print("SKILL.md sections are out of date. Run with --update to fix.")
        return 1
    else:
        print("SKILL.md sections would change:\n")
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
