"""LLM skills for geoparquet-io.

This package contains skill files that help LLMs (ChatGPT, Claude, etc.)
work effectively with the gpio CLI tool.

Skills are markdown files with structured instructions that teach LLMs
how to use gpio for spatial data workflows.

Usage:
    gpio skills              # List available skills
    gpio skills --show       # Print skill content to stdout
    gpio skills --copy .     # Copy skill to current directory
"""

from importlib.resources import files
from pathlib import Path


def get_skill_path(name: str = "geoparquet") -> Path:
    """Get the path to a bundled skill file.

    Args:
        name: Skill name (without .md extension). Default: "geoparquet"

    Returns:
        Path to the skill markdown file.

    Raises:
        FileNotFoundError: If skill doesn't exist.
    """
    skill_file = files(__package__) / f"{name}.md"
    if not skill_file.is_file():
        raise FileNotFoundError(f"Skill '{name}' not found")
    return Path(str(skill_file))


def get_skill_content(name: str = "geoparquet") -> str:
    """Get the content of a bundled skill file.

    Args:
        name: Skill name (without .md extension). Default: "geoparquet"

    Returns:
        Skill content as string.
    """
    skill_path = get_skill_path(name)
    return skill_path.read_text(encoding="utf-8")


def list_skills() -> list[str]:
    """List all available skill names.

    Returns:
        List of skill names (without .md extension).
    """
    skills_dir = files(__package__)
    return [
        p.name[:-3]  # Remove .md extension
        for p in skills_dir.iterdir()
        if p.name.endswith(".md")
    ]
