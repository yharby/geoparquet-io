"""Tests for mutation testing configuration."""

import subprocess
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


class TestMutmutConfig:
    """Test mutmut is properly configured."""

    def test_mutmut_installed(self):
        """Verify mutmut is installed."""
        result = subprocess.run(
            ["uv", "run", "mutmut", "--version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "mutmut" in result.stdout.lower()

    def test_mutmut_config_exists(self):
        """Verify mutmut config in pyproject.toml."""
        pyproject = Path(__file__).parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        assert "mutmut" in config.get("tool", {}), "mutmut config missing from [tool.mutmut]"
        mutmut_config = config["tool"]["mutmut"]
        assert "paths_to_mutate" in mutmut_config
        assert "geoparquet_io" in mutmut_config["paths_to_mutate"]

    def test_mutmut_paths_exist(self):
        """Verify paths_to_mutate points to existing directory."""
        pyproject = Path(__file__).parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        paths = config["tool"]["mutmut"]["paths_to_mutate"]
        project_root = pyproject.parent

        # paths_to_mutate can be a string or list
        if isinstance(paths, str):
            paths = [paths]

        for path in paths:
            full_path = project_root / path.rstrip("/")
            assert full_path.exists(), f"Mutation path does not exist: {path}"

    def test_mutmut_tests_dir_exists(self):
        """Verify tests_dir points to existing directory."""
        pyproject = Path(__file__).parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        tests_dir = config["tool"]["mutmut"].get("tests_dir", "tests/")
        project_root = pyproject.parent
        full_path = project_root / tests_dir.rstrip("/")
        assert full_path.exists(), f"Tests directory does not exist: {tests_dir}"

    def test_mutmut_dev_dependency(self):
        """Verify mutmut is in dev optional-dependencies."""
        pyproject = Path(__file__).parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            config = tomllib.load(f)

        dev_deps = config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        has_mutmut = any("mutmut" in dep for dep in dev_deps)
        assert has_mutmut, "mutmut not found in [project.optional-dependencies] dev"


class TestNightlyWorkflow:
    """Test nightly workflow configuration."""

    def test_nightly_workflow_exists(self):
        """Verify nightly workflow file exists."""
        workflow = Path(__file__).parent.parent / ".github/workflows/nightly.yml"
        assert workflow.exists(), "nightly.yml workflow missing"

    def test_nightly_workflow_valid_yaml(self):
        """Verify nightly workflow is valid YAML."""
        import yaml

        workflow = Path(__file__).parent.parent / ".github/workflows/nightly.yml"
        with open(workflow) as f:
            config = yaml.safe_load(f)

        # PyYAML parses 'on' as True (YAML boolean) - check either key
        triggers = config.get("on", config.get(True))
        assert triggers is not None, "Missing 'on' triggers section"
        assert "jobs" in config
        assert "mutation" in config["jobs"]

    def test_nightly_has_schedule_trigger(self):
        """Verify nightly workflow has schedule trigger."""
        import yaml

        workflow = Path(__file__).parent.parent / ".github/workflows/nightly.yml"
        with open(workflow) as f:
            config = yaml.safe_load(f)

        # PyYAML parses 'on' as True (YAML boolean)
        triggers = config.get("on", config.get(True))
        assert triggers is not None, "Missing 'on' triggers section"
        assert "schedule" in triggers, "Missing schedule trigger"

    def test_nightly_has_manual_trigger(self):
        """Verify nightly workflow can be triggered manually."""
        import yaml

        workflow = Path(__file__).parent.parent / ".github/workflows/nightly.yml"
        with open(workflow) as f:
            config = yaml.safe_load(f)

        # PyYAML parses 'on' as True (YAML boolean)
        triggers = config.get("on", config.get(True))
        assert triggers is not None, "Missing 'on' triggers section"
        assert "workflow_dispatch" in triggers, "Missing manual trigger"

    def test_nightly_mutation_job_has_timeout(self):
        """Verify mutation job has a timeout set."""
        import yaml

        workflow = Path(__file__).parent.parent / ".github/workflows/nightly.yml"
        with open(workflow) as f:
            config = yaml.safe_load(f)

        mutation_job = config["jobs"]["mutation"]
        assert "timeout-minutes" in mutation_job, "Missing timeout-minutes on mutation job"
        assert mutation_job["timeout-minutes"] >= 60, "Timeout should be at least 60 minutes"
