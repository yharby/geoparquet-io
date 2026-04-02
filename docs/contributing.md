# Contributing to geoparquet-io

Thank you for your interest in contributing to geoparquet-io! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) package manager

### Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/geoparquet/geoparquet-io.git
   cd geoparquet-io
   ```

2. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies**
   ```bash
   uv sync --all-extras
   ```

4. **Install pre-commit hooks**
   ```bash
   uv run pre-commit install
   ```

   This ensures code is automatically formatted and linted before each commit.

## Development Workflow

### Testing Your Changes

#### For External Contributors

!!! tip "You don't need all dev tools installed"
    Some tests check that tooling is available (codespell, mypy, bandit, etc.). These
    may fail locally if you don't have the full dev environment, but **that's OK**—they
    run in CI and aren't testing your code. Focus on the tests relevant to your changes.

#### Quick Test: Test Your PR Changes

For most contributions, test only the code you modified:

```bash
# Test specific file you changed
uv run pytest tests/test_yourfile.py -v

# Test with fast markers only (skips slow/network tests)
uv run pytest tests/test_yourfile.py -v -m "not slow and not network"
```

#### Core Tests: Skip Tooling Checks

Run core functional tests without tooling-availability checks:

```bash
uv run pytest -m "not slow and not network" \
  --ignore=tests/test_codespell.py \
  --ignore=tests/test_commitizen.py \
  --ignore=tests/test_mypy.py \
  --ignore=tests/test_mutmut.py \
  --ignore=tests/test_security.py
```

#### Full Test Suite: Match CI

To run the complete test suite as CI does (requires all extras installed):

```bash
# Install all dependencies including dev tools
uv sync --all-extras

# Run full suite
uv run pytest

# Run with coverage
uv run pytest --cov=geoparquet_io --cov-report=term-missing
```

#### Common Test Commands

```bash
# Run specific test file
uv run pytest tests/test_sort.py

# Run specific test function
uv run pytest tests/test_sort.py::test_hilbert_order

# Run tests matching a keyword
uv run pytest -k "hilbert"

# Skip slow/network tests
uv run pytest -m "not slow and not network"
```

### Code Style

We use [ruff](https://github.com/astral-sh/ruff) for linting and formatting.

```bash
# Format code
uv run ruff format .

# Check formatting
uv run ruff format --check .

# Run linter
uv run ruff check .

# Fix auto-fixable issues
uv run ruff check --fix .
```

**Style Guidelines:**
- Line length: 100 characters
- Follow PEP 8 conventions
- Use double quotes for strings
- Add docstrings to all public functions and classes
- Use type hints where helpful

### Pre-commit Hooks

Pre-commit hooks run automatically before each commit. They will:
- Format code with ruff
- Run linting checks
- Remove trailing whitespace
- Fix end-of-file issues
- Check YAML and TOML syntax
- Check commit message format (capital letter for imperative mood)
- Remind about Python API when CLI changes are made
- Sync dependencies when pyproject.toml changes

To run hooks manually:
```bash
uv run pre-commit run --all-files
```

#### Optional: Enable Pre-Push Tests

To run fast tests automatically before pushing, first install the pre-push hook:
```bash
# Install the pre-push hook
uv run pre-commit install --hook-type pre-push

# Then enable the tests
export ENABLE_PRE_PUSH_TESTS=1
```

This will run `pytest -m "not slow and not network"` before each push.

To disable:
```bash
unset ENABLE_PRE_PUSH_TESTS
```

## Making Changes

### Branch Naming

- Feature: `feature/description` (e.g., `feature/add-streaming-support`)
- Bug fix: `fix/description` (e.g., `fix/bbox-metadata-issue`)
- Documentation: `docs/description` (e.g., `docs/update-readme`)

### Commit Messages

Use imperative mood with a capital letter:

```
<Verb> <what changed>
```

**Good examples:**
```
Add streaming mode for large files
Fix bbox metadata format for GeoParquet 1.1
Update installation instructions in README
Add edge case tests for empty file partitions
Remove deprecated --format flag from convert
```

**Bad examples:**
```
added streaming mode  # Wrong: lowercase, past tense
Adding new feature    # Wrong: continuous tense
fixes bug            # Wrong: lowercase
```

Keep first line under 72 characters. Add details in body if needed.

### Pull Request Process

1. **Create a new branch** from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write code
   - Add/update tests
   - Update documentation

3. **Ensure tests pass** (see [Testing Your Changes](#testing-your-changes))
   ```bash
   # At minimum, test your changes:
   uv run pytest tests/test_yourfile.py -v
   ```

4. **Ensure code is formatted**
   ```bash
   uv run ruff format .
   uv run ruff check .
   ```

5. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat(scope): description"
   ```

6. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request**
   - Go to GitHub and create a PR from your branch to `main`
   - Fill in the PR template with a clear description
   - Link any related issues
   - Request review from maintainers

### Pull Request Requirements

Before submitting a PR, ensure:

- [ ] Tests for your changes pass (see [Testing Your Changes](#testing-your-changes))
- [ ] Code coverage is maintained or improved
- [ ] Code is formatted (`uv run ruff format --check .`)
- [ ] Linting passes (`uv run ruff check .`)
- [ ] Documentation is updated (README, docstrings, etc.)
- [ ] CHANGELOG.md is updated (for user-facing changes)
- [ ] Commit messages follow the [commit message guidelines](#commit-messages)

!!! note "CI runs the full test suite"
    Don't worry if some tooling tests (codespell, mypy, security) fail locally—CI will
    run the complete test suite with all dependencies installed.

## Testing Guidelines

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Name test functions `test_*`
- Use descriptive test names that explain what is being tested
- Group related tests in classes (e.g., `TestHilbertSort`)

### Test Structure

```python
def test_feature_description():
    """Brief description of what this test verifies."""
    # Arrange
    input_data = create_test_data()

    # Act
    result = function_under_test(input_data)

    # Assert
    assert result == expected_value
```

### Test Markers

Use pytest markers for special test categories:

```python
import pytest

@pytest.mark.slow
def test_large_file_processing():
    """Test that takes a long time to run."""
    pass

@pytest.mark.network
def test_remote_file_access():
    """Test that requires network access."""
    pass
```

### Test Fixtures

Add reusable fixtures to `tests/conftest.py`:

```python
import pytest

@pytest.fixture
def sample_geoparquet():
    """Provides a sample GeoParquet file for testing."""
    return "tests/data/sample.parquet"
```

## Code Review

### For Contributors

- Respond to feedback promptly
- Be open to suggestions and constructive criticism
- Keep discussions focused and professional
- Update your PR based on feedback

### For Reviewers

- Be respectful and constructive
- Explain the reasoning behind suggestions
- Approve when the code meets quality standards
- Help contributors improve their submissions

## Release Process

(For maintainers only)

### Pre-Release Checklist

1. **Run benchmarks** to check for performance regressions:
   ```bash
   # Install previous release
   git checkout v0.9.0 && uv sync
   uv run python scripts/version_benchmark.py --version-label "baseline" -o baseline.json -n 5

   # Install new version
   git checkout main && uv sync
   uv run python scripts/version_benchmark.py --version-label "new" -o current.json -n 5

   # Compare results
   uv run python scripts/version_benchmark.py --compare baseline.json current.json
   ```

2. **Review any regressions** (>25% slower on large files):
   - If intentional (new features), document in release notes
   - If unintentional, investigate and fix before release

### Release Steps

1. Update version in `pyproject.toml` and `geoparquet_io/cli/main.py`
2. Update `CHANGELOG.md` with release notes
3. Create and push a git tag: `git tag v0.x.0 && git push origin v0.x.0`
4. Create a GitHub release with the changelog content
5. GitHub Actions will automatically:
   - Build and publish to PyPI
   - Run benchmark comparison against previous release
   - Append performance results to release notes

See [Performance Benchmarks](guide/benchmarks.md) for details on the benchmark system.

## Questions?

- Open an issue for bug reports or feature requests
- Use GitHub Discussions for questions
- Check existing issues and PRs before creating new ones

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help create a welcoming environment
- Report unacceptable behavior to maintainers

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
