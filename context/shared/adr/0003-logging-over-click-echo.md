# ADR-0003: Logging Over click.echo in Core

## Status

Accepted

## Context

As described in ADR-0001, geoparquet-io separates CLI presentation from business logic. Core modules must not depend on the Click framework, but they still need to communicate status, warnings, and progress to users. Early development mixed `click.echo()` calls into core modules, creating a hidden dependency on the CLI framework and making it impossible to control output when using the Python API.

## Decision

Core modules use the logging helpers defined in `geoparquet_io/core/logging_config.py` instead of `click.echo()`. The module provides these convenience functions:

- `success(message)` -- Operation completed successfully (green output in CLI)
- `warn(message)` -- Non-fatal warning (yellow output)
- `error(message)` -- Error condition (red output)
- `info(message)` -- Informational message
- `debug(message)` -- Debug-level detail (hidden by default)
- `progress(message)` -- Progress updates for long-running operations

The logging system uses Python's standard `logging` module under the hood, with custom formatters (`CLIFormatter`, `LibraryFormatter`) that provide colored output in CLI context and clean output when used as a library.

**Rule: `click.echo()` is never used in `core/` modules.** It is only permitted in `cli/main.py` where Click-specific formatting (e.g., `click.style()`) is needed for the CLI presentation layer.

## Consequences

### Positive
- Core modules have no dependency on Click, maintaining the architectural boundary from ADR-0001.
- The Python API can control logging verbosity through standard Python logging configuration.
- Output formatting is consistent across all core modules through shared formatters.
- `verbose_logging()` context manager and `configure_verbose()` allow users and the CLI to toggle debug output.

### Negative
- Contributors must remember to import from `logging_config` rather than using the more familiar `click.echo()` or `print()`.
- The logging helpers add a small abstraction layer that new contributors need to learn.

### Neutral
- The `DynamicStreamHandler` class supports switching between stdout and stderr at runtime, which is necessary for certain CLI output patterns (e.g., piping data to stdout while logging to stderr).

## Alternatives Considered

### Allow click.echo everywhere
Permitting `click.echo()` in core modules. Rejected because it couples core logic to Click, making the Python API depend on a CLI framework and preventing output control in library usage.

### Python print() in core
Using bare `print()` statements. Rejected because print provides no log-level control, no colored output, and no way to suppress output in library mode.

### Third-party logging (e.g., Rich, Loguru)
Using a feature-rich logging library. Rejected as unnecessary additional dependency -- Python's built-in logging module with custom formatters provides everything needed.

## References

- `geoparquet_io/core/logging_config.py` -- Logging helpers and formatters
- ADR-0001 -- CLI/Core separation that motivates this decision
