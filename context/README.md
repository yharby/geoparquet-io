# Context Directory

This directory contains project context and reference documentation to support development.

## Structure

### `shared/` - Team Knowledge Base

Shared knowledge accessible to all team members and AI assistants.

#### Current Content

- **`documentation/`** - Reference documentation
  - `geometry-metadata-handling.md` - Geometry type handling and GeoParquet metadata
  - `platform-specific-issues.md` - Platform compatibility notes (Windows, macOS, etc.)

#### Future Use (Currently Empty)

These directories are available for future use but currently empty:

- **`plans/`** - Project planning documents (feature plans, roadmaps)
- **`reports/`** - Analysis and status reports (codebase reviews, coverage)
- **`research/`** - Auto-generated research documents (investigation results)

## Usage Guidelines

### What Goes Where?

| Document Type | Location | Commit? |
|--------------|----------|---------|
| Reference documentation | `shared/documentation/` | Yes |
| Feature plans | `shared/plans/` | Yes |
| Status reports | `shared/reports/` | Yes |
| Research findings | `shared/research/` | Yes |

### AI Assistant Context

This directory helps AI assistants:
- Access platform-specific documentation (`shared/documentation/`)
- Understand geometry handling patterns
- Maintain consistency across conversations
