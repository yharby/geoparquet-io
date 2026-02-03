# Sub-Partitioning Large Files

After partitioning by administrative boundaries or string columns, some partitions may still be too large for efficient querying. This guide shows how to sub-partition oversized files using shell script composition of existing gpio commands.

## The Problem

When you partition data by country or region, population density creates uneven file sizes:

```bash
# Initial partitioning by country
gpio partition admin input.parquet by_country/ --dataset gaul --levels country

# Check resulting file sizes
ls -lh by_country/*/
# USA.parquet       2.1G  ← Too large
# China.parquet     1.8G  ← Too large
# Vatican.parquet   12K   ← Fine
# Monaco.parquet    45K   ← Fine
```

Large files defeat the purpose of partitioning—queries still need to scan gigabytes of data.

## The Solution

Use a shell script to iterate through partitions and apply spatial sub-partitioning to oversized files:

```bash
#!/bin/bash
# sub-partition-large.sh - Sub-partition files exceeding size threshold

INPUT_DIR="by_country"
THRESHOLD=$((100 * 1024 * 1024))  # 100MB in bytes
H3_RESOLUTION=7

for file in "$INPUT_DIR"/*/*.parquet "$INPUT_DIR"/*.parquet; do
  [ -f "$file" ] || continue  # Skip if no matches

  # Get file size (cross-platform)
  size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file")

  if [ "$size" -gt "$THRESHOLD" ]; then
    echo "Sub-partitioning: $file ($(numfmt --to=iec $size 2>/dev/null || echo "${size} bytes"))"
    dir=$(dirname "$file")
    base=$(basename "$file" .parquet)

    # Create sub-partitions
    gpio partition h3 "$file" "$dir/${base}_h3/" --resolution "$H3_RESOLUTION"

    # Remove original after successful sub-partitioning
    rm "$file"
  fi
done
```

### Result Structure

```
by_country/
├── country=USA/
│   └── USA_h3/
│       ├── 872a1008fffffff.parquet
│       ├── 872a1009fffffff.parquet
│       └── ...
├── country=China/
│   └── China_h3/
│       ├── 8730e0c8fffffff.parquet
│       └── ...
├── country=Vatican/
│   └── Vatican.parquet  ← Unchanged (under threshold)
└── country=Monaco/
    └── Monaco.parquet   ← Unchanged (under threshold)
```

## Step-by-Step Workflow

### 1. Initial Partitioning

Start with admin or string partitioning:

=== "By Country"

    ```bash
    gpio partition admin input.parquet by_country/ --dataset gaul --levels country
    ```

=== "By Region"

    ```bash
    gpio partition admin input.parquet by_region/ --dataset gaul --levels continent,country
    ```

=== "By String Column"

    ```bash
    gpio partition string input.parquet by_category/ --column category
    ```

### 2. Preview What Needs Sub-Partitioning

Before running the full script, identify large files:

```bash
# Find files over 100MB
find by_country/ -name "*.parquet" -size +100M -exec ls -lh {} \;

# Or with size in bytes
find by_country/ -name "*.parquet" -size +100M -exec stat -c '%s %n' {} \;
```

### 3. Preview Sub-Partition Strategy

Test on one large file first:

```bash
# Preview H3 partitioning
gpio partition h3 by_country/country=USA/USA.parquet --resolution 7 --preview

# Adjust resolution if needed
gpio partition h3 by_country/country=USA/USA.parquet --resolution 6 --preview
```

### 4. Execute Sub-Partitioning

Run the script or use inline:

```bash
# Using the script
chmod +x sub-partition-large.sh
./sub-partition-large.sh

# Or inline one-liner
for f in by_country/*/*.parquet; do
  [ $(stat -c%s "$f" 2>/dev/null || stat -f%z "$f") -gt 104857600 ] && \
  gpio partition h3 "$f" "$(dirname "$f")/$(basename "$f" .parquet)_h3/" --resolution 7 && \
  rm "$f"
done
```

## Variations

### Using Different Spatial Index Types

=== "S2 Cells"

    ```bash
    # S2 level 10 (~78 km² cells)
    gpio partition s2 "$file" "$dir/${base}_s2/" --level 10
    ```

=== "Quadkey Tiles"

    ```bash
    # Quadkey zoom 9 (~150km tiles)
    gpio partition quadkey "$file" "$dir/${base}_qk/" --resolution 12 --partition-resolution 9
    ```

=== "Auto Resolution"

    ```bash
    # Let gpio calculate optimal resolution
    gpio partition h3 "$file" "$dir/${base}_h3/" --auto --target-rows 100000
    ```

### Adjusting Thresholds

| Threshold | Use Case |
|-----------|----------|
| 50MB | Aggressive splitting for web delivery |
| 100MB | Balanced (recommended default) |
| 250MB | Light splitting for local analysis |
| 500MB | Only split very large files |

```bash
# 50MB threshold for web delivery
THRESHOLD=$((50 * 1024 * 1024))

# 250MB threshold for local analysis
THRESHOLD=$((250 * 1024 * 1024))
```

### Choosing Resolution

Higher resolution = more, smaller files. Lower resolution = fewer, larger files.

| Index | Resolution | Approximate Cell Size |
|-------|------------|----------------------|
| H3 | 5 | ~253 km² |
| H3 | 7 | ~5 km² |
| H3 | 9 | ~0.1 km² |
| S2 | 8 | ~312 km² |
| S2 | 10 | ~78 km² |
| S2 | 12 | ~4.9 km² |

Use `--auto` to let gpio calculate optimal resolution:

```bash
gpio partition h3 "$file" "$dir/${base}_h3/" --auto --target-rows 50000
```

## Complete Example Script

Here's a production-ready script with error handling:

```bash
#!/bin/bash
set -euo pipefail

# Configuration
INPUT_DIR="${1:?Usage: $0 <input_dir> [threshold_mb] [resolution]}"
THRESHOLD_MB="${2:-100}"
RESOLUTION="${3:-7}"

THRESHOLD=$((THRESHOLD_MB * 1024 * 1024))

echo "Sub-partitioning files > ${THRESHOLD_MB}MB in $INPUT_DIR"
echo "Using H3 resolution: $RESOLUTION"
echo

processed=0
skipped=0

for file in "$INPUT_DIR"/**/*.parquet "$INPUT_DIR"/*.parquet; do
  [ -f "$file" ] || continue

  size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file")

  if [ "$size" -gt "$THRESHOLD" ]; then
    dir=$(dirname "$file")
    base=$(basename "$file" .parquet)
    outdir="$dir/${base}_h3"

    echo "Processing: $file"
    echo "  Size: $(numfmt --to=iec $size 2>/dev/null || echo "$size bytes")"

    if gpio partition h3 "$file" "$outdir/" --resolution "$RESOLUTION"; then
      rm "$file"
      echo "  Created: $outdir/"
      ((processed++))
    else
      echo "  ERROR: Failed to partition $file" >&2
    fi
    echo
  else
    ((skipped++))
  fi
done

echo "Complete: $processed files sub-partitioned, $skipped files skipped (under threshold)"
```

Usage:

```bash
# Default: 100MB threshold, H3 resolution 7
./sub-partition.sh by_country/

# Custom: 50MB threshold, H3 resolution 8
./sub-partition.sh by_country/ 50 8
```

## Tips

### Use `--preview` First

Always preview before executing on production data:

```bash
gpio partition h3 large_file.parquet --resolution 7 --preview
```

### Handle Errors Gracefully

The script should continue on errors. Add error handling:

```bash
if ! gpio partition h3 "$file" "$outdir/" --resolution "$RESOLUTION"; then
  echo "Warning: Failed to sub-partition $file, keeping original" >&2
  continue
fi
```

### Preserve Hive Structure

If your initial partition uses Hive format, maintain it:

```bash
gpio partition admin input.parquet output/ --dataset gaul --levels country --hive
# Creates: output/country=USA/data.parquet

# Sub-partition maintains hierarchy
gpio partition h3 output/country=USA/data.parquet output/country=USA/h3/ --resolution 7 --hive
# Creates: output/country=USA/h3/h3=872a1008fffffff/data.parquet
```

### Cross-Platform Compatibility

The `stat` command differs between Linux and macOS:

```bash
# Cross-platform file size
get_size() {
  stat -c%s "$1" 2>/dev/null || stat -f%z "$1"
}
size=$(get_size "$file")
```

## Why Documentation vs Native Feature?

This guide documents shell composition rather than a native `gpio partition --sub-partition` feature because:

1. **Composability**: Existing commands chain naturally via shell scripts
2. **Flexibility**: Users can customize thresholds, index types, and resolution per-partition
3. **Simplicity**: Avoids complex configuration for edge cases (different resolutions for different countries)

A native implementation would require unifying partition interfaces, handling output path conflicts, and supporting per-partition configuration—significant complexity for a use case well-served by scripting.

## See Also

- [Partitioning Files](partition.md) - All partition command options
- [Command Piping](piping.md) - Chaining commands without intermediate files
- [Remote Files](remote-files.md) - Working with S3 and cloud storage
