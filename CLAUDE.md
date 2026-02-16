# tidyshapes

A data pipeline that processes Overture Maps administrative boundaries into simplified, named output files.

## Quick reference

- **Run**: `uv run tidyshapes`
- **Lint**: `uv run ruff check src`
- **Test**: `uv run pytest`
- **Package manager**: uv (not pip)

## Project structure

```
src/tidyshapes/
  __init__.py
  cli.py          # All pipeline logic: download, join, filter, output
tests/
data/             # Local cache (gitignored), auto-populated on first run
output/           # Generated .bbox files (gitignored)
```

## Data sources

- **Overture Maps Divisions** (GeoParquet from S3, ~3.4GB for division_area)
  - `division`: has `wikidata`, `names`, `id` — no geometry used
  - `division_area`: has `geometry` (WKB), `division_id` (FK to division.id)
  - Join: `division_area.division_id → division.id`
  - English name: `COALESCE(names.common['en'][1], names."primary")`
  - S3 path pattern: `s3://overturemaps-us-west-2/release/{release}/theme=divisions/type={type}/*`
- **QRank** (gzipped CSV from qrank.toolforge.org, ~100MB)
  - Two columns: `Entity` (Wikidata QID like Q30), `QRank` (integer score)
  - Joined to divisions on `division.wikidata = QRank.Entity`

## Key design decisions

- DuckDB for Parquet I/O and joins (httpfs for S3, COPY for local caching)
- Shapely for geometry operations (from_wkb, bounds, future simplification)
- Local disk cache in `data/` keyed by release tag — delete files to re-download
- QRank threshold (default 50,000) filters which division areas get processed
- Output filenames are slugified English names (e.g. `united-states.bbox`)

## Conventions

- Python >=3.13, formatted with ruff (line-length 100)
- No unnecessary abstractions — cli.py is the single module for now
- Prefer DuckDB SQL for data joins/filtering, Python for geometry ops and file I/O
