# tidyshapes

Tidy polygon gazetteer. Processes [Overture Maps](https://overturemaps.org) administrative boundaries into simplified, named output files.

tidyshapes downloads division areas from Overture Maps, joins them with [QRank](https://qrank.toolforge.org) (a Wikidata popularity ranking), and outputs bounding boxes for the most notable places as simple text files named by their English name (e.g. `united-states.bbox`).

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## Install

```sh
uv sync
```

## Usage

Run the full pipeline:

```sh
uv run tidyshapes
```

This will:

1. Download Overture Maps `division` and `division_area` parquet files from S3 (~3.4 GB for `division_area`)
2. Download the QRank dataset (~100 MB)
3. Join areas with their English names via Wikidata IDs
4. Write `.bbox` files for places above the QRank threshold to `output/`

Downloaded data is cached in `data/` — delete files there to force a re-download.

### Options

```
--release TAG         Overture release tag (default: 2026-01-21.0)
--qrank-threshold N   Minimum QRank score to include (default: 50000)
-o, --output-dir DIR  Output directory (default: output)
```

Examples:

```sh
# Use a different Overture release
uv run tidyshapes --release 2025-10-01.0

# Include more places by lowering the QRank threshold
uv run tidyshapes --qrank-threshold 10000

# Write output to a custom directory
uv run tidyshapes -o my-output
```

## Output format

Each output file is named `{slugified-name}.bbox` and contains a single line with the bounding box coordinates:

```
minx,miny,maxx,maxy
```

For example, `united-states.bbox`:

```
-179.174265,17.913769,-66.949895,71.352561
```

## Development

```sh
uv run ruff check src   # lint
uv run pytest            # test
```
