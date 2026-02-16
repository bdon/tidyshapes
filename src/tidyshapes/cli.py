"""Command-line interface for tidyshapes."""

import argparse
import gzip
import re
import subprocess
import sys
import unicodedata
import urllib.request
from pathlib import Path

import duckdb
import shapely

RELEASE = "2026-01-21.0"
S3_BASE = "s3://overturemaps-us-west-2/release/{release}/theme=divisions"
QRANK_URL = "https://qrank.toolforge.org/download/qrank.csv.gz"
QRANK_THRESHOLD = 50_000
CACHE_DIR = Path("data")


def ensure_parquet(conn: duckdb.DuckDBPyConnection, release: str, type_name: str) -> Path:
    """Download a divisions parquet type to local cache if not already present."""
    cache_path = CACHE_DIR / f"{type_name}_{release}.parquet"
    if cache_path.exists():
        print(f"  Using cached {cache_path}")
        return cache_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    s3_base = S3_BASE.format(release=release)
    s3_path = f"{s3_base}/type={type_name}/*"
    print(f"  Downloading {type_name} from {release}...")
    conn.execute(
        f"COPY (SELECT * FROM read_parquet('{s3_path}', hive_partitioning=true)) "
        f"TO '{cache_path}' (FORMAT PARQUET)"
    )
    print(f"  Saved to {cache_path}")
    return cache_path


def ensure_qrank() -> Path:
    """Download QRank CSV to local cache if not already present."""
    cache_path = CACHE_DIR / "qrank.csv.gz"
    if cache_path.exists():
        print(f"  Using cached {cache_path}")
        return cache_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Downloading QRank from {QRANK_URL}...")
    urllib.request.urlretrieve(QRANK_URL, cache_path)
    print(f"  Saved to {cache_path}")
    return cache_path


def load_qrank(path: Path) -> dict[str, int]:
    """Load QRank CSV into a dict mapping Wikidata QID to rank score."""
    qrank = {}
    with gzip.open(path, "rt") as f:
        next(f)  # skip header
        for line in f:
            entity, score = line.strip().split(",")
            qrank[entity] = int(score)
    print(f"  {len(qrank):,} QRank entries loaded")
    return qrank


def slugify(text: str) -> str:
    """Convert text to an ASCII-only URL-friendly slug."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def load_areas_with_wikidata(
    conn: duckdb.DuckDBPyConnection, division_path: Path, area_path: Path
) -> list[tuple[str, str, bytes]]:
    """Join division_area with division to get (wikidata, english_name, geometry_wkb)."""
    rows = conn.execute(
        f"""
        SELECT d.wikidata, COALESCE(d.names.common['en'][1], d.names."primary") AS en_name,
               a.geometry
        FROM read_parquet('{area_path}') a
        JOIN read_parquet('{division_path}') d ON a.division_id = d.id
        WHERE d.wikidata IS NOT NULL
        """
    ).fetchall()
    print(f"  {len(rows):,} division areas with wikidata IDs")
    return rows


def cmd_build(args):
    """Run the build pipeline."""
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")

    print("Downloading data:")
    division_path = ensure_parquet(conn, args.release, "division")
    area_path = ensure_parquet(conn, args.release, "division_area")
    qrank_path = ensure_qrank()

    print("Loading QRank:")
    qrank = load_qrank(qrank_path)

    print("Joining division areas with divisions:")
    rows = load_areas_with_wikidata(conn, division_path, area_path)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for wikidata_id, en_name, geom_wkb in rows:
        score = qrank.get(wikidata_id, 0)
        if score < args.qrank_threshold:
            continue

        geom = shapely.from_wkb(geom_wkb)
        minx, miny, maxx, maxy = geom.bounds

        slug = slugify(en_name)
        if not slug:
            continue
        bbox_path = output_dir / f"{slug}.bbox"
        bbox_path.write_text(f"{minx},{miny},{maxx},{maxy}\n")
        count += 1

    print(f"Wrote {count} .bbox files to {output_dir}/")


def cmd_upload(args):
    """Upload output files to R2."""
    cmd = [
        "aws", "s3", "sync",
        args.output_dir,
        f"s3://{args.bucket}/{args.version}/",
        "--endpoint-url", args.endpoint_url,
    ]
    print(f"Running: {' '.join(cmd)}")
    sys.exit(subprocess.call(cmd))


def main():
    parser = argparse.ArgumentParser(description="Process Overture Maps division areas")
    subparsers = parser.add_subparsers(dest="command")

    # build subcommand (default)
    build_parser = subparsers.add_parser("build", help="Run the build pipeline")
    build_parser.add_argument(
        "--release", default=RELEASE, help=f"Overture release tag (default: {RELEASE})"
    )
    build_parser.add_argument(
        "--qrank-threshold",
        type=int,
        default=QRANK_THRESHOLD,
        help=f"Minimum QRank score to include (default: {QRANK_THRESHOLD})",
    )
    build_parser.add_argument("-o", "--output-dir", default="output", help="Output directory")

    # upload subcommand
    upload_parser = subparsers.add_parser("upload", help="Upload output files to R2")
    upload_parser.add_argument("version", help="Version prefix (e.g. v0, v1)")
    upload_parser.add_argument("--bucket", required=True, help="R2 bucket name")
    upload_parser.add_argument("--endpoint-url", required=True, help="R2 S3-compatible endpoint")
    upload_parser.add_argument("--output-dir", default="output", help="Output directory")

    args = parser.parse_args()

    if args.command == "upload":
        cmd_upload(args)
    else:
        # Default to build (handles both `tidyshapes build` and bare `tidyshapes`)
        if args.command is None:
            args = build_parser.parse_args()
        cmd_build(args)


if __name__ == "__main__":
    main()
