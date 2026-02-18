"""Command-line interface for tidyshapes."""

import argparse
import gzip
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import unicodedata
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def write_if_changed(path: Path, content: str) -> bool:
    """Write content to path only if it differs from existing file."""
    try:
        if path.read_text() == content:
            return False
    except FileNotFoundError:
        pass
    path.write_text(content)
    return True


def slugify(text: str) -> str:
    """Convert text to an ASCII-only URL-friendly slug."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


SIMPLIFY_TARGETS = {"1k": 1_000, "10k": 10_000}


def simplify_to_target(geom, target):
    """Binary search for a Douglas-Peucker tolerance that yields <= target vertices."""
    n = shapely.get_num_coordinates(geom)
    if n <= target:
        return geom
    minx, miny, maxx, maxy = geom.bounds
    low, high = 0.0, max(maxx - minx, maxy - miny)
    for _ in range(20):
        mid = (low + high) / 2
        if shapely.get_num_coordinates(shapely.simplify(geom, mid)) > target:
            low = mid
        else:
            high = mid
    return shapely.simplify(geom, high)


SUBTYPE_LABELS = {
    "locality": "city",
    "region": "state",
    "country": "country",
    "county": "county",
    "dependency": "dependency",
    "localadmin": "localadmin",
    "neighborhood": "neighborhood",
    "macrohood": "macrohood",
    "microhood": "microhood",
}

def load_areas(
    conn: duckdb.DuckDBPyConnection, division_path: Path, area_path: Path
) -> list[tuple]:
    """Join division_area with division and parent to get metadata for disambiguation."""
    rows = conn.execute(
        f"""
        SELECT d.wikidata, d.subtype,
               COALESCE(d.names.common['en'], d.names."primary") AS en_name,
               COALESCE(p.names.common['en'], p.names."primary") AS parent_name,
               a.geometry, octet_length(a.geometry) AS geom_size
        FROM read_parquet('{area_path}') a
        JOIN read_parquet('{division_path}') d ON a.division_id = d.id
        LEFT JOIN read_parquet('{division_path}') p ON d.parent_division_id = p.id
        WHERE d.wikidata IS NOT NULL
        """
    ).fetchall()
    print(f"  {len(rows):,} division areas with wikidata IDs")
    return rows


def dedup_by_wikidata(rows, qrank, threshold):
    """Keep only the largest geometry per wikidata ID, filtered by QRank."""
    best = {}
    for wikidata_id, subtype, en_name, parent_name, geom_wkb, geom_size in rows:
        if qrank.get(wikidata_id, 0) < threshold:
            continue
        if wikidata_id not in best or geom_size > best[wikidata_id][5]:
            best[wikidata_id] = (wikidata_id, subtype, en_name, parent_name, geom_wkb, geom_size)
    print(f"  {len(best):,} unique entries after dedup and QRank filter")
    return list(best.values())


def resolve_collisions(entries, qrank):
    """Assign unique slugs. Highest QRank keeps the bare slug; others get suffixed."""
    by_slug = defaultdict(list)
    for e in entries:
        slug = slugify(e[2])  # en_name
        if slug:
            by_slug[slug].append(e)

    result = {}  # slug -> (wikidata_id, geom_wkb)
    for base_slug, group in by_slug.items():
        if len(group) == 1:
            result[base_slug] = (group[0][0], group[0][4])
            continue

        # Highest QRank keeps the bare slug
        group.sort(key=lambda e: qrank.get(e[0], 0), reverse=True)
        result[base_slug] = (group[0][0], group[0][4])

        # Disambiguate the rest: try subtype, then parent name, then QID
        for e in group[1:]:
            label = SUBTYPE_LABELS.get(e[1], e[1])
            candidate = f"{base_slug}-{label}"
            if candidate not in result:
                result[candidate] = (e[0], e[4])
                continue
            parent_slug = slugify(e[3]) if e[3] else ""
            if parent_slug:
                candidate = f"{base_slug}-{parent_slug}"
                if candidate not in result:
                    result[candidate] = (e[0], e[4])
                    continue
            result[f"{base_slug}-{e[0].lower()}"] = (e[0], e[4])

    return result


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
    rows = load_areas(conn, division_path, area_path)

    print("Deduplicating:")
    entries = dedup_by_wikidata(rows, qrank, args.qrank_threshold)

    print("Resolving name collisions:")
    slug_map = resolve_collisions(entries, qrank)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def process_entry(slug, geom_wkb):
        geom = shapely.from_wkb(geom_wkb)
        minx, miny, maxx, maxy = geom.bounds
        write_if_changed(output_dir / f"{slug}.bbox", f"{minx},{miny},{maxx},{maxy}\n")
        gjcount = 0
        warnings = []
        for label, target in SIMPLIFY_TARGETS.items():
            try:
                simplified = simplify_to_target(geom, target)
                if simplified.is_empty:
                    warnings.append(f"{slug}.{label}.geojson simplified to empty, skipping")
                    continue
                write_if_changed(
                    output_dir / f"{slug}.{label}.geojson",
                    shapely.to_geojson(simplified),
                )
                gjcount += 1
            except Exception as e:
                warnings.append(f"{slug}.{label}.geojson failed: {e}")
        return gjcount, warnings

    total = len(slug_map)
    geojson_count = 0
    done = 0
    lock = threading.Lock()
    slugs = list(slug_map.keys())
    print(f"Writing {total} entries:")

    with ThreadPoolExecutor() as pool:
        futures = {
            pool.submit(process_entry, slug, geom_wkb): slug
            for slug, (wikidata_id, geom_wkb) in slug_map.items()
        }
        for future in as_completed(futures):
            gjcount, warnings = future.result()
            with lock:
                done += 1
                geojson_count += gjcount
                pct = done * 100 // total
                print(f"\r  [{pct:3d}%] {done}/{total}", end="", flush=True)
                for w in warnings:
                    print(f"\n  Warning: {w}", end="")
    print()

    slugs.sort()
    write_if_changed(output_dir / "index.csv", "\n".join(slugs) + "\n")
    shutil.copy(Path(__file__).parent / "index.html", output_dir / "index.html")
    print(f"Wrote {total} .bbox + {geojson_count} .geojson files + index to {output_dir}/")


def cmd_upload(args):
    """Upload output files to R2."""
    endpoint = ["--endpoint-url", args.endpoint_url]

    # Sync data files to versioned prefix, skipping unchanged files
    sync_cmd = [
        "aws", "s3", "sync",
        args.output_dir,
        f"s3://{args.bucket}/{args.version}/",
        "--exclude", "index.html",
        "--delete",
        *endpoint,
    ]
    print(f"Running: {' '.join(sync_cmd)}")
    rc = subprocess.call(sync_cmd)
    if rc != 0:
        sys.exit(rc)

    # Write index.html to bucket root with BASE pointing to the version
    index_src = Path(args.output_dir) / "index.html"
    index_content = index_src.read_text().replace(
        "const BASE = '.';", f"const BASE = '{args.version}';"
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False) as f:
        f.write(index_content)
        tmp_path = f.name
    cp_cmd = [
        "aws", "s3", "cp", tmp_path,
        f"s3://{args.bucket}/index.html",
        "--content-type", "text/html",
        *endpoint,
    ]
    print(f"Running: {' '.join(cp_cmd)}")
    rc = subprocess.call(cp_cmd)
    Path(tmp_path).unlink()
    sys.exit(rc)


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
