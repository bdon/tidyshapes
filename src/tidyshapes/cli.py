"""Command-line interface for tidyshapes."""

import argparse
import gzip
import re
import subprocess
import sys
import unicodedata
import urllib.request
from collections import defaultdict
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

INDEX_HTML = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>tidyshapes</title>
<link rel="stylesheet" href="https://unpkg.com/maplibre-gl/dist/maplibre-gl.css">
<script src="https://unpkg.com/maplibre-gl/dist/maplibre-gl.js"></script>
<style>
body { margin: 0; font-family: system-ui, sans-serif; }
#map { position: absolute; top: 0; bottom: 0; width: 100%; }
#search { position: absolute; top: 10px; left: 10px; z-index: 1; }
#q { padding: 8px 12px; width: 300px; font-size: 16px; border: 1px solid #ccc;
     border-radius: 4px; box-shadow: 0 1px 4px rgba(0,0,0,0.2); }
#results { background: white; max-height: 300px; overflow-y: auto; width: 324px;
           border-radius: 0 0 4px 4px; box-shadow: 0 2px 4px rgba(0,0,0,0.15); }
#results div { padding: 6px 12px; cursor: pointer; font-size: 14px; }
#results div:hover { background: #f0f0f0; }
</style>
</head>
<body>
<div id="search">
  <input type="text" id="q" placeholder="Search places..." autocomplete="off">
  <div id="results"></div>
</div>
<div id="map"></div>
<script>
let slugs = [];
const map = new maplibregl.Map({
  container: 'map',
  style: 'https://demotiles.maplibre.org/style.json',
  center: [0, 20], zoom: 2
});
map.on('load', () => {
  map.addSource('bbox', {type:'geojson', data:{type:'FeatureCollection', features:[]}});
  map.addLayer({id:'bbox-fill', type:'fill', source:'bbox',
    paint:{'fill-color':'#0080ff','fill-opacity':0.15}});
  map.addLayer({id:'bbox-line', type:'line', source:'bbox',
    paint:{'line-color':'#0080ff','line-width':2}});
});
fetch('index.csv').then(r => r.text()).then(t => { slugs = t.trim().split('\\n'); });
const input = document.getElementById('q');
const results = document.getElementById('results');
input.addEventListener('input', () => {
  const q = input.value.toLowerCase();
  results.innerHTML = '';
  if (!q) return;
  const matches = slugs.filter(s => s.startsWith(q)).slice(0, 20);
  for (const slug of matches) {
    const div = document.createElement('div');
    div.textContent = slug;
    div.onclick = () => select(slug);
    results.appendChild(div);
  }
});
async function select(slug) {
  input.value = slug;
  results.innerHTML = '';
  const text = await fetch(slug + '.bbox').then(r => r.text());
  const [minx, miny, maxx, maxy] = text.trim().split(',').map(Number);
  map.getSource('bbox').setData({type:'Feature', geometry:{type:'Polygon',
    coordinates:[[[minx,miny],[maxx,miny],[maxx,maxy],[minx,maxy],[minx,miny]]]}});
  map.fitBounds([[minx, miny], [maxx, maxy]], {padding: 50});
}
</script>
</body>
</html>
"""


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


def resolve_collisions(entries):
    """Assign unique slugs, disambiguating collisions by subtype then parent name."""
    by_slug = defaultdict(list)
    for e in entries:
        slug = slugify(e[2])  # en_name
        if slug:
            by_slug[slug].append(e)

    result = {}  # slug -> (wikidata_id, geom_wkb)
    for base_slug, group in by_slug.items():
        if len(group) == 1:
            e = group[0]
            result[base_slug] = (e[0], e[4])
            continue

        # Try disambiguating by subtype
        subtypes = {e[1] for e in group}
        if len(subtypes) == len(group):
            for e in group:
                label = SUBTYPE_LABELS.get(e[1], e[1])
                result[f"{base_slug}-{label}"] = (e[0], e[4])
            continue

        # Same subtype or mixed — append parent name
        attempted = {}
        for e in group:
            parent_slug = slugify(e[3]) if e[3] else ""
            if parent_slug:
                candidate = f"{base_slug}-{parent_slug}"
            else:
                candidate = f"{base_slug}-{e[0].lower()}"  # QID fallback
            # If parent also collides, fall back to QID
            if candidate in attempted or candidate in result:
                candidate = f"{base_slug}-{e[0].lower()}"
            attempted[candidate] = (e[0], e[4])
        result.update(attempted)

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
    slug_map = resolve_collisions(entries)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    slugs = []
    for slug, (wikidata_id, geom_wkb) in slug_map.items():
        geom = shapely.from_wkb(geom_wkb)
        minx, miny, maxx, maxy = geom.bounds
        bbox_path = output_dir / f"{slug}.bbox"
        bbox_path.write_text(f"{minx},{miny},{maxx},{maxy}\n")
        slugs.append(slug)
        count += 1

    slugs.sort()
    (output_dir / "index.csv").write_text("\n".join(slugs) + "\n")
    (output_dir / "index.html").write_text(INDEX_HTML)
    print(f"Wrote {count} .bbox files + index.csv + index.html to {output_dir}/")


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
