"""
Convert existing Metro (and any other store) Parquet files to CSV.
Usage:  python tools/parquet_to_csv.py               # converts all in data/raw/
        python tools/parquet_to_csv.py --store metro  # only metro files
        python tools/parquet_to_csv.py path/to/file.parquet
"""
import sys
import argparse
from pathlib import Path

def convert(path: Path, overwrite: bool = False) -> bool:
    csv_path = path.with_suffix(".csv")
    if csv_path.exists() and not overwrite:
        print(f"  SKIP (already exists): {csv_path.name}")
        return False
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"  OK  {path.name}  ->  {csv_path.name}  ({len(df):,} rows)")
        return True
    except Exception as exc:
        print(f"  ERR {path.name}: {exc}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Convert Parquet files to CSV")
    parser.add_argument("files", nargs="*", help="Specific .parquet files to convert")
    parser.add_argument("--store", help="Filter by store name prefix (e.g. metro)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing CSVs")
    parser.add_argument("--dir", default=None, help="Directory to scan (default: data/raw/)")
    args = parser.parse_args()

    # Specific files given
    if args.files:
        paths = [Path(f) for f in args.files]
    else:
        raw_dir = Path(args.dir) if args.dir else Path(__file__).parent.parent / "data" / "raw"
        pattern = f"{args.store}*.parquet" if args.store else "*.parquet"
        paths   = sorted(raw_dir.glob(pattern))

    if not paths:
        print("No .parquet files found.")
        return

    print(f"Converting {len(paths)} file(s):")
    done = sum(convert(p, overwrite=args.overwrite) for p in paths)
    print(f"\nDone: {done}/{len(paths)} converted.")

if __name__ == "__main__":
    main()
