#!/usr/bin/env python3
"""Check cache directory health and statistics.

Usage:
    uv run -p 3.13 python docs/skills/dsm-fcp-monitor/scripts/cache_health.py
    uv run -p 3.13 python docs/skills/dsm-fcp-monitor/scripts/cache_health.py --verbose
"""

import argparse
from datetime import datetime
from pathlib import Path


def get_cache_base() -> Path:
    """Get the cache base directory."""
    return Path.home() / ".cache" / "data_source_manager"


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def check_directory_health(path: Path, verbose: bool = False) -> dict:
    """Check health of a cache directory."""
    if not path.exists():
        return {"exists": False, "files": 0, "size": 0}

    arrow_files = list(path.rglob("*.arrow"))
    total_size = sum(f.stat().st_size for f in arrow_files)

    # Get date range from filenames
    dates = []
    for f in arrow_files:
        try:
            date_str = f.stem  # e.g., "2024-01-15"
            dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
        except ValueError:
            pass

    oldest = min(dates) if dates else None
    newest = max(dates) if dates else None

    return {
        "exists": True,
        "files": len(arrow_files),
        "size": total_size,
        "size_human": format_size(total_size),
        "oldest": oldest,
        "newest": newest,
    }


def main() -> None:
    """Check cache health."""
    parser = argparse.ArgumentParser(description="Check DSM cache health")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    cache_base = get_cache_base()
    print(f"Cache Directory: {cache_base}")
    print("=" * 60)

    if not cache_base.exists():
        print("\n❌ Cache directory does not exist")
        print("   This is normal if you haven't fetched any data yet.")
        return

    # Check each provider/market combination
    total_files = 0
    total_size = 0

    for provider_dir in cache_base.iterdir():
        if not provider_dir.is_dir():
            continue

        print(f"\n📁 Provider: {provider_dir.name}")

        for market_dir in provider_dir.iterdir():
            if not market_dir.is_dir():
                continue

            klines_dir = market_dir / "klines" / "daily"
            if not klines_dir.exists():
                continue

            print(f"   Market: {market_dir.name}")

            # Count symbols
            symbol_dirs = [d for d in klines_dir.iterdir() if d.is_dir()]

            for symbol_dir in sorted(symbol_dirs):
                health = check_directory_health(symbol_dir)
                total_files += health["files"]
                total_size += health["size"]

                if args.verbose and health["files"] > 0:
                    date_range = ""
                    if health["oldest"] and health["newest"]:
                        date_range = f" ({health['oldest'].strftime('%Y-%m-%d')} - {health['newest'].strftime('%Y-%m-%d')})"
                    print(f"      {symbol_dir.name}: {health['files']} files, {health['size_human']}{date_range}")

            if not args.verbose:
                symbol_count = len(symbol_dirs)
                print(f"      {symbol_count} symbols cached")

    print("\n" + "=" * 60)
    print(f"Total: {total_files} files, {format_size(total_size)}")

    # Health assessment
    print("\n📊 Health Assessment:")
    if total_files == 0:
        print("   ⚠️  Cache is empty - consider warming up with common symbols")
    elif total_size < 10 * 1024 * 1024:  # < 10MB
        print("   ⚠️  Cache is small - may need more data for efficient FCP")
    else:
        print("   ✅ Cache appears healthy")

    # Check permissions
    if cache_base.exists():
        try:
            test_file = cache_base / ".health_check"
            test_file.touch()
            test_file.unlink()
            print("   ✅ Write permissions OK")
        except PermissionError:
            print("   ❌ Cannot write to cache directory - check permissions")


if __name__ == "__main__":
    main()
