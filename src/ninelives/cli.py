"""Command-line inspection and maintenance tools."""

from __future__ import annotations

import argparse
import json
import re

from .cache import clear_cache, default_cache_dir
from .storage import read_entry


def _parse_size(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?i?b?|)\s*", value, re.IGNORECASE)
    if match is None:
        raise argparse.ArgumentTypeError(
            "size must be a non-negative number with optional KB/MB/GB/TB units"
        )
    number, unit = match.groups()
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "ki": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mi": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gi": 1024**3,
        "gib": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
        "ti": 1024**4,
        "tib": 1024**4,
    }
    return int(float(number) * multipliers[unit.lower()])


def main() -> None:
    parser = argparse.ArgumentParser(prog="ninelives")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ls")
    inspect_p = sub.add_parser("inspect")
    inspect_p.add_argument("key")
    clear_p = sub.add_parser("clear")
    clear_p.add_argument("--older-than")
    clear_p.add_argument("--tag")
    gc = sub.add_parser("gc")
    gc.add_argument("--older-than", default="30d")
    gc.add_argument("--max-size", type=_parse_size, default=None)
    gc.add_argument("--tag")
    sub.add_parser("verify")
    args = parser.parse_args()
    root = default_cache_dir()
    entries = list(root.rglob("*.nlv")) if root.exists() else []
    if args.command == "ls":
        for p in entries:
            print(p.stem, p.stat().st_size, p)
    elif args.command == "inspect":
        found = next((p for p in entries if p.stem == args.key), None)
        if found is None:
            parser.error("key not found")
        meta, payload = read_entry(found)
        print(json.dumps({**meta, "payload_bytes": len(payload)}, indent=2))
    elif args.command in {"clear", "gc"}:
        removed = clear_cache(
            older_than=args.older_than,
            tag=getattr(args, "tag", None),
            max_size=getattr(args, "max_size", None),
            cache_dir=root,
        )
        print(f"removed {removed} entries")
    elif args.command == "verify":
        bad = 0
        for p in entries:
            try:
                read_entry(p)
            except Exception as exc:  # noqa: BLE001
                bad += 1
                print(f"INVALID {p}: {exc}")
        print(f"verified {len(entries) - bad}/{len(entries)} entries")
        if bad:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
