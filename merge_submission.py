#!/usr/bin/env python3
"""
merge_submission.py

Merge a user-submitted config JSON into the global TurboStage game database.

Usage:
    python merge_submission.py submission.json /path/to/legal/archives/
"""

import json
import sys
import hashlib
import zipfile
import pathlib
import argparse
from datetime import datetime
from typing import Dict, Any

# --------------------------------------------------------------------------- #
# CONFIG – EDIT THESE PATHS
# --------------------------------------------------------------------------- #
GLOBAL_DB_PATH = pathlib.Path("archive/database.json")   # relative to script
ARCHIVE_ROOT   = pathlib.Path("/path/to/legal/archives")      # <-- SET THIS

# --------------------------------------------------------------------------- #
def load_json(path: pathlib.Path) -> Dict[str, Any]:
    if not path.exists():
        print(f"Error: {path} not found.")
        sys.exit(1)
    return json.loads(path.read_text())

def save_json(path: pathlib.Path, data: Dict[str, Any]):
    path.write_text(json.dumps(data, indent=2) + "\n")
    print(f"Saved: {path}")

def md5_of_file_in_zip(zip_path: pathlib.Path, inner_path: str) -> str:
    with zipfile.ZipFile(zip_path) as z:
        with z.open(inner_path) as f:
            return hashlib.md5(f.read()).hexdigest()

# --------------------------------------------------------------------------- #
def find_legal_archive(archive_name_hint: str) -> pathlib.Path:
    """
    Simple heuristic: look for a .zip in ARCHIVE_ROOT that contains the hint.
    You can make this smarter (e.g. map IGDB ID → known zip).
    """
    candidates = list(ARCHIVE_ROOT.rglob("*.zip"))
    for cand in candidates:
        if archive_name_hint.lower() in cand.name.lower():
            return cand
    return None

# --------------------------------------------------------------------------- #
def verify_submission(submission: Dict[str, Any]) -> bool:
    """Return True if *all* hashes match a legal copy."""
    ok = True
    for igdb_id, game in submission["games"].items():
        for ver_name, ver in game["versions"].items():
            exe = ver.get("executable", "")
            hashes = ver.get("hashes", {})

            # Guess archive from executable path
            dir_part = str(pathlib.Path(exe).parent).replace("\\", "/")
            archive = find_legal_archive(dir_part or "unknown")

            if not archive:
                print(f"Error: No legal archive found for '{exe}' (IGDB {igdb_id})")
                ok = False
                continue

            print(f"Verifying against: {archive.name}")
            for inner_file, expected_hash in hashes.items():
                try:
                    actual = md5_of_file_in_zip(archive, inner_file)
                    if actual != expected_hash:
                        print(f"  MISMATCH: {inner_file} -> {actual} (expected {expected_hash})")
                        ok = False
                    else:
                        print(f"  OK: {inner_file}")
                except KeyError:
                    print(f"  MISSING in archive: {inner_file}")
                    ok = False
    return ok

# --------------------------------------------------------------------------- #
def merge_into_global(global_db: Dict[str, Any], submission: Dict[str, Any]):
    """Merge submission into global DB. Preserves existing data."""
    if "games" not in global_db:
        global_db["games"] = {}

    merged_count = 0
    for igdb_id_str, game in submission["games"].items():
        igdb_id = int(igdb_id_str)
        if igdb_id not in global_db["games"]:
            global_db["games"][igdb_id] = {"versions": {}}

        for ver_name, ver in game["versions"].items():
            if ver_name not in global_db["games"][igdb_id]["versions"]:
                global_db["games"][igdb_id]["versions"][ver_name] = ver
                merged_count += 1
            else:
                # Optional: merge hashes if version exists
                existing = global_db["games"][igdb_id]["versions"][ver_name]
                existing["hashes"].update(ver.get("hashes", {}))

    print(f"Merged {merged_count} new version(s).")


# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(description="Merge TurboStage config submission")
    parser.add_argument("submission", type=pathlib.Path, help="Path to user JSON")
    parser.add_argument("archives", type=pathlib.Path, nargs="?",
                        default=ARCHIVE_ROOT, help="Folder with legal .zip archives")
    parser.add_argument("-v", "--validate", action="store_true", help="validate config")
    args = parser.parse_args()

    print(f"Loading submission: {args.submission}")
    submission = load_json(args.submission)

    if args.validate:
        print("Verifying hashes against legal copies...")
        if not verify_submission(submission):
            print("Verification failed. Aborting.")
            sys.exit(1)

    print("Loading global database...")
    global_db = load_json(GLOBAL_DB_PATH) if GLOBAL_DB_PATH.exists() else {
        "generated_at": datetime.now().isoformat(),
        "games": {}
    }

    print("Merging...")
    merge_into_global(global_db, submission)

    # Update metadata
    global_db["generated_at"] = datetime.now().isoformat()

    save_json(GLOBAL_DB_PATH, global_db)
    print("All done! Push to GitHub.")

if __name__ == "__main__":
    main()