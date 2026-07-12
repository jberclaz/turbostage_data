#!/usr/bin/env python3
"""
Interactive tool to download a game archive, verify it runs in DOSBox,
and add it to the TurboStage game database.

Usage:
    pip install -e ../turbostage
    python add_game.py --url <download_url> [--igdb-id <id>] [--name <name>] [--dosbox <path>] [--db <path>]
"""

import argparse
import os
import shutil
import sys
import tempfile
import zipfile

# Load/save helpers from sibling db_editor
from db_editor import load, save

import requests
from turbostage.dosbox_runner import run_dosbox
from turbostage.utils import compute_hash_for_largest_files_in_zip, compute_md5_from_zip


def prompt_choices(items: list[str], label: str, allow_none: bool = True) -> str | None:
    """Show numbered list and let the user pick one."""
    print(f"\n  {label}:")
    for i, item in enumerate(items, 1):
        print(f"    {i:4d}. {item}")
    while True:
        choice = input(f"  Enter number (0=none{', Enter=skip' if allow_none else ''}): ").strip()
        if allow_none and (choice == "" or choice == "0"):
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(items):
            return items[int(choice) - 1]
        print(f"  Invalid choice (1-{len(items)}).")


def prompt_multiline(prompt_text: str) -> str:
    """Read multi-line input until '.' on its own line or blank line."""
    print(f"  {prompt_text} (end with '.' on its own line):")
    lines = []
    while True:
        line = input("    ")
        if line == ".":
            break
        lines.append(line)
    return "\n".join(lines).strip()


def main():
    parser = argparse.ArgumentParser(description="Download, verify, and add a game to the database")
    parser.add_argument("--url", required=True, help="Download URL for the game archive")
    parser.add_argument("--igdb-id", type=int, help="IGDB ID (optional, will prompt if not provided)")
    parser.add_argument("--name", help="Game name (optional, used if no IGDB name found)")
    parser.add_argument("--dosbox", help="Path to DOSBox Staging binary (default: auto-detect)")
    parser.add_argument("--db", default="archive/database.json.gz", help="Path to database.json[.gz]")
    args = parser.parse_args()

    # ------------------------------------------------------------------ #
    # 1. Load or create database
    # ------------------------------------------------------------------ #
    db_path = args.db
    if os.path.exists(db_path):
        data = load(db_path)
    else:
        data = {"generated_at": "", "games": {}}

    games = data.setdefault("games", {})

    # ------------------------------------------------------------------ #
    # 2. Game identity — look up IGDB ID by name if not given
    # ------------------------------------------------------------------ #
    igdb_id = args.igdb_id
    game_name = args.name

    if igdb_id is None:
        # Try to look up by name
        if not game_name:
            game_name = input("  Game name (Enter to search later): ").strip()

        if game_name:
            try:
                from turbostage.igdb_client import IgdbClient

                client = IgdbClient()
                results = client.search_games(game_name)
                if results:
                    print(f"\n  IGDB results for '{game_name}':")
                    for i, g in enumerate(results, 1):
                        print(f"    {i:4d}. [{g['id']}] {g['name']}")
                    choice = input(f"  Pick a match (Enter to skip, 0 to skip): ").strip()
                    if choice.isdigit() and 1 <= int(choice) <= len(results):
                        igdb_id = results[int(choice) - 1]["id"]
                        game_name = results[int(choice) - 1]["name"]
                        print(f"  Selected: [{igdb_id}] {game_name}")
            except Exception as e:
                print(f"  IGDB lookup failed: {e}")

        # Fallback: manual ID entry
        if igdb_id is None:
            igdb_raw = input("  IGDB ID (Enter to skip): ").strip()
            igdb_id = int(igdb_raw) if igdb_raw.isdigit() else None

    igdb_key = str(igdb_id) if igdb_id else None

    if igdb_key and igdb_key in games:
        existing = games[igdb_key]
        ver_keys = list(existing.get("versions", {}).keys())
        print(f"\n  Game {igdb_key} already exists in DB with versions: {ver_keys}")
        overwrite = input("  Add a new version to this game? [y/N]: ").strip().lower()
        if overwrite != "y":
            print("  Aborted.")
            sys.exit(0)
    elif igdb_key is None:
        # No IGDB ID — need a name
        if not game_name:
            game_name = input("  Game name: ").strip()
        if not game_name:
            print("  A game name is required when no IGDB ID is provided.")
            sys.exit(1)

    # ------------------------------------------------------------------ #
    # 3. Download
    # ------------------------------------------------------------------ #
    print(f"\n  Downloading {args.url} ...")
    try:
        r = requests.get(args.url, stream=True, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  FAILED: {e}")
        sys.exit(1)

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        for chunk in r.iter_content(chunk_size=65536):
            tmp.write(chunk)
        archive_path = tmp.name

    print(f"  Downloaded to {archive_path}")

    # ------------------------------------------------------------------ #
    # 4. Extract to temp directory
    # ------------------------------------------------------------------ #
    extract_dir = tempfile.mkdtemp(prefix="turbostage-verify-")
    print(f"  Extracting to {extract_dir} ...")
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)
    except Exception as e:
        print(f"  FAILED to extract: {e}")
        shutil.rmtree(extract_dir)
        os.unlink(archive_path)
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 5. List executables
    # ------------------------------------------------------------------ #
    executables = []
    for root, _, files in os.walk(extract_dir):
        for f in files:
            if os.path.splitext(f)[1].lower() in (".exe", ".bat", ".com"):
                rel = os.path.relpath(os.path.join(root, f), extract_dir)
                executables.append(rel)
    executables.sort()

    if not executables:
        print("  No executables (.exe/.bat/.com) found in the archive!")
        shutil.rmtree(extract_dir)
        os.unlink(archive_path)
        sys.exit(1)

    print(f"\n  Found {len(executables)} executable(s).")

    # ------------------------------------------------------------------ #
    # 6. Pick executable and config_executable
    # ------------------------------------------------------------------ #
    executable = prompt_choices(executables, "Select the main game executable", allow_none=False)
    if executable is None and len(executables) == 1:
        executable = executables[0]
    if executable is None:
        print("  No executable selected. Aborting.")
        shutil.rmtree(extract_dir)
        os.unlink(archive_path)
        sys.exit(1)

    config_executable = prompt_choices(executables, "Select the setup/config executable (optional)")

    # ------------------------------------------------------------------ #
    # 7. Run DOSBox to verify
    # ------------------------------------------------------------------ #
    dosbox_path = args.dosbox
    if not dosbox_path:
        dosbox_path = shutil.which("dosbox")
    if not dosbox_path:
        dosbox_path = input("  DOSBox Staging binary path: ").strip()
    if not dosbox_path or not os.path.isfile(dosbox_path):
        print(f"  DOSBox binary not found at '{dosbox_path}'.")
        shutil.rmtree(extract_dir)
        os.unlink(archive_path)
        sys.exit(1)

    exe_full = os.path.join(extract_dir, executable)
    print(f"\n  Running DOSBox with: {executable}")
    print("  Test the game, then close DOSBox.")
    try:
        run_dosbox(dosbox_path, exe_full)
    except Exception as e:
        print(f"  DOSBox exited with error: {e}")
        ok = input("  Continue anyway? [y/N]: ").strip().lower()
        if ok != "y":
            shutil.rmtree(extract_dir)
            os.unlink(archive_path)
            sys.exit(1)

    worked = input("\n  Did the game run correctly? [y/N]: ").strip().lower()
    if worked != "y":
        print("  Game not verified. Entry will NOT be added to the database.")
        # Still give option to save partial data
        force = input("  Add to database anyway? [y/N]: ").strip().lower()
        if force != "y":
            shutil.rmtree(extract_dir)
            os.unlink(archive_path)
            sys.exit(1)

    # ------------------------------------------------------------------ #
    # 8. Compute hashes
    # ------------------------------------------------------------------ #
    print("\n  Computing file hashes ...")
    try:
        hashes = compute_hash_for_largest_files_in_zip(archive_path, n=4)
        hash_dict = {f: h for f, _, h in hashes}

        # Also hash the executable if not already covered
        if executable not in hash_dict:
            zf = zipfile.ZipFile(archive_path, "r")
            h = compute_md5_from_zip(zf, executable)
            zf.close()
            hash_dict[executable] = h
    except Exception as e:
        print(f"  FAILED to compute hashes: {e}")
        shutil.rmtree(extract_dir)
        os.unlink(archive_path)
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 9. Additional metadata
    # ------------------------------------------------------------------ #
    print()
    cycles_raw = input("  CPU cycles (Enter for auto, 0 for auto): ").strip()
    cycles = int(cycles_raw) if cycles_raw.isdigit() else 0

    config_text = prompt_multiline("Custom DOSBox config (optional)")

    version_name = input("\n  Version name (Enter for 'default'): ").strip() or "default"

    # ------------------------------------------------------------------ #
    # 10. Add to database
    # ------------------------------------------------------------------ #
    if igdb_key is None:
        # No IGDB ID — create a placeholder key
        igdb_key = f"_{game_name.lower().replace(' ', '_')}"

    game_entry = games.setdefault(igdb_key, {"versions": {}})
    if not game_name:
        # Try to find an existing name, or use a placeholder
        game_name = f"Game {igdb_key}"

    version_entry = {
        "executable": executable,
        "config_executable": config_executable,
        "config": config_text or None,
        "cycles": cycles,
        "hashes": hash_dict,
        "download_url": args.url,
    }

    game_entry["versions"][version_name] = version_entry

    save(db_path, data)

    print(f"\n  Added game '{game_name}' (ID: {igdb_key}, version: '{version_name}') to {db_path}")

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #
    shutil.rmtree(extract_dir)
    os.unlink(archive_path)


if __name__ == "__main__":
    main()
