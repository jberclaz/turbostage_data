#!/usr/bin/env python3
"""
TurboStage Game Database Editor

TUI viewer/editor for archive/database.json[.gz].
Allows browsing games and adding/editing download URLs.
Looks up game names via IGDB when possible.

Usage:
    python db_editor.py [path/to/database.json] [path/to/database.json.gz]
"""

import gzip
import hashlib
import json
import os
import sys
import tempfile
import time
import zipfile
from datetime import datetime

CACHE_DIR = "/tmp/turbostage-db-editor-cache"

PAGE_SIZE = 15

# --------------------------------------------------------------------------- #
# IGDB name lookup — optional; falls back silently to showing just IDs
# --------------------------------------------------------------------------- #

_GAME_NAMES: dict[str, str] = {}


def _fetch_game_names(game_ids: list[str]):
    """Try to fetch names from IGDB and populate _GAME_NAMES cache."""
    global _GAME_NAMES
    try:
        import requests

        # Reuse credentials from the sibling turbostage project
        _script_dir = os.path.dirname(os.path.abspath(__file__))
        _constants_path = os.path.join(_script_dir, "..", "turbostage", "turbostage", "constants.py")
        if not os.path.exists(_constants_path):
            return
        ns = {"IGDB_CLIENT_ID": None, "IGDB_CLIENT_SECRET": None, "IGDB_DOS_PLATFORM_ID": None}
        with open(_constants_path) as f:
            code = f.read()
        exec(code, ns)

        cid = ns["IGDB_CLIENT_ID"]
        secret = ns["IGDB_CLIENT_SECRET"]
        if not cid or not secret:
            return

        # Authenticate
        r = requests.post(
            "https://id.twitch.tv/oauth2/token",
            params={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
        )
        r.raise_for_status()
        token = r.json()["access_token"]

        # Query names
        headers = {"Client-ID": cid, "Authorization": f"Bearer {token}"}
        body = f"fields name; where id = ({','.join(game_ids)}); limit 500;"
        r = requests.post("https://api.igdb.com/v4/games", data=body, headers=headers)
        r.raise_for_status()
        _GAME_NAMES.update({str(g["id"]): g["name"] for g in r.json()})
    except Exception:
        pass  # IGDB unavailable — stick with IDs only


def game_label(igdb_id: str) -> str:
    """Return 'Name (id)' or just 'id' if name lookup failed."""
    name = _GAME_NAMES.get(igdb_id)
    return f"{name} ({igdb_id})" if name else igdb_id


def load(path: str) -> dict:
    if path.endswith(".gz"):
        with gzip.open(path, "rt") as f:
            return json.load(f)
    with open(path) as f:
        return json.load(f)


def save(path: str, data: dict):
    data["generated_at"] = datetime.now().isoformat()
    if path.endswith(".gz"):
        with gzip.open(path, "wt") as f:
            json.dump(data, f, separators=(",", ":"))
    else:
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
    print(f"\nSaved: {path}")


def list_games(data: dict) -> list[tuple[str, dict]]:
    """Return sorted list of (igdb_id_str, game_data)."""
    games = data.get("games", {})
    return sorted(games.items(), key=lambda kv: int(kv[0]))


def has_url(version: dict) -> bool:
    url = version.get("download_url")
    return bool(url and url.strip())


def show_list(data: dict, db_path: str):
    games = list_games(data)
    total = len(games)
    # Kick off name lookup (non-blocking enough for a TUI)
    _fetch_game_names([g[0] for g in games])
    page = 0
    max_page = (total - 1) // PAGE_SIZE if total else 0

    while True:
        os.system("clear" if os.name == "posix" else "cls")
        start = page * PAGE_SIZE
        end = min(start + PAGE_SIZE, total)

        print("=" * 60)
        print("  TurboStage Game Database Editor")
        print(f"  {db_path} ({total} games)")
        print("=" * 60)
        print()

        for i in range(start, end):
            igdb_id, game = games[i]
            versions = game.get("versions", {})
            # Show URL indicator for any version that has one
            url_mark = "URL" if any(has_url(v) for v in versions.values()) else " -"
            ver_names = ", ".join(versions.keys())
            label = game_label(igdb_id)
            print(f"  {i+1:3d}. [{url_mark}] {label}  ({ver_names})")

        print()
        print(f"  Page {page+1}/{max_page+1}  (games {start+1}-{end} of {total})")
        print()
        cmd = input("  [#=select  N=next  P=prev  Q=quit] > ").strip().lower()

        if cmd == "q":
            break
        elif cmd == "n" and page < max_page:
            page += 1
        elif cmd == "p" and page > 0:
            page -= 1
        elif cmd.isdigit():
            idx = int(cmd) - 1
            if 0 <= idx < total:
                show_game(data, games[idx], db_path)
            else:
                input(f"  Invalid number (1-{total}). Press Enter...")
        elif cmd == "":
            continue
        else:
            input("  Unknown command. Press Enter...")


def show_game(data: dict, entry: tuple[str, dict], db_path: str):
    igdb_id_str, game = entry

    while True:
        os.system("clear" if os.name == "posix" else "cls")
        print("=" * 60)
        print(f"  {game_label(igdb_id_str)}")
        print("=" * 60)
        print()

        versions = game.get("versions", {})
        for ver_name, ver in versions.items():
            print(f"  Version: {ver_name}")
            print(f"    executable       : {ver.get('executable', 'N/A')}")
            print(f"    config_executable: {ver.get('config_executable', 'N/A')}")
            print(f"    cycles           : {ver.get('cycles', 0)}")
            url = ver.get("download_url", "")
            if url:
                print(f"    download_url     : {url}")
            else:
                print(f"    download_url     : (not set)")
            print()
            hashes = ver.get("hashes", {})
            if hashes:
                print(f"    Hashes ({len(hashes)} files):")
                for fname, h in list(hashes.items())[:8]:
                    print(f"      {fname}: {h[:16]}...")
                if len(hashes) > 8:
                    print(f"      ... and {len(hashes)-8} more")
            print()

        has_urls = any(has_url(v) for v in versions.values())
        print(f"  Commands: U(set/download URL)  R(remove URL)  V(verify hashes)  H(regen hashes)  E(edit exe paths)  B(back)  Q(quit)")
        cmd = input("  > ").strip().lower()

        if cmd == "q":
            sys.exit(0)
        elif cmd == "b":
            return
        elif cmd == "u":
            edit_url(data, igdb_id_str, versions, db_path)
        elif cmd == "r":
            remove_url(data, igdb_id_str, versions, db_path)
        elif cmd == "v":
            verify_hashes(igdb_id_str, versions)
        elif cmd == "h":
            regen_hashes(data, igdb_id_str, versions, db_path)
        elif cmd == "e":
            edit_executables(data, igdb_id_str, versions, db_path)
        else:
            print("  Unknown command.")
            input("  Press Enter...")


def edit_url(data: dict, igdb_id_str: str, versions: dict, db_path: str):
    # Pick which version to edit (usually just "default")
    ver_names = list(versions.keys())
    if len(ver_names) == 1:
        ver_name = ver_names[0]
    else:
        print(f"  Versions: {', '.join(f'{i+1}. {n}' for i, n in enumerate(ver_names))}")
        choice = input("  Select version number: ").strip()
        if not choice.isdigit() or int(choice) < 1 or int(choice) > len(ver_names):
            input("  Invalid selection. Press Enter...")
            return
        ver_name = ver_names[int(choice) - 1]

    ver = versions[ver_name]
    current = ver.get("download_url", "")
    print(f"  Current URL: {current or '(not set)'}")
    new_url = input("  New URL (Enter to keep, blank to clear): ").strip()

    if new_url == "" and current == "":
        input("  No change. Press Enter...")
        return
    elif new_url == "":
        # User hit Enter with no new URL but current existed — keep it, or remove if explicit
        ver.pop("download_url", None)
        save(db_path, data)
        print(f"  Removed download_url for game {igdb_id_str}.")
    elif new_url == current:
        input("  URL unchanged. Press Enter...")
        return
    else:
        ver["download_url"] = new_url
        save(db_path, data)
        print(f"  Set download_url for game {igdb_id_str}.")

    input("  Press Enter...")


def remove_url(data: dict, igdb_id_str: str, versions: dict, db_path: str):
    removed = 0
    for ver in versions.values():
        if "download_url" in ver:
            del ver["download_url"]
            removed += 1

    if removed:
        save(db_path, data)
        print(f"  Removed download_url from {removed} version(s) of game {igdb_id_str}.")
    else:
        print("  No download URLs to remove for this game.")
    input("  Press Enter...")


def edit_executables(data: dict, igdb_id_str: str, versions: dict, db_path: str):
    """Download the archive, list its files, and let the user pick
    executable and config_executable paths."""
    import requests

    ver_names = list(versions.keys())
    if len(ver_names) == 1:
        ver_name = ver_names[0]
    else:
        print(f"  Versions: {', '.join(f'{i+1}. {n}' for i, n in enumerate(ver_names))}")
        choice = input("  Select version number: ").strip()
        if not choice.isdigit() or int(choice) < 1 or int(choice) > len(ver_names):
            input("  Invalid selection. Press Enter...")
            return
        ver_name = ver_names[int(choice) - 1]

    ver = versions[ver_name]
    url = ver.get("download_url", "")
    if not url:
        print(f"  Version '{ver_name}' has no download URL.")
        input("  Press Enter...")
        return

    print(f"  Downloading {url} ...")
    try:
        tmp_path = _download_archive(url)
    except Exception as e:
        print(f"    FAILED to download: {e}")
        input("  Press Enter...")
        return

    try:
        with zipfile.ZipFile(tmp_path, "r") as zf:
            files = [info.filename for info in zf.infolist() if not info.is_dir()]
            files.sort()

        print(f"\n  Files in archive ({len(files)} total):")
        for i, fname in enumerate(files, 1):
            print(f"    {i:4d}. {fname}")

        current_exe = ver.get("executable", "")
        current_cfg = ver.get("config_executable", "")

        print()
        print(f"  Current executable       : {current_exe or '(not set)'}")
        print(f"  Current config_executable: {current_cfg or '(not set)'}")

        # Pick executable
        print()
        exe_choice = input(f"  Executable number (Enter to keep, 0 to clear): ").strip()
        if exe_choice == "0":
            ver["executable"] = ""
            print("  Cleared executable.")
        elif exe_choice.isdigit() and 1 <= int(exe_choice) <= len(files):
            ver["executable"] = files[int(exe_choice) - 1]
            print(f"  Set executable to: {ver['executable']}")

        # Pick config_executable
        cfg_choice = input(f"  Config executable number (Enter to keep, 0 to clear): ").strip()
        if cfg_choice == "0":
            ver["config_executable"] = ""
            print("  Cleared config_executable.")
        elif cfg_choice.isdigit() and 1 <= int(cfg_choice) <= len(files):
            ver["config_executable"] = files[int(cfg_choice) - 1]
            print(f"  Set config_executable to: {ver['config_executable']}")

        save(db_path, data)
        print(f"  Updated executables for game {igdb_id_str} version '{ver_name}'.")

    except Exception as e:
        print(f"    FAILED: {e}")

    input("  Press Enter...")


def _download_archive(url: str) -> str:
    """Download a URL to a cached file and return the path.
    Caches in /tmp by URL hash; reuses if less than 1 hour old."""
    import requests

    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_key = hashlib.md5(url.encode()).hexdigest()
    cache_path = os.path.join(CACHE_DIR, cache_key)

    if os.path.exists(cache_path):
        age = time.time() - os.path.getmtime(cache_path)
        if age < 3600:
            print(f"    Using cached {cache_path} ({int(age)}s old)")
            return cache_path
        print(f"    Cache expired ({int(age)}s old), re-downloading...")

    r = requests.get(url, stream=True, timeout=30)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    downloaded = 0
    with open(cache_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
            if total:
                downloaded += len(chunk)
                pct = int(downloaded * 100 / total)
                bar = "#" * (pct // 5) + "." * (20 - pct // 5)
                print(f"    Downloading [{bar}] {pct}%", end="\r")
    if total:
        full_bar = "#" * 20
        print(f"    Downloading [{full_bar}] 100%")
    else:
        print(f"    Downloaded {downloaded} bytes")
    return cache_path


def _compute_md5_from_zip(zf: zipfile.ZipFile, fname: str) -> str:
    """Compute the MD5 hash of a file inside a ZIP archive (matching
    turbostage.utils.compute_md5_from_zip)."""
    h = hashlib.md5()
    with zf.open(fname) as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def regen_hashes(data: dict, igdb_id_str: str, versions: dict, db_path: str):
    """Download the archive, recompute hashes, and update the database.
    Matches turbostage's `add_game_worker` algorithm:
      - hash the top 4 largest files
      - also hash the executable if it's not among them
    """
    import requests

    confirm = input("  This will replace all stored hashes with freshly computed ones. Continue? (y/N): ").strip().lower()
    if confirm != "y":
        print("  Cancelled.")
        input("  Press Enter...")
        return

    for ver_name, ver in versions.items():
        url = ver.get("download_url", "")
        if not url:
            print(f"  Version '{ver_name}' has no download URL, skipping.")
            continue

        print(f"\n  Version '{ver_name}':")
        print(f"    Downloading {url} ...")

        try:
            tmp_path = _download_archive(url)
        except Exception as e:
            print(f"    FAILED to download: {e}")
            continue

        print(f"    Downloaded to {tmp_path}")

        try:
            with zipfile.ZipFile(tmp_path, "r") as zf:
                # Collect all non-directory files
                files = [
                    (info.filename, info.file_size)
                    for info in zf.infolist()
                    if not info.is_dir()
                ]
                # Top 4 largest files
                files.sort(key=lambda x: x[1], reverse=True)
                largest = files[:4]

                new_hashes = {}

                # Hash the top 4
                for fname, size in largest:
                    file_hash = _compute_md5_from_zip(zf, fname)
                    new_hashes[fname] = file_hash
                    print(f"    {fname} ({size} bytes): {file_hash}")

                # Also hash the executable if it's not already covered
                executable = ver.get("executable") or ver.get("binary", "")
                if executable and executable not in new_hashes:
                    hash_exe = _compute_md5_from_zip(zf, executable)
                    new_hashes[executable] = hash_exe
                    print(f"    {executable} (executable): {hash_exe}")

            print(f"\n    Computed {len(new_hashes)} hash(es).")
            ver["hashes"] = new_hashes
            save(db_path, data)
            print(f"    Updated hashes for game {igdb_id_str} version '{ver_name}'.")
        except Exception as e:
            print(f"    FAILED: {e}")

    input("  Press Enter...")


def verify_hashes(igdb_id_str: str, versions: dict):
    """Download the archive and verify file hashes match the database.
    Also checks that the archive contains the executable and config_executable."""
    import requests

    for ver_name, ver in versions.items():
        url = ver.get("download_url", "")
        if not url:
            print(f"  Version '{ver_name}' has no download URL, skipping.")
            continue

        db_hashes = ver.get("hashes", {})
        if not db_hashes:
            print(f"  Version '{ver_name}' has no hashes stored, skipping.")
            continue

        print(f"\n  Version '{ver_name}':")
        print(f"    Downloading {url} ...")

        try:
            tmp_path = _download_archive(url)
        except Exception as e:
            print(f"    FAILED to download: {e}")
            continue

        print(f"    Downloaded to {tmp_path}")
        print(f"    Verifying {len(db_hashes)} file hash(es)...")

        try:
            with zipfile.ZipFile(tmp_path, "r") as zf:
                zip_names = zf.namelist()

                matched = 0
                mismatched = 0
                missing = 0

                for fname, expected_hash in db_hashes.items():
                    if fname not in zip_names:
                        # Try matching by basename (some zips strip directory prefixes)
                        basename = os.path.basename(fname)
                        candidates = [n for n in zip_names if os.path.basename(n) == basename]
                        if len(candidates) == 1:
                            fname = candidates[0]
                        else:
                            print(f"    MISSING  {fname}")
                            missing += 1
                            continue

                    actual = _compute_md5_from_zip(zf, fname)
                    if actual == expected_hash:
                        print(f"    OK       {fname}")
                        matched += 1
                    else:
                        print(f"    MISMATCH {fname}")
                        print(f"      expected: {expected_hash}")
                        print(f"      actual:   {actual}")
                        mismatched += 1

                print(f"\n    Result: {matched} ok, {mismatched} mismatched, {missing} missing")

                # Check executable paths exist in archive
                print()
                for label in ("executable", "config_executable"):
                    path = ver.get(label, "")
                    if not path:
                        continue
                    if path in zip_names:
                        print(f"    OK       {label}: {path}")
                    else:
                        basename = os.path.basename(path)
                        candidates = [n for n in zip_names if os.path.basename(n) == basename]
                        if len(candidates) == 1:
                            print(f"    MISMATCH {label}: {path} (basename found as '{candidates[0]}')")
                        elif len(candidates) > 1:
                            print(f"    MISMATCH {label}: {path} (basename matches: {candidates})")
                        else:
                            print(f"    MISSING  {label}: {path}")

                # Show any extra files in the zip not in the DB
                db_fnames = set(db_hashes.keys())
                for fname in list(db_hashes.keys()):
                    candidates = [n for n in zip_names if os.path.basename(n) == os.path.basename(fname)]
                    db_fnames.update(candidates)
                extra = set(zip_names) - db_fnames
                if extra:
                    print(f"\n    Extra files in zip (not in DB): {', '.join(sorted(extra)[:10])}")
                    if len(extra) > 10:
                        print(f"      ... and {len(extra)-10} more")

        except Exception as e:
            print(f"    FAILED to verify archive: {e}")

    input("  Press Enter...")


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "archive/database.json.gz"

    if not os.path.exists(db_path):
        # Fall back to uncompressed
        alt = db_path.replace(".gz", "")
        if os.path.exists(alt):
            db_path = alt
        else:
            print(f"Error: {db_path} not found.")
            sys.exit(1)

    data = load(db_path)
    show_list(data, db_path)


if __name__ == "__main__":
    main()
