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
import json
import os
import sys
from datetime import datetime

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

        print(f"  Commands: U(set/download URL)  R(remove URL)  B(back)  Q(quit)")
        cmd = input("  > ").strip().lower()

        if cmd == "q":
            sys.exit(0)
        elif cmd == "b":
            return
        elif cmd == "u":
            edit_url(data, igdb_id_str, versions, db_path)
        elif cmd == "r":
            remove_url(data, igdb_id_str, versions, db_path)
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
