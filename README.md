# TurboStage Data Tools

Tools for managing the `database.json[.gz]` that feeds TurboStage's downloadable-games feature.

## Setup

From this directory:

```bash
../turbostage/venv/bin/pip install -e ../turbostage --no-deps
../turbostage/venv/bin/pip install requests
```

This makes `turbostage.utils` and `turbostage.dosbox_runner` available to scripts here.

## Tools

### `db_editor.py` — Browse and edit the database

```bash
../turbostage/venv/bin/python db_editor.py [path/to/database.json]
```

TUI viewer/editor for game entries, URLs, and hashes.

| Key | Action |
|-----|--------|
| `U` | Set or edit download URL for a version |
| `R` | Remove download URL |
| `V` | Verify hashes against the actual archive |
| `H` | Regenerate hashes from the archive |
| `E` | Pick executable and config_executable from the archive's file list |

### `add_game.py` — Download, verify, and add a new game

```bash
../turbostage/venv/bin/python add_game.py --url <download_url> [options]
```

Interactive wizard that:

1. Downloads the archive
2. Extracts it and lists all executables
3. Lets you pick the main executable and optional setup executable
4. Computes hashes (top 4 largest files + executable)
5. Runs DOSBox so you can verify the game works
6. Prompts for CPU cycles, custom config, and version name
7. Saves the entry to `database.json[.gz]`

Options:

| Flag | Description |
|------|-------------|
| `--url` | (Required) Download URL |
| `--igdb-id` | IGDB game ID (optional — prompted if missing) |
| `--name` | Game name (optional, used when no IGDB ID) |
| `--dosbox` | Path to DOSBox Staging binary (auto-detected if omitted) |
| `--db` | Path to database file (default: `archive/database.json.gz`) |
