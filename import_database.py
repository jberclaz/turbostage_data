import argparse
import sqlite3
import os

DB_VERSION = "0.5.0"

def initialize_database(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            release_date INTEGER,
            genre TEXT,
            summary TEXT,
            publisher TEXT,
            igdb_id INTEGER,
            cover_url TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            version TEXT,
            executable TEXT,
            archive TEXT,
            config TEXT,
            cycles INTEGER DEFAULT 0
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS hashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            file_name TEXT,
            hash TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS local_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            archive TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS config_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            type INTEGER NOT NULL,
            path TEXT,
            content BLOB
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS db_version (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT NOT NULL
        );
        """
    )

    cursor.execute("""INSERT INTO db_version (version) VALUES (?)""", (DB_VERSION,))

    conn.commit()
    conn.close()

def get_database_version(db_path: str) -> str:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT version FROM db_version")
    rows = cursor.fetchall()
    conn.close()
    return rows[0][0]


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Add games to TurboStage database"
    )

    # Mandatory input database argument
    parser.add_argument(
        "input_db",
        type=str,
        help="Path to the input TurboStage database file"
    )

    # Optional output database argument
    parser.add_argument(
        "-o", "--output_db",
        type=str,
        default=None,
        help="Path to the output TurboStage database file (optional)"
    )

    args = parser.parse_args()

    # Validate input database exists
    if not os.path.isfile(args.input_db):
        parser.error(f"Input database '{args.input_db}' does not exist.")

    return args

def get_table_columns(cursor, table_name):
    """Retrieve column names of a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [info[1] for info in cursor.fetchall()]

def copy_table(table_name: str, input_cursor: sqlite3.Cursor, output_cursor: sqlite3.Cursor, version_id_mapping: dict):
    columns = get_table_columns(input_cursor, table_name)
    input_version_ids = list(version_id_mapping.keys())
    placeholders = ",".join(["?" for _ in input_version_ids])
    input_cursor.execute(f"SELECT * FROM {table_name} WHERE version_id IN ({placeholders})", input_version_ids)
    input_rows = input_cursor.fetchall()

    insert_columns = [col for col in columns if col != "id"]
    value_placeholders = ",".join(["?" for _ in insert_columns])
    insert_query = f"INSERT INTO {table_name} ({','.join(insert_columns)}) VALUES ({value_placeholders})"

    inserted_row_count = 0
    version_id_idx = columns.index("version_id")

    for row in input_rows:
        input_version_id = row[version_id_idx]
        if input_version_id not in version_id_mapping:
            continue

        row_data = [
            version_id_mapping[input_version_id] if col == "version_id" else row[columns.index(col)]
            for col in insert_columns
        ]
        output_cursor.execute(insert_query, tuple(row_data))
        inserted_row_count += 1

    print(f"Processed {len(input_rows)} {table_name} rows from input database.")
    print(f"Inserted {inserted_row_count} new {table_name} rows into output database.")

def copy_versions(input_cursor: sqlite3.Cursor, output_cursor: sqlite3.Cursor, game_id_mapping: dict) -> dict:
    input_game_ids = list(game_id_mapping.keys())
    placeholders = ",".join(["?" for _ in input_game_ids])
    input_cursor.execute(f"SELECT * FROM versions WHERE game_id IN ({placeholders})", input_game_ids)
    input_version_rows = input_cursor.fetchall()

    version_columns = get_table_columns(input_cursor, "versions")
    insert_columns = [col for col in version_columns if col != "id"]
    version_placeholders = ",".join(["?" for _ in insert_columns])
    version_insert_query = f"INSERT INTO versions ({','.join(insert_columns)}) VALUES ({version_placeholders})"

    version_id_mapping = {}
    inserted_version_count = 0
    game_id_idx = version_columns.index("game_id")
    for row in input_version_rows:
        input_game_id = row[game_id_idx]
        input_version_id = row[version_columns.index("id")]
        if input_game_id not in game_id_mapping:
            raise RuntimeError(f"Game ID '{input_game_id}' not found.")

        # Prepare row data, excluding 'id' and updating 'game_id'
        row_data = [
            game_id_mapping[input_game_id] if col == "game_id" else row[version_columns.index(col)]
            for col in insert_columns
        ]
        output_cursor.execute(version_insert_query, row_data)
        inserted_version_count += 1
        new_version_id = output_cursor.lastrowid
        version_id_mapping[input_version_id] = new_version_id

    print(f"Processed {len(input_version_rows)} version rows from input database.")
    print(f"Inserted {inserted_version_count} new version rows into output database.")

    return version_id_mapping


def copy_db_content(input_cursor: sqlite3.Cursor, output_cursor: sqlite3.Cursor) -> dict:

    columns = get_table_columns(input_cursor, "games")
    if "igdb_id" not in columns:
        raise ValueError("Input database 'games' table does not have an 'igdb_id' column.")

    input_cursor.execute(f"SELECT * FROM games")
    input_rows = input_cursor.fetchall()

    # Get existing igdb_ids in output database
    output_cursor.execute("SELECT igdb_id FROM games")
    existing_igdb_ids = set(row[0] for row in output_cursor.fetchall())

    # Prepare insert query
    insert_columns = columns[:columns.index("id")] + columns[columns.index("id")+1:]
    placeholders = ",".join(["?" for _ in insert_columns])
    insert_query = f"INSERT INTO games ({','.join(insert_columns)}) VALUES ({placeholders})"

    # Compare and insert new rows
    inserted_count = 0
    game_id_mapping= {}
    for row in input_rows:
        igdb_id = row[columns.index("igdb_id")]
        if igdb_id in existing_igdb_ids:
            continue
        input_id = row[columns.index("id")]
        insert_row = row[:columns.index("id")] + row[columns.index("id")+1:]
        output_cursor.execute(insert_query, insert_row)
        inserted_count += 1
        existing_igdb_ids.add(igdb_id)  # Update set to avoid duplicates
        output_game_id = output_cursor.lastrowid
        game_id_mapping[input_id] = output_game_id

    print(f"Processed {len(input_rows)} rows from input database.")
    print(f"Inserted {inserted_count} new rows into output database.")
    return game_id_mapping


def main():
    args = parse_arguments()

    input_db_version = get_database_version(args.input_db)

    output_db = args.output_db if args.output_db else "turbostage.db"
    if os.path.isfile(output_db):
        output_db_version = get_database_version(output_db)
        if input_db_version != output_db_version:
            print(f"Database version mismatch: {input_db_version} != {output_db_version}.")
            exit(1)
    else:
        initialize_database(output_db)

    input_conn = sqlite3.connect(args.input_db)
    input_cursor = input_conn.cursor()

    output_conn = sqlite3.connect(output_db)
    output_cursor = output_conn.cursor()
    try:
        game_id_mapping = copy_db_content(input_cursor, output_cursor)
        output_conn.commit()
        version_id_mapping = copy_versions(input_cursor, output_cursor, game_id_mapping)
        output_conn.commit()
        copy_table("hashes", input_cursor, output_cursor, version_id_mapping)
        output_conn.commit()
        copy_table("config_files", input_cursor, output_cursor, version_id_mapping)
        output_conn.commit()
    except sqlite3.Error as error:
        print(f"Database error: {error}")
        exit(1)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
    finally:
        input_conn.close()
        output_conn.close()


if __name__ == "__main__":
    main()
