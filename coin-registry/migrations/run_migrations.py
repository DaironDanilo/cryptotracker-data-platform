#!/usr/bin/env python3
"""Applies every .sql file in this directory, in filename order, exactly once.

Re-running is safe: already-applied migrations are recorded in
schema_migrations and skipped. Each migration's own DDL is also written with
IF NOT EXISTS guards, so even a manual re-run of a single file is harmless.
"""
import os
import sys
from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).parent
DATABASE_URL = os.environ.get("DATABASE_URL")


def main() -> None:
    if not DATABASE_URL:
        print("DATABASE_URL environment variable is required", file=sys.stderr)
        sys.exit(1)

    sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not sql_files:
        print("no .sql migration files found")
        return

    with psycopg.connect(DATABASE_URL, autocommit=False, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    filename    text PRIMARY KEY,
                    applied_at  timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute("SELECT filename FROM schema_migrations")
            already_applied = {row[0] for row in cur.fetchall()}
        conn.commit()

        for path in sql_files:
            if path.name in already_applied:
                print(f"skip  {path.name} (already applied)")
                continue

            print(f"apply {path.name}")
            with conn.cursor() as cur:
                cur.execute(path.read_text())
                cur.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (path.name,),
                )
            conn.commit()

    print("migrations up to date")


if __name__ == "__main__":
    main()
