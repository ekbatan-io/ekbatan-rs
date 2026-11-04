#!/usr/bin/env python3
"""Writes each example's MySQL and MariaDB copies from its PostgreSQL source.

The `-pg` project is the source. Its `src/` is copied byte for byte (with the crate name
changed); its `Cargo.toml`, `build.rs`, `ekbatan.json`, `README.md` and `tests/` are copied with
the few database-specific lines changed. A copy's `migrations/` is its own and is never
touched: the DDL differs per database (native UUID and JSONB against CHAR(36) and JSON).

    python3 ekbatan-rs-examples/sync-examples.py           # write the copies
    python3 ekbatan-rs-examples/sync-examples.py --check   # fail if a copy is out of date
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = ["axum-wallet-rest"]

DATABASES = {
    "mysql": {
        "name": "MySQL",
        "dialect": "MySql",
        "jdbc": "jdbc:mysql://localhost:3306/wallet",
        "container": ("mysql::Mysql", "Mysql", "8.4", 3306),
        "test_url": "jdbc:mysql://127.0.0.1:{port}/test",
        "env": ("MYSQL_USER", "MYSQL_PASSWORD"),
        "compose": """# The database `cargo run` talks to, with the login and database `ekbatan.json` defaults to.
# `docker compose up -d` starts it; `docker compose down` removes it. init.sql lets `wallet`
# create the `eventlog` database the first migration makes.
services:
  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_USER: wallet
      MYSQL_PASSWORD: wallet
      MYSQL_DATABASE: wallet
      TZ: UTC
    ports:
      - "3306:3306"
    volumes:
      - "./init.sql:/docker-entrypoint-initdb.d/init.sql:ro"
""",
    },
    "mariadb": {
        "name": "MariaDB",
        "dialect": "MariaDb",
        "jdbc": "jdbc:mariadb://localhost:3306/wallet",
        "container": ("mariadb::Mariadb", "Mariadb", "11.8", 3306),
        "test_url": "jdbc:mariadb://127.0.0.1:{port}/test",
        "env": ("MARIADB_USER", "MARIADB_PASSWORD"),
        "compose": """# The database `cargo run` talks to, with the login and database `ekbatan.json` defaults to.
# `docker compose up -d` starts it; `docker compose down` removes it. init.sql lets `wallet`
# create the `eventlog` database the first migration makes.
services:
  mariadb:
    image: mariadb:11.8
    environment:
      MARIADB_ROOT_PASSWORD: root
      MARIADB_USER: wallet
      MARIADB_PASSWORD: wallet
      MARIADB_DATABASE: wallet
      TZ: UTC
    ports:
      - "3306:3306"
    volumes:
      - "./init.sql:/docker-entrypoint-initdb.d/init.sql:ro"
""",
    },
}


def rewrite(text: str, source: str, db: str) -> str:
    d = DATABASES[db]
    crate, target = f"{source}-pg", f"{source}-{db}"
    text = text.replace(crate, target).replace(crate.replace("-", "_"), target.replace("-", "_"))
    text = text.replace("ekbatan::Dialect::Postgres", f"ekbatan::Dialect::{d['dialect']}")
    text = text.replace("jdbc:postgresql://localhost:5432/wallet", d["jdbc"])
    text = re.sub(r'\n\s*"dataSourceProperties": \{[^}]*\},', "", text)
    # The test's container.
    module, image, tag, port = d["container"]
    text = text.replace("testcontainers_modules::postgres::Postgres", f"testcontainers_modules::{module}")
    text = text.replace("Postgres::default()", f"{image}::default()")
    user_var, password_var = d["env"]
    text = text.replace(
        '.with_tag("17-alpine")',
        f'.with_tag("{tag}")\n'
        f'        .with_env_var("{user_var}", "wallet")\n'
        f'        .with_env_var("{password_var}", "wallet")\n'
        '        .with_copy_to(\n'
        '            "/docker-entrypoint-initdb.d/init.sql",\n'
        '            include_bytes!("../init.sql").to_vec(),\n'
        '        )',
    )
    text = text.replace("get_host_port_ipv4(5432)", f"get_host_port_ipv4({port})")
    text = text.replace(
        'format!("jdbc:postgresql://127.0.0.1:{port}/postgres")',
        'format!("' + d["test_url"] + '")',
    )
    text = text.replace(
        '"DATABASE_USER" | "DATABASE_PASSWORD" => Some("postgres".to_owned()),',
        '"DATABASE_USER" | "DATABASE_PASSWORD" => Some("wallet".to_owned()),',
    )
    text = text.replace("PostgreSQL", d["name"])
    return text


INIT_SQL = """-- Runs as root when the container is first created. The named user (`wallet`) only has rights
-- on its own database, and the first migration creates the `eventlog` database and writes
-- tables into it, so it gets rights on every database. Java's examples grant the same.
GRANT ALL PRIVILEGES ON *.* TO 'wallet'@'%';
FLUSH PRIVILEGES;
"""


def expected_files(source: str, db: str) -> dict[Path, str]:
    src_dir = ROOT / f"{source}-pg"
    files = {}
    for path in sorted(src_dir.rglob("*")):
        rel = path.relative_to(src_dir)
        if not path.is_file() or rel.parts[0] in ("migrations", "target"):
            continue
        if rel == Path("compose.yaml"):
            files[rel] = DATABASES[db]["compose"]
        else:
            files[rel] = rewrite(path.read_text(), source, db)
    files[Path("init.sql")] = INIT_SQL
    return files


def main() -> int:
    check = "--check" in sys.argv
    stale = []
    for source in SOURCES:
        for db in DATABASES:
            out_dir = ROOT / f"{source}-{db}"
            if not (out_dir / "migrations").is_dir():
                print(f"{out_dir.name}/migrations is missing: a copy's migrations are its own")
                return 1
            expected = expected_files(source, db)
            present = {
                p.relative_to(out_dir)
                for p in out_dir.rglob("*")
                if p.is_file() and p.relative_to(out_dir).parts[0] not in ("migrations", "target")
            }
            for rel, text in expected.items():
                target = out_dir / rel
                if not target.exists() or target.read_text() != text:
                    stale.append(f"{out_dir.name}/{rel}")
                    if not check:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_text(text)
            for rel in present - set(expected):
                stale.append(f"{out_dir.name}/{rel} (not in the source)")
                if not check:
                    (out_dir / rel).unlink()
    if check and stale:
        print("Out of date; run python3 ekbatan-rs-examples/sync-examples.py:")
        for line in stale:
            print(f"  {line}")
        return 1
    if not check:
        print(f"wrote {len(stale)} file(s)" if stale else "already up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
