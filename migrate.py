from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Iterable, List, Sequence

import yaml
import oracledb
import pyodbc


# Configuration models-------------------------------------------------

@dataclass
class OracleConfig:
    user: str
    password: str
    dsn: str


@dataclass
class MSSQLConfig:
    conn_str: str


@dataclass
class OptionsConfig:
    schema: str
    batch_size: int
    clear_before_load: bool
    validate_row_counts: bool
    stop_on_count_mismatch: bool
    include_tables: List[str]


@dataclass
class AppConfig:
    oracle: OracleConfig
    mssql: MSSQLConfig
    options: OptionsConfig


# Logging---------------------------------------------------------

def setup_logging() -> logging.Logger:
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("oracle_to_mssql")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        logs_dir / "migration.log",
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


LOGGER = setup_logging()


# Config loading-----------------------------------------

def load_config(path: str = "config.yaml") -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    oracle = OracleConfig(**raw["oracle"])
    mssql = MSSQLConfig(**raw["mssql"])
    options = OptionsConfig(**raw["options"])

    options.schema = options.schema.upper()
    options.include_tables = [t.upper() for t in options.include_tables]

    return AppConfig(oracle=oracle, mssql=mssql, options=options)


# Database connections -----------------------------------------------------

def connect_oracle(cfg: OracleConfig) -> oracledb.Connection:
    LOGGER.info("Connecting to Oracle: %s", cfg.dsn)
    conn = oracledb.connect(
        user=cfg.user,
        password=cfg.password,
        dsn=cfg.dsn,
    )
    return conn


def connect_mssql(cfg: MSSQLConfig) -> pyodbc.Connection:
    LOGGER.info("Connecting to SQL Server")
    conn = pyodbc.connect(cfg.conn_str)
    conn.autocommit = False
    return conn


# Metadata helpers ---------------------------------------------

def fetch_oracle_tables(
    ora: oracledb.Connection,
    schema: str,
    include_tables: Sequence[str],
) -> List[str]:
    cur = ora.cursor()
    cur.execute(
        """
        SELECT table_name
        FROM all_tables
        WHERE owner = :owner
        ORDER BY table_name
        """,
        owner=schema,
    )
    tables = [row[0] for row in cur.fetchall()]

    if include_tables:
        include_set = set(include_tables)
        tables = [t for t in tables if t in include_set]

    return tables


def fetch_mssql_tables(ms: pyodbc.Connection, schema: str) -> List[str]:
    cur = ms.cursor()
    cur.execute(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
        """,
        schema,
    )
    return [row[0] for row in cur.fetchall()]


def fetch_common_tables(
    ora: oracledb.Connection,
    ms: pyodbc.Connection,
    schema: str,
    include_tables: Sequence[str],
) -> List[str]:
    oracle_tables = set(fetch_oracle_tables(ora, schema, include_tables))
    mssql_tables = set(fetch_mssql_tables(ms, schema))
    common = sorted(oracle_tables.intersection(mssql_tables))

    missing_in_sql = sorted(oracle_tables - mssql_tables)
    if missing_in_sql:
        LOGGER.warning(
            "These Oracle tables are missing in SQL Server target and will be skipped: %s",
            ", ".join(missing_in_sql),
        )

    if not common:
        raise RuntimeError(
            f"No common tables found between Oracle schema {schema} and SQL Server schema {schema}."
        )

    return common


def get_oracle_columns(
    ora: oracledb.Connection,
    schema: str,
    table: str,
) -> List[str]:
    cur = ora.cursor()
    cur.execute(f'SELECT * FROM "{schema}"."{table}" WHERE 1 = 0')
    return [desc[0] for desc in cur.description]


# Constraint handling -----------------------------------------------------------------------------

def disable_constraints(ms: pyodbc.Connection, schema: str) -> None:
    LOGGER.info("Disabling constraints on schema %s", schema)
    cur = ms.cursor()
    cur.execute(
        """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = ?
        """,
        schema,
    )
    for table_schema, table_name in cur.fetchall():
        sql = f"ALTER TABLE [{table_schema}].[{table_name}] NOCHECK CONSTRAINT ALL"
        cur.execute(sql)
    ms.commit()


def enable_constraints(ms: pyodbc.Connection, schema: str) -> None:
    LOGGER.info("Re-enabling constraints with validation on schema %s", schema)
    cur = ms.cursor()
    cur.execute(
        """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = ?
        """,
        schema,
    )
    for table_schema, table_name in cur.fetchall():
        sql = f"ALTER TABLE [{table_schema}].[{table_name}] WITH CHECK CHECK CONSTRAINT ALL"
        cur.execute(sql)
    ms.commit()


# Data cleanup-----------------------------------------------------

def delete_target_data(ms: pyodbc.Connection, schema: str, tables: Sequence[str]) -> None:
    """
    Uses DELETE, not TRUNCATE, because TRUNCATE can fail when foreign keys exist.
    """
    LOGGER.info("Clearing existing target data with DELETE")
    cur = ms.cursor()
    for table in tables:
        sql = f"DELETE FROM [{schema}].[{table}]"
        cur.execute(sql)
    ms.commit()


# Validation helpers ---------------------------------------------------------

def count_oracle_rows(ora: oracledb.Connection, schema: str, table: str) -> int:
    cur = ora.cursor()
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    return int(cur.fetchone()[0])


def count_mssql_rows(ms: pyodbc.Connection, schema: str, table: str) -> int:
    cur = ms.cursor()
    cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
    return int(cur.fetchone()[0])


# disable triggers ------------------------------------------------------
def disable_triggers(ms: pyodbc.Connection, schema: str, tables: Sequence[str]) -> None:
    LOGGER.info("Disabling triggers on selected tables in schema %s", schema)
    cur = ms.cursor()
    for table in tables:
        cur.execute(f"ALTER TABLE [{schema}].[{table}] DISABLE TRIGGER ALL")
    ms.commit()

# enable triggers ------------------------------------------------------
def enable_triggers(ms: pyodbc.Connection, schema: str, tables: Sequence[str]) -> None:
    LOGGER.info("Re-enabling triggers on selected tables in schema %s", schema)
    cur = ms.cursor()
    for table in tables:
        cur.execute(f"ALTER TABLE [{schema}].[{table}] ENABLE TRIGGER ALL")
    ms.commit()





# Migration core------------------------------------------------------

def fetch_batches(cur: oracledb.Cursor, batch_size: int) -> Iterable[Sequence[Any]]:
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def migrate_table(
    ora: oracledb.Connection,
    ms: pyodbc.Connection,
    schema: str,
    table: str,
    batch_size: int,
) -> int:
    LOGGER.info("Migrating table %s.%s", schema, table)

    columns = get_oracle_columns(ora, schema, table)
    if not columns:
        LOGGER.warning("Skipping %s.%s because no columns were discovered", schema, table)
        return 0

    select_sql = f'SELECT * FROM "{schema}"."{table}"'
    insert_sql = (
        f"INSERT INTO [{schema}].[{table}] "
        f"({', '.join(f'[{c}]' for c in columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})"
    )

    ora_cur = ora.cursor()
    ora_cur.arraysize = batch_size
    ora_cur.execute(select_sql)

    ms_cur = ms.cursor()
    ms_cur.fast_executemany = True

    loaded = 0
    for batch in fetch_batches(ora_cur, batch_size):
        ms_cur.executemany(insert_sql, batch)
        ms.commit()
        loaded += len(batch)

    LOGGER.info("Loaded %s.%s: %d rows", schema, table, loaded)
    return loaded


# Main flow------------------------------------------------------------

def main() -> None:
    cfg = load_config()
    schema = cfg.options.schema
    batch_size = cfg.options.batch_size

    ora = None
    ms = None

    try:
        ora = connect_oracle(cfg.oracle)
        ms = connect_mssql(cfg.mssql)

        tables = fetch_common_tables(
            ora=ora,
            ms=ms,
            schema=schema,
            include_tables=cfg.options.include_tables,
        )

        LOGGER.info("Tables selected for migration: %s", ", ".join(tables))

        disable_constraints(ms, schema)
        disable_triggers(ms, schema, tables)

        if cfg.options.clear_before_load:
            delete_target_data(ms, schema, tables)

        mismatches: list[str] = []

        for table in tables:
            migrate_table(
                ora=ora,
                ms=ms,
                schema=schema,
                table=table,
                batch_size=batch_size,
            )

            if cfg.options.validate_row_counts:
                oracle_count = count_oracle_rows(ora, schema, table)
                mssql_count = count_mssql_rows(ms, schema, table)

                if oracle_count != mssql_count:
                    msg = (
                        f"Row-count mismatch for {schema}.{table}: "
                        f"Oracle={oracle_count}, SQL Server={mssql_count}"
                    )
                    LOGGER.error(msg)
                    mismatches.append(msg)
                else:
                    LOGGER.info(
                        "Validated %s.%s successfully: %d rows",
                        schema,
                        table,
                        oracle_count,
                    )

        enable_constraints(ms, schema)
        enable_triggers(ms, schema, tables)
        
        if mismatches and cfg.options.stop_on_count_mismatch:
            raise RuntimeError(
                "Migration finished but row-count validation failed:\n"
                + "\n".join(mismatches)
            )

        LOGGER.info("Migration finished successfully.")

    except Exception:
        LOGGER.exception("Migration failed.")
        if ms is not None:
            try:
                ms.rollback()
            except Exception:
                pass
        raise

    finally:
        if ms is not None:
            ms.close()
        if ora is not None:
            ora.close()


if __name__ == "__main__":
    main()