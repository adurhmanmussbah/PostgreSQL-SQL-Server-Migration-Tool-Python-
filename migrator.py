import os
import json
import logging
import psycopg2
import psycopg2.extras
import pyodbc
from typing import List, Dict, Any


# -------------------------------------------------------------
#  LOGGING
# -------------------------------------------------------------
def setup_logging():
    if not os.path.exists("logs"):
        os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/migration.log"),
            logging.StreamHandler()
        ]
    )

setup_logging()


# -------------------------------------------------------------
#  LOAD CONFIG
# -------------------------------------------------------------
with open("config.json", "r") as f:
    CFG = json.load(f)

PG = CFG["postgres"]
SQL = CFG["sqlserver"]
SCHEMAS = CFG["schemas"]
BATCH_SIZE = CFG["batch_size"]
EXPORT_DIR = CFG["export_dir"]


# -------------------------------------------------------------
#  CONNECTION HELPERS
# -------------------------------------------------------------
def pg_conn():
    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        dbname=PG["database"],
        user=PG["user"],
        password=PG["password"],
    )
    conn.autocommit = True
    return conn


def sql_conn():
    conn_str = (
        f"DRIVER={SQL['driver']};"
        f"SERVER={SQL['server']};"
        f"DATABASE={SQL['database']};"
        f"UID={SQL['uid']};"
        f"PWD={SQL['pwd']};"
        f"Encrypt={SQL['encrypt']};"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    return conn


# -------------------------------------------------------------
#  TYPE MAPPING
# -------------------------------------------------------------
def map_pg_type(pg_type, length, precision, scale, is_serial):
    pg_type = pg_type.lower()

    if is_serial:
        return "INT IDENTITY(1,1)"

    mapping = {
        "integer": "INT",
        "int4": "INT",
        "bigint": "BIGINT",
        "int8": "BIGINT",
        "smallint": "SMALLINT",
        "boolean": "BIT",
        "date": "DATE",
        "timestamp without time zone": "DATETIME2",
        "timestamp with time zone": "DATETIME2",
        "double precision": "FLOAT",
        "real": "REAL",
    }

    if pg_type in mapping:
        return mapping[pg_type]

    if pg_type == "character varying":
        if not length or length > 4000:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({length})"

    if pg_type == "text":
        return "NVARCHAR(MAX)"

    if pg_type in ["numeric", "decimal"]:
        return f"DECIMAL({precision or 18},{scale or 4})"

    return "NVARCHAR(MAX)"


# -------------------------------------------------------------
#  METADATA FUNCTIONS
# -------------------------------------------------------------
def get_tables(pg):
    pg.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ANY(%s)
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;
    """, (SCHEMAS,))
    return pg.fetchall()


def get_columns(pg, schema, table):
    pg.execute("""
        SELECT column_name, data_type, is_nullable,
               character_maximum_length,
               numeric_precision, numeric_scale,
               column_default
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, table))

    cols = []
    for row in pg.fetchall():
        is_serial = row[6] and isinstance(row[6], str) and row[6].startswith("nextval")
        cols.append({
            "name": row[0],
            "data_type": row[1],
            "nullable": row[2] == "YES",
            "length": row[3],
            "precision": row[4],
            "scale": row[5],
            "is_serial": is_serial
        })
    return cols


def get_primary_key(pg, schema, table):
    pg.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type='PRIMARY KEY'
          AND tc.table_schema=%s
          AND tc.table_name=%s
        ORDER BY kcu.ordinal_position
    """, (schema, table))
    return [r[0] for r in pg.fetchall()]


def get_indexes(pg, schema, table):
    pg.execute("""
        SELECT
            idx.indexname,
            idx.indexdef
        FROM pg_indexes idx
        WHERE schemaname=%s AND tablename=%s
    """, (schema, table))
    return pg.fetchall()


def get_foreign_keys(pg, schema, table):
    pg.execute("""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name  AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
             ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
             ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema=%s
          AND tc.table_name=%s
    """, (schema, table))
    return pg.fetchall()


# -------------------------------------------------------------
#  CREATE TABLE
# -------------------------------------------------------------
def create_table(sql, schema, table, columns, pk):
    sql.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema}') EXEC('CREATE SCHEMA [{schema}]');")

    col_defs = []
    for c in columns:
        sql_type = map_pg_type(c["data_type"], c["length"], c["precision"], c["scale"], c["is_serial"])
        null_str = "NULL" if c["nullable"] and not c["is_serial"] else "NOT NULL"
        col_defs.append(f"[{c['name']}] {sql_type} {null_str}")

    pk_clause = ""
    if pk:
        pk_cols = ", ".join(f"[{c}]" for c in pk)
        pk_clause = f", CONSTRAINT [PK_{schema}_{table}] PRIMARY KEY ({pk_cols})"

    create_sql = f"""
        IF OBJECT_ID('{schema}.{table}', 'U') IS NULL
        CREATE TABLE [{schema}].[{table}] (
            {", ".join(col_defs)}
            {pk_clause}
        );
    """

    logging.info(f"Creating table {schema}.{table}")
    sql.execute(create_sql)


# -------------------------------------------------------------
#  INDEX MIGRATION
# -------------------------------------------------------------
def create_indexes(sql, schema, table, indexes):
    for name, idxdef in indexes:
        if "unique" in idxdef.lower():
            unique = "UNIQUE"
        else:
            unique = ""

        # Extract column list
        start = idxdef.index("(") + 1
        end = idxdef.index(")")
        cols = idxdef[start:end].replace('"', "").split(",")

        col_list = ", ".join(f"[{c.strip()}]" for c in cols)
        index_name = f"IX_{schema}_{table}_{name}"

        sql.execute(f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes WHERE name='{index_name}'
            )
            CREATE {unique} INDEX [{index_name}]
            ON [{schema}].[{table}] ({col_list});
        """)

        logging.info(f"Created index {index_name}")


# -------------------------------------------------------------
#  FOREIGN KEY MIGRATION
# -------------------------------------------------------------
def create_foreign_keys(sql, schema, table, fks):
    for fk in fks:
        fk_name, column, ref_schema, ref_table, ref_col = fk
        fk_sql = f"""
        ALTER TABLE [{schema}].[{table}]
        ADD CONSTRAINT [FK_{schema}_{table}_{fk_name}]
        FOREIGN KEY ([{column}])
        REFERENCES [{ref_schema}].[{ref_table}]([{ref_col}]);
        """

        logging.info(f"Creating FK {fk_name}")
        sql.execute(fk_sql)


# -------------------------------------------------------------
#  DATA COPY
# -------------------------------------------------------------
def copy_data(pg, sql, schema, table, columns):
    col_names = [c["name"] for c in columns]
    pg_cols = ", ".join(f'"{c}"' for c in col_names)
    sql_cols = ", ".join(f"[{c}]" for c in col_names)
    placeholders = ", ".join("?" for _ in col_names)

    pg.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    total = pg.fetchone()[0]
    logging.info(f"Copying {total} rows from {schema}.{table}")

    pg.execute(f'SELECT {pg_cols} FROM "{schema}"."{table}"')

    copied = 0
    while True:
        batch = pg.fetchmany(BATCH_SIZE)
        if not batch:
            break
        sql.executemany(f"INSERT INTO [{schema}].[{table}] ({sql_cols}) VALUES ({placeholders})", batch)
        copied += len(batch)
        logging.info(f"  Copied {copied}/{total}")


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
def migrate():
    logging.info("Starting Migration...")

    pgc = pg_conn()
    pg = pgc.cursor()

    sqlc = sql_conn()
    sql = sqlc.cursor()

    tables = get_tables(pg)

    for schema, table in tables:
        logging.info(f"Processing table: {schema}.{table}")

        cols = get_columns(pg, schema, table)
        pk = get_primary_key(pg, schema, table)
        indexes = get_indexes(pg, schema, table)
        fks = get_foreign_keys(pg, schema, table)

        create_table(sql, schema, table, cols, pk)
        copy_data(pg, sql, schema, table, cols)
        create_indexes(sql, schema, table, indexes)
        create_foreign_keys(sql, schema, table, fks)

    logging.info("Migration completed successfully!")


if __name__ == "__main__":
    migrate()
