import json
import os
import time
from dotenv import load_dotenv

load_dotenv()

DB_TARGET = os.getenv("DB_TARGET", "databend").lower()


def _get_source_name():
    name = os.getenv("SOURCE_NAME")
    if name:
        return name

    base = os.path.basename(os.path.abspath(os.getcwd()))
    if base.startswith("src__"):
        return base.replace("src__", "", 1)
    if base.startswith("source_"):
        return base.replace("source_", "", 1)
    return "source"


def get_connection():
    if DB_TARGET == "snowflake":
        import snowflake.connector

        return snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        )

    import pymysql

    return pymysql.connect(
        host=os.environ["DATABEND_HOST"],
        port=int(
            os.environ.get("DATABEND_HTML_PORT")
            or os.environ.get("DATABEND_PORT", "3307")
        ),
        user=os.environ["DATABEND_USER"],
        password=os.environ["DATABEND_PASSWORD"],
    )


def ensure_schema(cursor):
    source_name = _get_source_name()
    if DB_TARGET == "snowflake":
        db = os.environ.get("SNOWFLAKE_DATABASE", "DATALAKE")
        default_schema = f"SOURCE_{source_name.upper()}"
        schema = os.environ.get("SNOWFLAKE_SCHEMA", default_schema)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
        cursor.execute(f"USE DATABASE {db}")
        cursor.execute(f"USE SCHEMA {db}.{schema}")
    else:
        default_schema = f"source_{source_name}"
        schema = os.environ.get("DATABEND_SCHEMA", default_schema)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
        cursor.execute(f"USE {schema}")


def ensure_tables(cursor, table_name):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            record_content VARIANT
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}_tmp (
            record_content VARIANT
        )
        """
    )
    cursor.execute(f"TRUNCATE TABLE {table_name}_tmp")


def insert_batch(cursor, table_name, json_set, timeout_seconds=30):
    if not json_set:
        return

    batch = [(json.dumps(obj),) for obj in json_set]
    max_retries = 10
    base_delay = 0.5
    deadline = time.time() + timeout_seconds

    for attempt in range(max_retries):
        if time.time() > deadline:
            raise TimeoutError("Insert batch timed out")
        try:
            cursor.executemany(
                f"""
                INSERT INTO {table_name}_tmp (record_content)
                VALUES (%s)
                """,
                batch,
            )
            return
        except Exception:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt)
            time.sleep(delay)


def finalize_table(cursor, table_name):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"ALTER TABLE {table_name}_tmp RENAME TO {table_name}")
