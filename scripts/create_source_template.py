#!/usr/bin/env python3
import re
import shutil
from pathlib import Path
import json
import subprocess
import sys

TEMPLATE_DIR = Path("source_bag")
ENV_TEMPLATE = """DBT_TARGET=dev-databend
DB_TARGET=databend
SOURCE_NAME={slug}

# To load these variables in your shell:
# cd source_{slug}
# export $(grep -v '^#' .env | xargs)

# Databend (dev)
DATABEND_HOST=
DATABEND_HTML_PORT=3307
DATABEND_DBT_PORT=8000
DATABEND_USER=
DATABEND_PASSWORD=
DATABEND_SCHEMA=
DATABEND_DATABASE=

# Snowflake (dev - SSO)
SNOWFLAKE_DEV_ACCOUNT=
SNOWFLAKE_DEV_USER=
SNOWFLAKE_DEV_ROLE=
SNOWFLAKE_DEV_WAREHOUSE=
SNOWFLAKE_DEV_DATABASE=DATALAKE
SNOWFLAKE_DEV_SCHEMA=
SNOWFLAKE_DEV_AUTHENTICATOR=externalbrowser

# Snowflake (prod - keypair in GitHub secrets)
# SNOWFLAKE_PROD_ACCOUNT=
# SNOWFLAKE_PROD_USER=
# SNOWFLAKE_PROD_ROLE=
# SNOWFLAKE_PROD_WAREHOUSE=
# SNOWFLAKE_PROD_DATABASE=
# SNOWFLAKE_PROD_SCHEMA=
# SNOWFLAKE_PROD_PRIVATE_KEY=
# SNOWFLAKE_PROD_PRIVATE_KEY_PASSPHRASE=
"""
SNOWFLAKE_TASK_TEMPLATE = """USE DATABASE DATALAKE;
USE SCHEMA SOURCE_{slug_upper};

-- Snowflake task to run the notebook + dbt project on a schedule.
-- Replace the placeholders below to match your DBT project location.
CREATE OR REPLACE TASK RUN_NOTEBOOK_SOURCE_{slug_upper}
  WAREHOUSE = CICD_WAREHOUSE
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  USER_TASK_TIMEOUT_MS = 14400000
  EXECUTE AS USER "CICD_USER_ROLE"
AS
  EXECUTE NOTEBOOK "SOURCE_{slug_upper}"."INGESTION_{slug_upper}";

CREATE OR REPLACE TASK DBT_PROJECT_SOURCE_{slug_upper}
  WAREHOUSE = CICD_WAREHOUSE
  AFTER RUN_NOTEBOOK_SOURCE_{slug_upper}
  USER_TASK_TIMEOUT_MS = 14400000
  EXECUTE AS USER "CICD_USER_ROLE"
AS
  EXECUTE DBT PROJECT "DATALAKE"."SOURCE_{slug_upper}"."SOURCE_{slug_upper}"
  ARGS = 'run';

ALTER TASK RUN_NOTEBOOK_SOURCE_{slug_upper} RESUME;
ALTER TASK RUN_DBT_PROJECT_SOURCE_{slug_upper} RESUME;
"""


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9_]+", "_", name.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        raise ValueError("Source name must contain at least one alphanumeric character.")
    return slug


def copy_template(dst: Path) -> None:
    ignore = shutil.ignore_patterns(
        ".venv",
        "target",
        "dbt_packages",
        "logs",
        "tmp",
        ".env",
        ".DS_Store",
    )
    shutil.copytree(TEMPLATE_DIR, dst, ignore=ignore)


def rename_files(dst: Path, slug: str) -> None:
    ingestion_src = dst / "ingestion_bag.ipynb"
    if ingestion_src.exists():
        ingestion_src.rename(dst / f"ingestion_{slug}.ipynb")

    raw_dir = dst / "models" / "raw"
    if raw_dir.exists():
        for path in raw_dir.glob("stg__bag_*.sql"):
            path.rename(raw_dir / path.name.replace("stg__bag_", f"stg__{slug}_"))


def replace_content(dst: Path, slug: str) -> None:
    replacements = [
        ("SOURCE_BAG", f"SOURCE_{slug.upper()}"),
        ("source_bag_", f"source_{slug}_"),
        ("stg__bag_", f"stg__{slug}_"),
        ("ingestion_bag", f"ingestion_{slug}"),
        ("INGESTION_BAG", f"INGESTION_{slug.upper()}"),
        ("source_bag", f"source_{slug}"),
    ]

    text_exts = {".yml", ".yaml", ".sql", ".md", ".ipynb", ".txt", ".gitignore"}
    for path in dst.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in text_exts and path.name not in text_exts:
            continue
        content = path.read_text(encoding="utf-8")
        for old, new in replacements:
            content = content.replace(old, new)
        path.write_text(content, encoding="utf-8")


def ensure_env_file(dst: Path, slug: str) -> None:
    env_path = dst / ".env"
    if env_path.exists():
        return

    env_example = dst / ".env.example"
    if env_example.exists():
        env_example.rename(env_path)
        return

    env_path.write_text(ENV_TEMPLATE.format(slug=slug), encoding="utf-8")


def update_notebook_template(dst: Path, slug: str) -> None:
    notebook_path = dst / f"ingestion_{slug}.ipynb"
    if not notebook_path.exists():
        return

    title = f"Ingestion Notebook - {slug}"
    table_name = f"source_{slug}_test_table"

    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# {title}\n",
                "This notebook is a clean starting point for building a source ingestion.\n",
                "It creates a small test table by default so you can verify connectivity.\n",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Setup\n",
                "- Confirm your `.env` is present and filled with connection details.\n",
                "- Optional: set `DB_TARGET=databend` or `DB_TARGET=snowflake`.\n",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Helper Functions (Quick Reference)\n",
                "- `_get_source_name()`: resolves the source name from `SOURCE_NAME` or the folder name.\n",
                "- `get_connection()`: opens a DB connection for the current target.\n",
                "- `ensure_schema(cursor)`: creates and selects the target schema.\n",
                "- `ensure_tables(cursor, table_name)`: creates `{table_name}` and `{table_name}_tmp`.\n",
                "- `insert_batch(cursor, table_name, json_set)`: inserts JSON rows into `{table_name}_tmp`.\n",
                "- `finalize_table(cursor, table_name)`: replaces `{table_name}` with the temp table.\n",
            ],
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "## Template Script (Runs Live)\n",
                "Includes an **empty row** example and performs a real insert into the test table.\n",
            ],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "execution_count": None,
            "outputs": [],
            "source": [
                "from ingestion_helpers import (\n",
                "    get_connection,\n",
                "    ensure_schema,\n",
                "    ensure_tables,\n",
                "    insert_batch,\n",
                "    finalize_table,\n",
                ")\n",
                "\n",
                f"TABLE_NAME = \"{table_name}\"\n",
                "\n",
                "def drop_table(cursor, table_name):\n",
                "    cursor.execute(f\"DROP TABLE IF EXISTS {table_name}\")\n",
                "\n",
                "example_rows = [\n",
                "    {\"example\": \"value\"},\n",
                "    {},  # empty row example\n",
                "]\n",
                "\n",
                "conn = get_connection()\n",
                "cursor = conn.cursor()\n",
                "\n",
                "ensure_schema(cursor)\n",
                "ensure_tables(cursor, TABLE_NAME)\n",
                "\n",
                "insert_batch(cursor, TABLE_NAME, example_rows)\n",
                "finalize_table(cursor, TABLE_NAME)\n",
                "\n",
                "conn.commit()\n",
                "print(\"Inserted rows and finalized table.\")\n",
                "\n",
                "# Uncomment to drop the test table after running.\n",
                "# drop_table(cursor, TABLE_NAME)\n",
            ],
        },
    ]

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    notebook_path.write_text(json.dumps(notebook, indent=2), encoding="utf-8")


def ensure_snowflake_task_file(dst: Path, slug: str) -> None:
    task_path = dst / "snowflake_task.sql"
    if task_path.exists():
        return
    task_path.write_text(
        SNOWFLAKE_TASK_TEMPLATE.format(
            slug_upper=slug.upper(),
        ),
        encoding="utf-8",
    )


def create_venv(dst: Path) -> None:
    venv_path = dst / ".venv"
    if venv_path.exists():
        return

    subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)

    if sys.platform == "win32":
        python_bin = venv_path / "Scripts" / "python"
    else:
        python_bin = venv_path / "bin" / "python"

    req_file = dst / "requirements.txt"
    if req_file.exists():
        subprocess.run([str(python_bin), "-m", "pip", "install", "-r", str(req_file)], check=True)


def main() -> None:
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 scripts/create_source_template.py <source_name>")

    slug = slugify(sys.argv[1])
    dst = Path(f"src__{slug}")

    if dst.exists():
        raise SystemExit(f"Destination already exists: {dst}")

    if not TEMPLATE_DIR.exists():
        raise SystemExit(f"Template not found: {TEMPLATE_DIR}")

    copy_template(dst)
    rename_files(dst, slug)
    replace_content(dst, slug)
    ensure_env_file(dst, slug)
    update_notebook_template(dst, slug)
    ensure_snowflake_task_file(dst, slug)
    create_venv(dst)

    print(f"Created template at {dst}")


if __name__ == "__main__":
    main()
