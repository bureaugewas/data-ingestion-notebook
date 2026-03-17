#!/usr/bin/env python3
import re
import shutil
from pathlib import Path
import json

TEMPLATE_DIR = Path("source_bag")


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
        ("source_bag", f"source_{slug}"),
    ]

    text_exts = {".yml", ".yaml", ".sql", ".md", ".ipynb", ".txt"}
    for path in dst.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in text_exts:
            continue
        content = path.read_text(encoding="utf-8")
        for old, new in replacements:
            content = content.replace(old, new)
        path.write_text(content, encoding="utf-8")


def update_notebook_template(dst: Path, slug: str) -> None:
    notebook_path = dst / f"ingestion_{slug}.ipynb"
    if not notebook_path.exists():
        return

    title = f"Ingestion Notebook - {slug}"
    table_name = f"source_{slug}_records"

    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# {title}\n",
                "This notebook is a clean starting point for building a source ingestion.\n",
                "It is safe to run as-is because `DRY_RUN` is enabled by default.\n",
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
                "## Template Script (Safe by Default)\n",
                "Includes an **empty row** example and a `DRY_RUN` switch so you can run without inserting anything.\n",
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
                "DRY_RUN = True  # Set to False to actually insert and finalize\n",
                "\n",
                "example_rows = [\n",
                "    {\"example\": \"value\"},\n",
                "    {},  # empty row example\n",
                "]\n",
                "\n",
                "if DRY_RUN:\n",
                "    print(\"DRY_RUN enabled -- no database calls will be made.\")\n",
                "    print(f\"Would insert {len(example_rows)} rows into {TABLE_NAME}_tmp\")\n",
                "    print(\"Empty row payload:\", example_rows[1])\n",
                "else:\n",
                "    conn = get_connection()\n",
                "    cursor = conn.cursor()\n",
                "\n",
                "    ensure_schema(cursor)\n",
                "    ensure_tables(cursor, TABLE_NAME)\n",
                "\n",
                "    insert_batch(cursor, TABLE_NAME, example_rows)\n",
                "    finalize_table(cursor, TABLE_NAME)\n",
                "\n",
                "    conn.commit()\n",
                "    print(\"Inserted rows and finalized table.\")\n",
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
    env_example = dst / ".env.example"
    if env_example.exists():
        env_example.rename(dst / ".env")
    rename_files(dst, slug)
    replace_content(dst, slug)
    update_notebook_template(dst, slug)

    print(f"Created template at {dst}")


if __name__ == "__main__":
    main()
