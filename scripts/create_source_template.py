#!/usr/bin/env python3
import re
import shutil
from pathlib import Path

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

    print(f"Created template at {dst}")


if __name__ == "__main__":
    main()
