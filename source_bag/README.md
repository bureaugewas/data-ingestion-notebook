Source template for ingesting raw JSON into `DATALAKE.SOURCE_BAG` and shredding it with dbt.

### Quick start
1. Create `.env` from `.env.example` and fill in credentials.
2. Run the notebook to ingest raw JSON into `DATALAKE.SOURCE_BAG`.
3. Run dbt:
   - `DBT_TARGET=dev dbt build --project-dir . --profiles-dir .`
   - `DBT_TARGET=prod dbt build --project-dir . --profiles-dir .`

### Conventions
- Raw schema: `DATALAKE.SOURCE_BAG`
- Raw table naming: `source_bag_<object>`
- Staging models: `models/raw/stg__bag_<object>.sql`

### Create a new source template
- `python3 scripts/create_source_template.py <source_name>`

### Ingestion helpers
Use `ingestion_helpers.py` from the notebook:
```python
from ingestion_helpers import (
    get_connection,
    ensure_schema,
    ensure_tables,
    insert_batch,
    finalize_table,
)
```
