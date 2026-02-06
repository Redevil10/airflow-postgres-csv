# airflow-postgres-csv

Airflow 3 operators for bulk PostgreSQL ↔ CSV transfers using `COPY`.

## Operators

- **`PostgresToCsvOperator`** — Run a SQL query and export results to a CSV file
- **`CsvToPostgresOperator`** — Load a CSV file into a PostgreSQL table

Both use PostgreSQL's `COPY` command for maximum throughput.

## Installation

```bash
pip install airflow-postgres-csv
```

## Usage

```python
from airflow_postgres_csv import PostgresToCsvOperator, CsvToPostgresOperator

# Export query results to CSV
export_task = PostgresToCsvOperator(
    task_id="export_users",
    conn_id="my_postgres",
    sql_query="SELECT * FROM users WHERE active = %(active)s",
    query_params={"active": True},
    csv_file_path="/tmp/users.csv",
)

# Load CSV into a table
import_task = CsvToPostgresOperator(
    task_id="import_users",
    conn_id="my_postgres",
    table_name="staging.users",
    csv_file_path="/tmp/users.csv",
)
```

### Key parameters

**PostgresToCsvOperator:**
| Parameter | Description | Default |
|---|---|---|
| `conn_id` | Airflow Postgres connection ID | required |
| `csv_file_path` | Output file path (template-able) | required |
| `sql_query` | SQL to execute | `None` |
| `sql_file_path` | Path to `.sql` file | `None` |
| `query_params` | Dict passed to `cursor.mogrify` | `{}` |
| `has_header` | Include CSV header | `True` |
| `timeout` | Query timeout (minutes) | `60` |

**CsvToPostgresOperator:**
| Parameter | Description | Default |
|---|---|---|
| `conn_id` | Airflow Postgres connection ID | required |
| `table_name` | Target table (template-able, supports `schema.table`) | required |
| `csv_file_path` | Input file path (template-able) | required |
| `columns` | Explicit column list | `None` |
| `has_header` | CSV has header row | `True` |
| `delimiter` | CSV delimiter | `","` |
| `quote_char` | CSV quote character | `'"'` |
| `null_string` | String representing NULL | `""` |
| `timeout` | Query timeout (minutes) | `60` |

## Requirements

- Apache Airflow >= 3.0.0
- apache-airflow-providers-postgres >= 6.0.0

## License

MIT
