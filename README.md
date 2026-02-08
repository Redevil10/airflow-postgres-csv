# airflow-postgres-csv

[![lint](https://github.com/Redevil10/airflow-postgres-csv/actions/workflows/lint.yml/badge.svg)](https://github.com/Redevil10/airflow-postgres-csv/actions/workflows/lint.yml)
[![tests](https://github.com/Redevil10/airflow-postgres-csv/actions/workflows/test.yml/badge.svg)](https://github.com/Redevil10/airflow-postgres-csv/actions/workflows/test.yml)
[![codecov](https://codecov.io/github/Redevil10/airflow-postgres-csv/graph/badge.svg)](https://codecov.io/gh/Redevil10/airflow-postgres-csv)
[![python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/airflow-postgres-csv)](https://pypi.org/project/airflow-postgres-csv/)

Airflow 3 operators for bulk PostgreSQL <-> CSV transfers using `COPY`.

## Operators

- **`PostgresToCsvOperator`** - Run a SQL query and export results to a CSV file
- **`CsvToPostgresOperator`** - Load a CSV file into a PostgreSQL table

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
    sql="SELECT * FROM users WHERE active = %(active)s",
    parameters={"active": True},
    csv_file_path="/tmp/users.csv",
)

# Load CSV into a table (with truncate)
import_task = CsvToPostgresOperator(
    task_id="import_users",
    conn_id="my_postgres",
    table_name="staging.users",
    csv_file_path="/tmp/users.csv",
    truncate=True,
)
```

### SQL from file

The `sql` parameter supports multiple formats:

```python
# Inline SQL
PostgresToCsvOperator(sql="SELECT * FROM users", ...)

# Relative path (loaded by Airflow from DAG folder or template_searchpath)
PostgresToCsvOperator(sql="sql/export_users.sql", ...)

# Absolute path (loaded directly)
PostgresToCsvOperator(sql="/opt/airflow/sql/export_users.sql", ...)
```

### Gzip compression

Both operators support gzip compression for large files:

```python
# Export to gzip
PostgresToCsvOperator(
    sql="SELECT * FROM large_table",
    csv_file_path="/tmp/data.csv.gz",
    compression="gzip",
    ...
)

# Import from gzip
CsvToPostgresOperator(
    csv_file_path="/tmp/data.csv.gz",
    compression="gzip",
    ...
)
```

## Parameters

### PostgresToCsvOperator

| Parameter | Description | Default |
|-----------|-------------|---------|
| `conn_id` | Airflow Postgres connection ID | required |
| `csv_file_path` | Output file path (templated) | required |
| `sql` | SQL query string, or path to `.sql` file | required |
| `parameters` | Dict passed to `cursor.mogrify` | `{}` |
| `has_header` | Include CSV header row | `True` |
| `compression` | Compression format (`"gzip"` or `None`) | `None` |
| `timeout` | Query timeout in minutes | `60` |

### CsvToPostgresOperator

| Parameter | Description | Default |
|-----------|-------------|---------|
| `conn_id` | Airflow Postgres connection ID | required |
| `table_name` | Target table (templated, supports `schema.table`) | required |
| `csv_file_path` | Input file path (templated) | required |
| `columns` | Explicit column list | `None` |
| `has_header` | CSV has header row | `True` |
| `truncate` | Truncate table before loading | `False` |
| `compression` | Compression format (`"gzip"` or `None`) | `None` |
| `delimiter` | CSV delimiter | `","` |
| `quote_char` | CSV quote character | `'"'` |
| `null_string` | String representing NULL | `""` |
| `timeout` | Query timeout in minutes | `60` |

## Requirements

- Apache Airflow >= 3.0.0
- apache-airflow-providers-postgres >= 6.0.0

## License

MIT
