"""Custom Airflow operators for PostgreSQL <-> CSV file transfers."""

import gzip
import os
from collections.abc import Sequence

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.operator import BaseOperator


class PostgresToCsvOperator(BaseOperator):
    """
    Execute a SQL query on PostgreSQL and save the result as a CSV file.

    Uses ``COPY (...) TO STDOUT WITH CSV`` for high-performance bulk export.

    :param conn_id: Airflow connection ID for the PostgreSQL database.
    :param csv_file_path: Local file path where the CSV will be saved.
    :param sql_query: SQL query to execute. Overrides *sql_file_path* if both given.
    :param sql_file_path: Path to a ``.sql`` file containing the query.
    :param query_params: Parameters passed to the SQL query via ``cursor.mogrify``.
    :param has_header: Include a CSV header row. Defaults to ``True``.
    :param compression: Compression format. Supports ``"gzip"``.
        Defaults to ``None`` (no compression).
    :param timeout: Query timeout in minutes. Defaults to ``60``.
    """

    template_fields: Sequence[str] = (
        "sql_query",
        "sql_file_path",
        "csv_file_path",
    )

    def __init__(
        self,
        conn_id: str,
        csv_file_path: str,
        sql_query: str | None = None,
        sql_file_path: str | None = None,
        query_params: dict | None = None,
        has_header: bool = True,
        compression: str | None = None,
        timeout: int = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.csv_file_path = csv_file_path
        self.sql_query = sql_query
        self.sql_file_path = sql_file_path
        self.query_params = query_params or {}
        self.has_header = has_header
        self.compression = compression
        self.timeout = timeout

    def execute(self, context):
        if not self.sql_query and not self.sql_file_path:
            raise AirflowException("Either sql_query or sql_file_path must be provided")

        if not self.sql_query:
            with open(self.sql_file_path, encoding="utf-8") as f:
                self.sql_query = f.read()

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        cleaned_sql = self.sql_query.strip().rstrip(";")

        self.log.info("Running query and saving to CSV: %s", self.csv_file_path)

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = %s;", (self.timeout * 60 * 1000,))
                formatted_sql = cursor.mogrify(cleaned_sql, self.query_params).decode("utf-8")

                header_clause = " HEADER" if self.has_header else ""
                copy_command = f"COPY ({formatted_sql}) TO STDOUT WITH CSV{header_clause}"

                rows = 0
                open_func = self._get_open_func()
                with open_func(self.csv_file_path, "wt", encoding="utf-8") as csv_file:
                    cursor.copy_expert(copy_command, csv_file)
                    rows = cursor.rowcount

        self.log.info(
            "CSV saved: %s (%s rows, %s)",
            self.csv_file_path,
            rows if rows >= 0 else "unknown",
            "with header" if self.has_header else "no header",
        )
        return self.csv_file_path

    def _get_open_func(self):
        """Return the appropriate file open function based on compression setting."""
        return gzip.open if self.compression == "gzip" else open


class CsvToPostgresOperator(BaseOperator):
    """
    Load a CSV file into a PostgreSQL table.

    Uses ``COPY ... FROM STDIN WITH CSV`` for high-performance bulk import.

    :param conn_id: Airflow connection ID for the PostgreSQL database.
    :param table_name: Target table (may include schema, e.g. ``"myschema.mytable"``).
    :param csv_file_path: Local file path of the CSV to load.
    :param delimiter: CSV delimiter. Defaults to ``','``.
    :param quote_char: CSV quote character. Defaults to ``'"'``.
    :param null_string: String representing NULL values. Defaults to ``''``.
    :param has_header: Whether the CSV has a header row. Defaults to ``True``.
    :param columns: Explicit column list. If provided, maps CSV columns to these
        table columns and skips the file header (if present).
    :param truncate: Truncate the table before loading. Defaults to ``False``.
    :param compression: Compression format. Supports ``"gzip"``.
        Defaults to ``None`` (no compression).
    :param timeout: Query timeout in minutes. Defaults to ``60``.
    """

    template_fields: Sequence[str] = ("csv_file_path", "table_name")

    def __init__(
        self,
        conn_id: str,
        table_name: str,
        csv_file_path: str,
        delimiter: str = ",",
        quote_char: str = '"',
        null_string: str = "",
        has_header: bool = True,
        columns: list[str] | None = None,
        truncate: bool = False,
        compression: str | None = None,
        timeout: int = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.null_string = null_string
        self.has_header = has_header
        self.columns = columns
        self.truncate = truncate
        self.compression = compression
        self.timeout = timeout

    def execute(self, context):
        if not os.path.exists(self.csv_file_path):
            raise AirflowException(f"CSV file not found: {self.csv_file_path}")

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Loading %s into %s", self.csv_file_path, self.table_name)

        column_clause = self._build_column_clause()
        header_clause = "HEADER" if self.has_header and not self.columns else ""

        copy_command = (
            f"COPY {self._quote_table_name()} {column_clause} "
            f"FROM STDIN WITH CSV "
            f"DELIMITER '{self.delimiter}' "
            f"QUOTE '{self.quote_char}' "
            f"NULL '{self.null_string}' "
            f"{header_clause}"
        )

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = %s;", (self.timeout * 60 * 1000,))
                if self.truncate:
                    self.log.info("Truncating table %s", self.table_name)
                    cursor.execute(f"TRUNCATE {self._quote_table_name()}")
                open_func = self._get_open_func()
                with open_func(self.csv_file_path, "rt", encoding="utf-8") as csv_file:
                    if self.columns and self.has_header:
                        next(csv_file)
                    cursor.copy_expert(copy_command, csv_file)
                rows = cursor.rowcount
            conn.commit()

        self.log.info(
            "Loaded %s rows from %s into %s",
            rows if rows >= 0 else "unknown",
            self.csv_file_path,
            self.table_name,
        )
        return rows

    @staticmethod
    def _quote_identifier(name: str) -> str:
        """Quote a SQL identifier, escaping any double quotes."""
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def _quote_table_name(self) -> str:
        parts = self.table_name.split(".")
        return ".".join(self._quote_identifier(p) for p in parts)

    def _build_column_clause(self) -> str:
        if not self.columns:
            return ""
        cols = ", ".join(self._quote_identifier(c) for c in self.columns)
        return f"({cols})"

    def _get_open_func(self):
        """Return the appropriate file open function based on compression setting."""
        return gzip.open if self.compression == "gzip" else open
