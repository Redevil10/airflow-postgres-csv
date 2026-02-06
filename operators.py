"""Custom Airflow operators for PostgreSQL <-> CSV file transfers."""

import os
from typing import List, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.operator import BaseOperator
from psycopg2 import sql as psycopg2_sql


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
        self.timeout = timeout

    def execute(self, context):
        if not self.sql_query and not self.sql_file_path:
            raise AirflowException("Either sql_query or sql_file_path must be provided")

        if not self.sql_query:
            with open(self.sql_file_path, "r", encoding="utf-8") as f:
                self.sql_query = f.read()

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        cleaned_sql = self.sql_query.strip().rstrip(";")

        self.log.info("Running query and saving to CSV: %s", self.csv_file_path)

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SET statement_timeout = %s;", (self.timeout * 60 * 1000,)
                )
                formatted_sql = cursor.mogrify(cleaned_sql, self.query_params).decode(
                    "utf-8"
                )

                header_clause = " HEADER" if self.has_header else ""
                copy_command = f"COPY ({formatted_sql}) TO STDOUT WITH CSV{header_clause}"

                rows = 0
                with open(self.csv_file_path, "w", encoding="utf-8") as csv_file:
                    cursor.copy_expert(copy_command, csv_file)
                    rows = cursor.rowcount

        self.log.info(
            "CSV saved: %s (%s rows, %s)",
            self.csv_file_path,
            rows if rows >= 0 else "unknown",
            "with header" if self.has_header else "no header",
        )
        return self.csv_file_path


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
        columns: Optional[List[str]] = None,
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
        self.timeout = timeout

    def execute(self, context):
        if not os.path.exists(self.csv_file_path):
            raise AirflowException(f"CSV file not found: {self.csv_file_path}")

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Loading %s into %s", self.csv_file_path, self.table_name)

        # Build COPY command with safe identifier quoting
        table_ident = self._build_table_identifier()
        column_clause = self._build_column_clause()
        header_clause = self._build_header_clause()

        copy_sql = psycopg2_sql.SQL(
            "COPY {table} {columns} FROM STDIN WITH CSV "
            "DELIMITER {delimiter} QUOTE {quote} NULL {null_str} {header}"
        ).format(
            table=table_ident,
            columns=column_clause,
            delimiter=psycopg2_sql.Literal(self.delimiter),
            quote=psycopg2_sql.Literal(self.quote_char),
            null_str=psycopg2_sql.Literal(self.null_string),
            header=psycopg2_sql.SQL(header_clause),
        )

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SET statement_timeout = %s;", (self.timeout * 60 * 1000,)
                )
                copy_command = copy_sql.as_string(conn)
                with open(self.csv_file_path, "r", encoding="utf-8") as csv_file:
                    if self.columns and self.has_header:
                        next(csv_file)  # skip file header when using explicit columns
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

    def _build_table_identifier(self) -> psycopg2_sql.Composable:
        """Build a safely-quoted table identifier, handling schema.table format."""
        parts = self.table_name.split(".")
        return psycopg2_sql.SQL(".").join(psycopg2_sql.Identifier(p) for p in parts)

    def _build_column_clause(self) -> psycopg2_sql.Composable:
        if not self.columns:
            return psycopg2_sql.SQL("")
        cols = psycopg2_sql.SQL(", ").join(
            psycopg2_sql.Identifier(c) for c in self.columns
        )
        return psycopg2_sql.SQL("({})").format(cols)

    def _build_header_clause(self) -> str:
        if self.has_header and not self.columns:
            return "HEADER"
        return ""
