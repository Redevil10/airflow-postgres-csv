"""Tests for PostgresToCsvOperator and CsvToPostgresOperator."""

import os
import tempfile
from unittest.mock import MagicMock, mock_open, patch

import pytest
from airflow.exceptions import AirflowException

from airflow_postgres_csv.operators import CsvToPostgresOperator, PostgresToCsvOperator


@pytest.fixture
def mock_pg_hook():
    with patch(
        "airflow_postgres_csv.operators.PostgresHook"
    ) as mock_hook_cls:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.mogrify.return_value = b"SELECT 1"
        mock_cursor.rowcount = 42

        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook_cls.return_value.get_conn.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_hook_cls.return_value.get_conn.return_value.__exit__ = MagicMock(
            return_value=False
        )

        yield {
            "hook_cls": mock_hook_cls,
            "conn": mock_conn,
            "cursor": mock_cursor,
        }


class TestPostgresToCsvOperator:
    def test_raises_when_no_query_or_file(self, mock_pg_hook):
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path="/tmp/out.csv",
        )
        with pytest.raises(AirflowException, match="Either sql_query or sql_file_path"):
            op.execute(context={})

    def test_executes_with_sql_query(self, mock_pg_hook, tmp_path):
        csv_path = str(tmp_path / "out.csv")
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql_query="SELECT * FROM users",
        )
        result = op.execute(context={})
        assert result == csv_path
        mock_pg_hook["cursor"].copy_expert.assert_called_once()

    def test_reads_sql_from_file(self, mock_pg_hook, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT 1")
        csv_path = str(tmp_path / "out.csv")

        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql_file_path=str(sql_file),
        )
        op.execute(context={})
        mock_pg_hook["cursor"].mogrify.assert_called_once()

    def test_strips_trailing_semicolon(self, mock_pg_hook, tmp_path):
        csv_path = str(tmp_path / "out.csv")
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql_query="SELECT 1;  ",
        )
        op.execute(context={})
        call_args = mock_pg_hook["cursor"].mogrify.call_args
        assert not call_args[0][0].endswith(";")

    def test_no_header(self, mock_pg_hook, tmp_path):
        csv_path = str(tmp_path / "out.csv")
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql_query="SELECT 1",
            has_header=False,
        )
        op.execute(context={})
        copy_call = mock_pg_hook["cursor"].copy_expert.call_args[0][0]
        assert "HEADER" not in copy_call


class TestCsvToPostgresOperator:
    def test_raises_when_file_missing(self, mock_pg_hook):
        op = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path="/nonexistent/file.csv",
        )
        with pytest.raises(AirflowException, match="CSV file not found"):
            op.execute(context={})

    def test_loads_csv(self, mock_pg_hook, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        op = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path=str(csv_file),
        )
        result = op.execute(context={})
        assert result == 42  # mocked rowcount
        mock_pg_hook["cursor"].copy_expert.assert_called_once()
        mock_pg_hook["conn"].commit.assert_called_once()

    def test_schema_qualified_table(self, mock_pg_hook, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        op = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="staging.my_table",
            csv_file_path=str(csv_file),
        )
        op.execute(context={})
        copy_call = mock_pg_hook["cursor"].copy_expert.call_args[0][0]
        # Should contain quoted identifiers
        assert "staging" in copy_call
        assert "my_table" in copy_call

    def test_explicit_columns(self, mock_pg_hook, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col_a,col_b\n1,2\n")

        op = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path=str(csv_file),
            columns=["col_a", "col_b"],
        )
        op.execute(context={})
        copy_call = mock_pg_hook["cursor"].copy_expert.call_args[0][0]
        assert "col_a" in copy_call
        assert "col_b" in copy_call
        # Should NOT have HEADER since columns are explicit
        assert "HEADER" not in copy_call
