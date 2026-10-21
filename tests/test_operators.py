"""Tests for PostgresToCsvOperator and CsvToPostgresOperator."""

import gzip
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from airflow_postgres_csv.operators import CsvToPostgresOperator, PostgresToCsvOperator


@pytest.fixture
def mock_pg_hook():
    with patch("airflow_postgres_csv.operators.PostgresHook") as mock_hook_cls:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.mogrify.return_value = b"SELECT 1"
        mock_cursor.rowcount = 42

        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook_cls.return_value.get_conn.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_hook_cls.return_value.get_conn.return_value.__exit__ = MagicMock(return_value=False)

        yield {
            "hook_cls": mock_hook_cls,
            "conn": mock_conn,
            "cursor": mock_cursor,
        }


class TestPostgresToCsvOperator:
    def test_executes_with_sql_query(self, mock_pg_hook, tmp_path):
        csv_path = str(tmp_path / "out.csv")
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql="SELECT * FROM users",
        )
        result = op.execute(context={})
        assert result == csv_path
        mock_pg_hook["cursor"].copy_expert.assert_called_once()

    def test_reads_sql_from_file(self, mock_pg_hook, tmp_path):
        """Test absolute path fallback when Airflow templating doesn't load the file."""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM fallback_table")
        csv_path = str(tmp_path / "out.csv")

        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql=str(sql_file),  # Absolute path
        )
        op.execute(context={})
        # Verify the file content was loaded and passed to mogrify
        call_args = mock_pg_hook["cursor"].mogrify.call_args[0][0]
        assert "fallback_table" in call_args

    def test_strips_trailing_semicolon(self, mock_pg_hook, tmp_path):
        csv_path = str(tmp_path / "out.csv")
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql="SELECT 1;  ",
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
            sql="SELECT 1",
            has_header=False,
        )
        op.execute(context={})
        copy_call = mock_pg_hook["cursor"].copy_expert.call_args[0][0]
        assert "HEADER" not in copy_call

    def test_gzip_compression(self, mock_pg_hook, tmp_path):
        csv_path = str(tmp_path / "out.csv.gz")
        op = PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql="SELECT 1",
            compression="gzip",
        )
        op.execute(context={})
        # Verify file was created and is gzip format
        assert (tmp_path / "out.csv.gz").exists()
        with gzip.open(csv_path, "rt") as f:
            f.read()  # Should not raise


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

    def test_truncate_before_load(self, mock_pg_hook, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        op = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path=str(csv_file),
            truncate=True,
        )
        op.execute(context={})
        # Verify TRUNCATE was called
        execute_calls = mock_pg_hook["cursor"].execute.call_args_list
        truncate_called = any("TRUNCATE" in str(call) for call in execute_calls)
        assert truncate_called

    def test_gzip_compression(self, mock_pg_hook, tmp_path):
        csv_file = tmp_path / "data.csv.gz"
        with gzip.open(csv_file, "wt") as f:
            f.write("a,b\n1,2\n")

        op = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path=str(csv_file),
            compression="gzip",
        )
        result = op.execute(context={})
        assert result == 42  # mocked rowcount
        mock_pg_hook["cursor"].copy_expert.assert_called_once()


# ---------------------------------------------------------------------------
# Airflow 2 compatibility tests
#
# In Airflow 3, airflow.models.BaseOperator is a re-export of
# airflow.sdk.bases.operator.BaseOperator, so these tests run in either version.
# The try/except in operators.py is what enables the code to import on a real
# Airflow 2 install where airflow.sdk does not exist.
# ---------------------------------------------------------------------------


class TestAirflow2Compatibility:
    """
    Verify that operators are compatible with the Airflow 2 import path
    (``airflow.models.BaseOperator``).

    A real Airflow 2 environment cannot be simulated in an Airflow 3 process
    because Airflow 3 re-exports ``BaseOperator`` through the same ``airflow.sdk``
    machinery from both import paths.  These tests confirm that:

    * ``from airflow.models import BaseOperator`` resolves successfully (the
      fallback import the operators use on Airflow 2).
    * Both operators are subclasses of that ``BaseOperator``.
    * Both operators execute correctly when invoked the Airflow 2 way.
    """

    def test_fallback_import_target_is_accessible(self):
        """airflow.models.BaseOperator can be imported (the Airflow 2 fallback path)."""
        from airflow.models import BaseOperator

        assert BaseOperator is not None

    def test_operators_subclass_models_baseoperator(self):
        """Both operators are subclasses of airflow.models.BaseOperator."""
        from airflow.models import BaseOperator

        from airflow_postgres_csv.operators import CsvToPostgresOperator, PostgresToCsvOperator

        assert issubclass(PostgresToCsvOperator, BaseOperator)
        assert issubclass(CsvToPostgresOperator, BaseOperator)

    def test_postgres_to_csv_is_instance_of_models_baseoperator(self, mock_pg_hook, tmp_path):
        """A PostgresToCsvOperator instance is recognised as a BaseOperator on Airflow 2."""
        from airflow.models import BaseOperator

        from airflow_postgres_csv.operators import PostgresToCsvOperator

        csv_path = str(tmp_path / "out.csv")
        op = PostgresToCsvOperator(
            task_id="test_af2",
            conn_id="test_conn",
            csv_file_path=csv_path,
            sql="SELECT 1",
        )
        assert isinstance(op, BaseOperator)
        result = op.execute(context={})
        assert result == csv_path
        mock_pg_hook["cursor"].copy_expert.assert_called_once()

    def test_csv_to_postgres_is_instance_of_models_baseoperator(self, mock_pg_hook, tmp_path):
        """A CsvToPostgresOperator instance is recognised as a BaseOperator on Airflow 2."""
        from airflow.models import BaseOperator

        from airflow_postgres_csv.operators import CsvToPostgresOperator

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        op = CsvToPostgresOperator(
            task_id="test_af2",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path=str(csv_file),
        )
        assert isinstance(op, BaseOperator)
        result = op.execute(context={})
        assert result == 42  # mocked rowcount
        mock_pg_hook["cursor"].copy_expert.assert_called_once()


# ---------------------------------------------------------------------------
# psycopg3 compatibility tests
#
# Newer apache-airflow-providers-postgres versions return psycopg3 connections
# when SQLAlchemy 2 and psycopg are installed. psycopg3 has no ``copy_expert``
# and its default cursor has no ``mogrify``.
# ---------------------------------------------------------------------------


class _FakeCopy:
    def __init__(self, chunks=()):
        self.chunks = list(chunks)
        self.written = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        return iter(self.chunks)

    def write(self, data):
        self.written.append(data)


@pytest.fixture
def mock_pg3_hook(mock_pg_hook):
    pytest.importorskip("psycopg")
    cursor = mock_pg_hook["cursor"]
    del cursor.copy_expert
    del cursor.mogrify
    mock_pg_hook["conn"].info.encoding = "utf-8"
    with patch("psycopg.ClientCursor") as client_cursor_cls:
        client_cursor_cls.return_value.mogrify.return_value = "SELECT 1"
        yield {**mock_pg_hook, "client_cursor_cls": client_cursor_cls}


class TestPsycopg3Compatibility:
    def test_statement_timeout_does_not_use_set_with_bind_param(self, mock_pg3_hook, tmp_path):
        mock_pg3_hook["cursor"].copy.return_value = _FakeCopy()
        PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=str(tmp_path / "out.csv"),
            sql="SELECT 1",
            timeout=2,
        ).execute(context={})
        sql, params = mock_pg3_hook["cursor"].execute.call_args_list[0][0]
        assert "set_config('statement_timeout'" in sql
        assert params == ("120000",)

    def test_postgres_to_csv_uses_copy(self, mock_pg3_hook, tmp_path):
        # Split a multi-byte UTF-8 character across chunks.
        payload = "id,name\n1,café\n".encode()
        chunks = [payload[:13], payload[13:]]
        mock_pg3_hook["cursor"].copy.return_value = _FakeCopy(chunks)
        csv_path = tmp_path / "out.csv"

        PostgresToCsvOperator(
            task_id="test",
            conn_id="test_conn",
            csv_file_path=str(csv_path),
            sql="SELECT * FROM t WHERE id = %(id)s",
            parameters={"id": 1},
        ).execute(context={})

        mock_pg3_hook["client_cursor_cls"].return_value.mogrify.assert_called_once_with(
            "SELECT * FROM t WHERE id = %(id)s", {"id": 1}
        )
        copy_sql = mock_pg3_hook["cursor"].copy.call_args[0][0]
        assert copy_sql == "COPY (SELECT 1) TO STDOUT WITH CSV HEADER"
        assert csv_path.read_text(encoding="utf-8") == "id,name\n1,café\n"

    def test_csv_to_postgres_uses_copy(self, mock_pg3_hook, tmp_path):
        fake_copy = _FakeCopy()
        mock_pg3_hook["cursor"].copy.return_value = fake_copy
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")

        result = CsvToPostgresOperator(
            task_id="test",
            conn_id="test_conn",
            table_name="my_table",
            csv_file_path=str(csv_file),
            columns=["a", "b"],
        ).execute(context={})

        assert result == 42
        assert "".join(fake_copy.written) == "1,2\n"
        mock_pg3_hook["conn"].commit.assert_called_once()
