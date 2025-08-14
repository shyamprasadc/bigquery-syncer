import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .logger import get_logger

logger = get_logger("etl.extract")


class MySQLExtractor:
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params

    def _connect(self):
        return mysql.connector.connect(**self.connection_params)

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(Error),
    )
    def fetch_columns(self, table: str) -> List[Tuple[str, str]]:
        query = f"SHOW COLUMNS FROM `{table}`"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        return [(row[0], str(row[1])) for row in rows]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type(Error),
    )
    def fetch_incremental(
        self,
        table: str,
        columns: List[str],
        incremental_column: str,
        start_value: Any,
        end_value: Optional[Any] = None,
        batch_size: int = 5000,
        operator: str = ">",
    ) -> List[Dict[str, Any]]:
        if operator not in (">", ">="):
            raise ValueError("operator must be '>' or '>='")
        cols = ", ".join([f"`{c}`" for c in columns])
        if end_value is None:
            query = f"SELECT {cols} FROM `{table}` WHERE `{incremental_column}` {operator} %s ORDER BY `{incremental_column}` ASC LIMIT %s"
            params = (start_value, batch_size)
        else:
            lt_op = "<=" if operator == ">=" else "<"
            query = f"SELECT {cols} FROM `{table}` WHERE `{incremental_column}` {operator} %s AND `{incremental_column}` {lt_op} %s ORDER BY `{incremental_column}` ASC LIMIT %s"
            params = (start_value, end_value, batch_size)
        with self._connect() as conn:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return rows


def build_mysql_params() -> Dict[str, Any]:
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
        "autocommit": True,
    }
