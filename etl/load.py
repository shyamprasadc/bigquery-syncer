import os
from typing import Any, Dict, List

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from .logger import get_logger

logger = get_logger("etl.load")


class BigQueryLoader:
    def __init__(self, project: str, dataset: str, location: str = "US"):
        self.client = bigquery.Client(project=project)
        self.dataset = dataset
        self.location = location
        self.ensure_dataset()

    def ensure_dataset(self) -> None:
        dataset_ref = bigquery.DatasetReference(self.client.project, self.dataset)
        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = self.location
            self.client.create_dataset(ds)

    def ensure_table(self, table: str) -> None:
        ref = bigquery.TableReference(
            bigquery.DatasetReference(self.client.project, self.dataset), table
        )
        try:
            self.client.get_table(ref)
        except NotFound:
            t = bigquery.Table(ref)
            t.location = self.location
            self.client.create_table(t)

    def load_append(self, table: str, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        ref = f"{self.client.project}.{self.dataset}.{table}"
        job_config = bigquery.LoadJobConfig()
        job = self.client.load_table_from_json(rows, ref, job_config=job_config)
        result = job.result()
        return result.output_rows or len(rows)

    def load_upsert(
        self, table: str, rows: List[Dict[str, Any]], primary_keys: List[str]
    ) -> int:
        if not rows:
            return 0
        self.ensure_table(table)
        # Create temp table
        temp_table_name = f"_{table}_staging_{os.getpid()}"
        temp_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.client.project, self.dataset),
            temp_table_name,
        )
        self.client.delete_table(temp_ref, not_found_ok=True)
        temp_table = bigquery.Table(temp_ref)
        self.client.create_table(temp_table)

        # Load into temp
        job_config = bigquery.LoadJobConfig(
            autodetect=True, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = self.client.load_table_from_json(rows, temp_ref, job_config=job_config)
        job.result()

        # Build MERGE
        target = f"`{self.client.project}.{self.dataset}.{table}`"
        staging = f"`{self.client.project}.{self.dataset}.{temp_table_name}`"
        on_clause = " AND ".join([f"T.{k} = S.{k}" for k in primary_keys])
        all_cols = list(rows[0].keys())
        non_keys = [c for c in all_cols if c not in primary_keys]
        update_clause = ", ".join([f"{c} = S.{c}" for c in non_keys])
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"S.{c}" for c in all_cols])
        if non_keys:
            merge_sql = f"""
			MERGE {target} T
			USING {staging} S
			ON {on_clause}
			WHEN MATCHED THEN UPDATE SET {update_clause}
			WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
			"""
        else:
            merge_sql = f"""
			MERGE {target} T
			USING {staging} S
			ON {on_clause}
			WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
			"""
        query_job = self.client.query(merge_sql)
        query_job.result()

        # Cleanup
        self.client.delete_table(temp_ref, not_found_ok=True)
        return len(rows)
