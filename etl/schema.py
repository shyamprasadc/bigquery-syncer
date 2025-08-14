from typing import Dict, List, Tuple
from google.cloud import bigquery


MYSQL_TO_BQ_TYPE = {
    "int": "INT64",
    "bigint": "INT64",
    "smallint": "INT64",
    "tinyint": "INT64",
    "float": "FLOAT64",
    "double": "FLOAT64",
    "decimal": "NUMERIC",
    "varchar": "STRING",
    "char": "STRING",
    "text": "STRING",
    "longtext": "STRING",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "time": "TIME",
    "json": "STRING",
}


def normalize_mysql_type(mysql_type: str) -> str:
    base = mysql_type.lower().split("(")[0].strip()
    return MYSQL_TO_BQ_TYPE.get(base, "STRING")


def get_bq_table_schema(
    client: bigquery.Client, dataset: str, table: str
) -> List[bigquery.SchemaField]:
    ref = bigquery.TableReference(
        bigquery.DatasetReference(client.project, dataset), table
    )
    table_obj = client.get_table(ref)
    return list(table_obj.schema)


def schema_difference(
    mysql_columns: List[Tuple[str, str]], bq_schema: List[bigquery.SchemaField]
) -> Dict[str, List]:
    bq_fields = {f.name: f for f in bq_schema}
    missing = []
    type_mismatches = []
    for name, mysql_type in mysql_columns:
        expected_type = normalize_mysql_type(mysql_type)
        if name not in bq_fields:
            missing.append((name, expected_type))
        else:
            if bq_fields[name].field_type != expected_type:
                type_mismatches.append(
                    (name, bq_fields[name].field_type, expected_type)
                )
    return {"missing": missing, "type_mismatches": type_mismatches}


def add_missing_columns(
    client: bigquery.Client, dataset: str, table: str, columns: List[Tuple[str, str]]
) -> None:
    if not columns:
        return
    ref = bigquery.TableReference(
        bigquery.DatasetReference(client.project, dataset), table
    )
    table_obj = client.get_table(ref)
    new_schema = list(table_obj.schema)
    for name, field_type in columns:
        new_schema.append(
            bigquery.SchemaField(name=name, field_type=field_type, mode="NULLABLE")
        )
    table_obj.schema = new_schema
    client.update_table(table_obj, ["schema"])
