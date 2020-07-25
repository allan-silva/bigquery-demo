import os

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "balrog")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")


client = bigquery.Client()


def dataset_id(name):
    return f"{client.project}.{name}"


def table_id(dataset, table_name):
    return f"{client.project}.{dataset.dataset_id}.{table_name}"


def get_dataset(name):
    did = dataset_id(name)
    try:
        return client.get_dataset(did)
    except NotFound:
        print(f"Dataset `{name}` not found - creating...")
        dataset = bigquery.Dataset(did)
        dataset.location = BIGQUERY_LOCATION
        dataset.description = "Balrog BigQuery Dataset"
        return client.create_dataset(dataset)


def get_table(dataset, table_name):
    tid = table_id(dataset, table_name)
    try:
        return client.get_table(tid)
    except NotFound:
        print(f"Table `{table_name}` not found - creating...")
        table = bigquery.Table(tid)
        return client.create_table(table)


def get_update_log_table_schema():
    return [
        bigquery.SchemaField(
            "full_url", "STRING", mode="REQUIRED", description="Full update url."),
        bigquery.SchemaField(
            "url_version", "STRING", mode="REQUIRED", description="Url version.")
    ]


def migrate_schema(table, schema):
    current_fields = [field.name for field in table.schema]
    new_fields = [field for field in schema if field.name not in current_fields]
    if new_fields:
        print("Updating table schema")
        new_schema = table.schema[:]
        new_schema.extend(new_fields)
        table.schema = new_schema
        return client.update_table(table, ["schema"])
    return table


def run():
    balrog_dataset = get_dataset(BIGQUERY_DATASET)
    update_log_table = get_table(balrog_dataset, "update_log")
    update_log_table = migrate_schema(update_log_table, get_update_log_table_schema())
    print([field for field in update_log_table.schema])


__name__ == "__main__" and run()
