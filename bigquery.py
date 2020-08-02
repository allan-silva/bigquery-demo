import re
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
            "url_version", "STRING", mode="REQUIRED", description="Url version."),
        bigquery.SchemaField(
            "product", "STRING", mode="REQUIRED", description="The name of the application requesting an update."
        ),
        bigquery.SchemaField(
            "version", "STRING", description="The version of the application requesting an update. Must be at least a two-part version string."
        ),
        bigquery.SchemaField(
            "buildID", "STRING", description="The build ID of the application requesting an update."
        ),
        bigquery.SchemaField(
            "buildTarget", "STRING", description="The “build target” of the application requesting an update. This is usually related to the target platform the app was built for."
        ),
        bigquery.SchemaField(
            "locale", "STRING", description="The locale of the application requesting an update."
        ),
        bigquery.SchemaField(
            "channel", "STRING", mode="REQUIRED", description="The update channel of the application request an update."
        ),
        bigquery.SchemaField(
            "osVersion", "STRING", description="The OS Version of the application requesting an update. This field is primarily used to point desupported operating systems to their last supported build."
        ),
        bigquery.SchemaField(
            "distribution", "STRING", description="The partner distributions names that the application must send in order for the rule to match or “default” if the application is not a partner build. A comma separated list may be used to list multiple distributions"
        ),
        bigquery.SchemaField(
            "distVersion", "STRING", description="The version of the partner distribution of the application requesting an update or “default” if the application is not a partner build."
        ),
        bigquery.SchemaField(
            "platformVersion", "STRING", description="Platform Version"
        ),
        bigquery.SchemaField(
            "IMEI", "STRING", description="IMEI"
        ),
        bigquery.SchemaField(
            "systemCapabilities", "STRING", description="The supported hardware features of the application requesting an update. This field is primarily used to point desupported users based on their hardware. Eg: users who do not support SSE2."
        )
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


def insert_data(update_log_table, rows):
    r = client.insert_rows(update_log_table, rows)
    print(r)


def clear(dataset_name):
    client.delete_dataset(
        dataset_id(dataset_name), delete_contents=True)


PRODUCT = r"(?P<product>\w+)"
VERSION = r"(?P<version>[\w\d.]+)"
BUILD_ID = r"(?P<buildID>[\w\d]+)"
BUILD_TARGET = r"(?P<buildTarget>[\w\d_-]+)"
LOCALE = r"(?P<locale>[\w-]+)"
CHANNEL = r"(?P<channel>[\w]+)"


def extract_url_v1_data(url):
    url_pattern = fr"/update/1/{PRODUCT}/{VERSION}/{BUILD_ID}/{BUILD_TARGET}/{LOCALE}/{CHANNEL}/update.xml"
    return extract_parameters(url_pattern, url)


def extract_parameters(pattern, url):
    parameters = {}
    url_match = re.match(pattern, url)
    if url_match:
        parameters.update(url_match.groupdict())
    return parameters


def extract_data_from_url(url):
    url_version_pattern = r"/update/(?P<url_version>\d+)/"
    url_version_match = re.match(url_version_pattern, url)
    if url_version_match:
        ext_fn = {
            "1": extract_url_v1_data
        }
        url_version = url_version_match.group("url_version")
        url_data = {"url_version": url_version}
        url_data.update(ext_fn[url_version](url))
        return url_data

    return {}


def run():
    # clear(BIGQUERY_DATASET)
    balrog_dataset = get_dataset(BIGQUERY_DATASET)
    update_log_table = get_table(balrog_dataset, "update_log")
    update_log_table = migrate_schema(update_log_table, get_update_log_table_schema())

    urls = [
        "/update/1/Firefox/75.0/20200403170909/Darwin_x86_64-gcc3-u-i386-x86_64/pt-BR/release/update.xml"
    ]

    rows = []

    for url in urls:
        data = extract_data_from_url(url)
        data["full_url"] = url
        rows.append(data)

    insert_data(update_log_table, rows)


__name__ == "__main__" and run()
