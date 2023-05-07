# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "liquid-optics-384501"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

files = [
    "events",
    "addresses",
    "order_items",
    "orders",
    "products",
    "promos",
    "users",
]

events=[
    bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("user", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("order", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("product", bigquery.SqlTypeNames.STRING),
]

addresses=[
    bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("zipcode", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("state", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("country", bigquery.SqlTypeNames.STRING),
]

order_items=[
    bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("quantity", bigquery.SqlTypeNames.INT64),
]

orders=[
    bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT64),
    bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT64),
    bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT64),
    bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("user", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("promo", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
]

products=[
    bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("name", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("price", bigquery.SqlTypeNames.FLOAT64),
    bigquery.SchemaField("inventory", bigquery.SqlTypeNames.INT64),
]

promos=[
    bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("discount", bigquery.SqlTypeNames.INT64),
    bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
]

users=[
    bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
]

def set_schema(file):
    if file == "events":
        return events
    elif file == "addresses":
        return addresses
    elif file == "order_items":
        return order_items
    elif file == "orders":
        return orders
    elif file == "products":
        return products
    elif file == "promos":
        return promos
    else:
        return users

def config(file):
    if file in ["events","orders","users"]:
        if file == "users":
            return bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=set_schema(file),
                source_format=bigquery.SourceFormat.CSV,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                ),
                clustering_fields=["first_name", "last_name"],
            )
        else:
            return bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=set_schema(file),
                source_format=bigquery.SourceFormat.CSV,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                ),
            )
    else:
        return bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=set_schema(file),
            source_format=bigquery.SourceFormat.CSV,
        )

def set_table_id(file):
    if file == "events":
        dt = "2021-02-10"
        partition = dt.replace("-", "")
        return f"{project_id}.deb_bootcamp.{file}${partition}"
    elif file == "orders":
        dt = "2021-02-10"
        partition = dt.replace("-", "")
        return f"{project_id}.deb_bootcamp.{file}${partition}"
    elif file == "users":
        dt = "2020-10-23"
        partition = dt.replace("-", "")
        return f"{project_id}.deb_bootcamp.{file}${partition}"
    else:
        return f"{project_id}.deb_bootcamp.{file}"


for file in files:
    job_config = config(file)
    file_path = f"./data/{file}.csv"
    df = pd.read_csv(file_path)
    df.info()

    table_id = set_table_id(file)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")