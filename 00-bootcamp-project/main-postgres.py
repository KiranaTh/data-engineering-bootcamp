import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data"

table = "addresses"
header = ["address_id", "address", "zipcode", "state", "country"]
with open(f"{DATA_FOLDER}/addresses.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)


table = "order_items"
header = ["order_id", "product_id", "quantity"]
with open(f"{DATA_FOLDER}/{table}.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)

table = "events"
header = ["event_id","session_id","user_id","page_url","created_at","event_type","order_id","product_id"]
with open(f"{DATA_FOLDER}/{table}.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)


table = "orders"
header = ["order_id","user_id","promo_id","address_id","created_at","order_cost","shipping_cost","order_total","tracking_id","shipping_service","estimated_delivery_at","delivered_at","status"]
with open(f"{DATA_FOLDER}/{table}.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)


table = "products"
header = ["product_id","name","price","inventory"]
with open(f"{DATA_FOLDER}/{table}.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)

table = "promos"
header = ["promo_id","discount","status"]
with open(f"{DATA_FOLDER}/{table}.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)


table = "users"
header = ["user_id","first_name","last_name","email","phone_number","created_at","updated_at","address_id"]
with open(f"{DATA_FOLDER}/{table}.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    query = f"select * from {table}"
    cursor.execute(query)

    results = cursor.fetchall()
    for each in results:
        writer.writerow(each)