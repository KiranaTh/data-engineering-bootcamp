import csv
import json
import os
import sys

import scrapy
from scrapy.crawler import CrawlerProcess
from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account


URL = "https://ทองคําราคา.com/"


class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)

        for row in rows:
            print(row.css("td::text").extract())
            # print(row.xpath("td//text()").extract())

        # Write to CSV
        with open("price.csv", "w") as f:
            writer = csv.writer(f)
            header = rows
            writer.writerow(header)

            for row in rows:
                writer.writerow(row.css("td::text").extract())

        # The ID of your GCS bucket
        bucket_name = "jill-100005"
        # The path to your file to upload
        source_file_name = "price.csv"
        # The ID of your GCS object
        destination_blob_name = "gold_price/2023-05-07/price.csv"

        keyfile = os.environ.get("KEYFILE_PATH")
        service_account_info = json.load(open(keyfile))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        project_id = "liquid-optics-384501"

        storage_client = storage.Client(
            project=project_id,
            credentials=credentials,
        )
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
