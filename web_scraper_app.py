import threading
from typing import List
import boto3
import uvicorn
from fastapi import FastAPI

import scraper as sc
import scraper_utils as su

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "Welcome to the Web Scraper API!"}


@app.post("/scrape/")
async def start_scraping(initial_urls: List[str]):
    scraper = sc.Scraper()
    scraper.start_scraping(initial_urls)
    scraper.cleanup()
    return {"message": "Scraping completed!"}


@app.get("/file_list/")
async def read_files():
    # List all files in the S3 bucket
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket='yonatangazitwebscraper')
    if 'Contents' in response:
        file_list = [obj['Key'] for obj in response['Contents']]
        return {"files": file_list}
    else:
        return {"files": []}


@app.get("/file_list/{file_name}")
async def read_file(file_name: str):
    file_content = su.get_file_content_from_s3('yonatangazitwebscraper', file_name)
    if file_content:
        # Split the file content into 'raw url' and 'raw HTML'
        raw_url, raw_html = file_content.split('\n', 1)
        return {"raw_url": raw_url, "raw_html": raw_html}
    else:
        return {"error": "File not found"}


def run_kafka_consumer(scraper):
    scraper.run_kafka_consumer()


if __name__ == "__main__":
    # Start the Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=run_kafka_consumer, args=(sc.Scraper(),))
    consumer_thread.daemon = True
    consumer_thread.start()

    uvicorn.run(app, host="0.0.0.0", port=8000)
