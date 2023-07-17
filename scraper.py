import json
import logging
import os
import threading
from urllib.parse import urlparse, urljoin
from typing import List
import time
import requests
import uvicorn
from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer
import redis
from fastapi import FastAPI

# Configure the logging module
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper_log.txt", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def create_file(url, raw_html, output_dir="."):
    # Create the filename by replacing special characters with underscores
    filename = url.replace("/", "_").replace(":", "_").replace(".", "_")
    filename += ".txt"

    # Construct the full path to the file in the output directory
    file_path = os.path.join(output_dir, filename)

    # Write the URL and raw HTML to the file
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(url + "\n")
        file.write(raw_html)

    return file_path


def delete_file(file_path):
    try:
        os.remove(file_path)
        print("File deleted successfully:", file_path)
    except FileNotFoundError:
        print("File not found:", file_path)
    except Exception as e:
        print("Error occurred while deleting the file:", str(e))


class Scraper:
    def __init__(self):
        self.max_depth = 4
        self.max_urls = 3000
        self.max_time_in_sec = 30
        self.db_file = 'scraper.db'
        self.kafka_broker = 'kafka:9092'
        self.kafka_topic = 'new_urls'
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.start_time = time.time()

        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker)

        # Connect to S3 - You should implement the S3 connection logic here
        # For example: self.s3_client = boto3.client('s3', region_name='your_region')

    def start_scraping(self, initial_urls):
        for url in initial_urls:
            logging.info(f"Start Scraping from {url} with max depth of {self.max_depth}")
            self.scrape(url, depth=0)

    def scrape(self, url, depth):
        if depth > self.max_depth:
            logging.info(f"current depth: {depth}\tmax depth: {self.max_depth}\tscrape will not be processed")
            return

        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")
            return

        soup = BeautifulSoup(response.content, "html")
        raw_html = response.text

        file_path = create_file(url, raw_html)

        print(f"save to S3 process, url:\t{url}")
        # Save the file to S3 - You should implement the logic to upload the file to S3 here
        # For example: self.upload_to_s3(url, raw_html)

        delete_file(file_path)

        # Increment the urls_visited_count for the current initial_url
        urls_visited_key = f"urls_visited_count:{url}"
        self.redis_client.incr(urls_visited_key)

        urls_visited_count = int(self.redis_client.get(urls_visited_key))
        if urls_visited_count >= self.max_urls:
            logging.info(f"Maximum number of URLs ({self.max_urls}) visited for {url}. Stopping scraping.")
            return

        if self.max_time_in_sec and time.time() - self.start_time >= self.max_time_in_sec:
            logging.info(f"Maximum time limit ({self.max_time_in_sec} seconds) reached. Stopping scraping.")
            return

        logging.info(f"Scraped {url}, depth={depth}")

        # Process links in the web page and send them to Kafka for further processing
        for link in soup.find_all("a"):
            href = link.get('href')
            self.process_link(href, depth + 1)

    def process_link(self, href, depth):
        if not href:
            return

        # Check if the URL has already been visited for the current initial_url
        visited_key = f"visited_urls:{href}"
        if not self.redis_client.sismember(visited_key, href):
            self.redis_client.sadd(visited_key, href)

            parsed_url = urlparse(href)
            if not parsed_url.scheme:
                # If the URL does not have a scheme, assume it is a relative URL and join it with the base URL
                base_url = urlparse(self.kafka_topic)
                href = urljoin(f"{base_url.scheme}://{base_url.netloc}", href)

            message = json.dumps({'url': href, 'depth': depth}, ensure_ascii=False).encode('utf-8')
            self.producer.send(self.kafka_topic, value=message)

    def run_kafka_consumer(self):
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_broker,
            group_id='web_scraping_group'
        )
        while True:
            time.sleep(0.2)
            for message in consumer:
                data = json.loads(message.value.decode('utf-8'))
                print(f"data:\t{data}")
                url = data['url']
                depth = data['depth']
                self.scrape(url, depth)

    def cleanup(self):
        try:
            # Close the Kafka producer
            self.producer.close()

            # Consume any remaining messages from the Kafka topic
            self.empty_kafka_queue()

        except Exception as e:
            logging.error(f"Error during cleanup: {e}")

    def empty_kafka_queue(self):
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_broker,
            group_id='web_scraping_group',
            auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
            enable_auto_commit=False,  # Disable auto-commit to manually commit offsets
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            # Acknowledge the message to commit the offset
            consumer.commit()

        # Close the consumer
        consumer.close()


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "Welcome to the Web Scraper API!"}


@app.post("/scrape/")
async def start_scraping(initial_urls: List[str], max_depth: int = 3, max_urls: int = 3000, max_time_in_sec: int = 180):
    scraper = Scraper()
    scraper.start_scraping(initial_urls)
    scraper.cleanup()
    return {"message": "Scraping completed!"}


def run_kafka_consumer(scraper):
    scraper.run_kafka_consumer()


if __name__ == "__main__":
    # Start the Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=run_kafka_consumer, args=(Scraper(),))
    consumer_thread.daemon = True
    consumer_thread.start()

    uvicorn.run(app, host="0.0.0.0", port=8000)
