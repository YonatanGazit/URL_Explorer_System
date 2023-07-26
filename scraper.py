import json
from urllib.parse import urlparse, urljoin
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer
import redis
import boto3


import scraper_log
import scraper_utils


class Scraper:
    def __init__(self):
        self.max_depth = 1
        self.max_urls = 5
        self.max_time_in_sec = 30
        self.db_file = 'scraper.db'
        self.kafka_broker = 'kafka:9092'
        self.kafka_topic = 'new_urls'
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.start_time = time.time()
        self.bucket_name = 'yonatangazitwebscraper'
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker)

    def start_scraping(self, initial_urls):
        for url in initial_urls:
            scraper_log.logging.info(f"Start Scraping from {url} with max depth of {self.max_depth}")
            self.scrape(url, depth=0)

    def scrape(self, url, depth):
        if depth > self.max_depth:
            scraper_log.logging.info(
                f"current depth: {depth}\tmax depth: {self.max_depth}\tscrape will not be processed")
            return

        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            scraper_log.logging.error(f"Error scraping {url}: {e}")
            return

        soup = BeautifulSoup(response.content, features="html.parser")
        raw_html = response.text

        file_path, file_name = scraper_utils.create_file(url, raw_html)

        print(f"save to S3 process, url:\t{url}")
        try:
            s3_client = boto3.client('s3')
            s3_client.upload_file(file_path, self.bucket_name, file_name)
            # Print the URL of the uploaded file
            file_url = f'https://{self.bucket_name}.s3.amazonaws.com/{file_name}'
            scraper_log.logging.info(f"File uploaded successfully. URL: {file_url}")
        except Exception as e:
            scraper_log.logging.error(f"Error uploading file to S3: {e}")

        scraper_utils.delete_file(file_path)

        # Increment the urls_visited_count for the current initial_url
        urls_visited_key = "urls_visited_count"
        self.redis_client.incr(urls_visited_key)

        urls_visited_count = int(self.redis_client.get(urls_visited_key))
        if urls_visited_count >= self.max_urls:
            scraper_log.logging.info(f"Maximum number of URLs ({self.max_urls}) visited for {url}. Stopping scraping.")
            return

        if self.max_time_in_sec and time.time() - self.start_time >= self.max_time_in_sec:
            scraper_log.logging.info(f"Maximum time limit ({self.max_time_in_sec} seconds) reached. Stopping scraping.")
            return

        scraper_log.logging.info(f"Scraped {url}, depth={depth}")

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
            scraper_log.logging.error(f"Error during cleanup: {e}")

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
