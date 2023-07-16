# Yonatan Gazit Web Scraper - README

System Architecture:
--------------------
The Web Scraper is a distributed web scraping system that utilizes Kafka for message queueing and Redis for shared in memory storage.
It consists of several components working together:

1. Kafka: A message broker used for communication between the web scraper and the consumers.
The producer sends URLs to be scraped, and the consumers process the URLs and send newly discovered URLs back to the producer for further scraping.

2. Redis: A key-value store used to keep track of visited URLs.
It helps prevent duplicate scraping and enforces the maximum number of URLs and the maximum time limit for scraping.

3. Web Scraper: The main component responsible for scraping web pages.
It uses BeautifulSoup for parsing HTML, and it saves each URL along with its corresponding raw HTML into .txt files.

4. S3 (Simple Storage Service) for saving the .txt files

5. FastAPI: The web framework used for exposing API endpoints to start the scraping process and interact with the Web Scraper.


Algorithm - Recursive BFS of Discovered URLs:
-----------------------------------------------
The web scraper uses a Recursive Breadth-First Search (BFS) algorithm to discover and scrape URLs from web pages. The process is as follows:

1. Start with a list of initial URLs provided by the user in the API request.

2. For each URL in the list, send it to Kafka to start the scraping process.

3. When processing a URL, the scraper fetches the web page, extracts the raw HTML content, and saves it to a .txt file.

4. The scraper then parses the HTML using BeautifulSoup to find all anchor (a) tags containing href attributes.

5. For each discovered URL (href), the scraper adds it to the Kafka queue to be processed in the future.
This process is recursive, allowing the scraper to traverse the web in a breadth-first manner, scraping new URLs as they are discovered.
Because it's a recursive algorithm, stop points was added and can be changed base on the system needs.
The algorithm will stop base on 3 scenarios:
a. max_depth
b. max_urls
c. max_time_in_sec


Database Schema:
----------------
Because raw HTML is an un-structure data format with no limited size, data is stored in a .txt file.
For each URL a corresponding file will emerge named {ASCII(URL)} in lowercase.
Inside the files, the first row will be the 'raw URL string, and the second row will contain the 'raw HTML' (also a string)

API Endpoints:
--------------
1. GET /:
   Description: The root endpoint that welcomes users to the Web Scraper API.
   Response: {"Hello": "Welcome to the Web Scraper API!"}

2. POST /scrape/:
   Description: Initiates the web scraping process with the provided initial URLs and parameters.
   Request Body:
   {
       "initial_urls": List[str]: The list of initial URLs to start scraping from.
       "max_depth": int (optional): The maximum depth of recursive scraping (default is 3).
       "max_urls": int (optional): The maximum number of URLs to scrape (default is 3000).
       "max_time_in_sec": int (optional): The maximum time limit for scraping in seconds (default is 180).
   }
   Response: {"message": "Scraping completed!"}

3. GET /url/:
   Retrieve stored URLs and their associated raw HTML content.


Please note that before running the Web Scraper, make sure to have Kafka and Redis up and running, and to configure the necessary connection details in the code.

For any questions or issues, please contact jonatangazit@gmail.com
