import os
import string
import boto3

import scraper_log as sl


def create_file(url, raw_html, output_dir="."):
    # Create the filename by replacing special characters with underscores
    valid_chars = f"-_.() {string.ascii_letters}{string.digits}"
    filename = "".join(c if c in valid_chars else "_" for c in url)
    filename += ".txt"

    # Construct the full path to the file in the output directory
    file_path = os.path.join(output_dir, filename)

    # Write the URL and raw HTML to the file
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(url + "\n")
        file.write(raw_html)

    return file_path, filename


def delete_file(file_path):
    try:
        os.remove(file_path)
        print("File deleted successfully:", file_path)
    except FileNotFoundError:
        print("File not found:", file_path)
    except Exception as e:
        print("Error occurred while deleting the file:", str(e))


def get_file_content_from_s3(bucket_name, file_name):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        sl.logging.error(f"Error retrieving file from S3: {e}")
        return None
