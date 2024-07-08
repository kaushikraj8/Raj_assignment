import shutil
import pandas as pd
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sqlite3
import requests
import gzip
import os
import datetime
import numpy as np
import logging
from concurrent.futures import ThreadPoolExecutor,wait
import threading as th
import bconfig
from functools import lru_cache
import traceback

config = bconfig.GetConfig()
# Set up logging
log_directory = config['log_directory']
log_directory_path = os.path.join(os.getcwd(), log_directory)
if not os.path.exists(log_directory_path):
    os.makedirs(log_directory_path)
log_file = os.path.join(log_directory_path, f"Question1_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to download the file
def download_file(url, filename, chunk_size = 1024) -> None:
    """
    Downloads a file from a given URL and saves it to a specified filename.
    
    Args:
        url (str): The URL of the file to be downloaded.
        filename (str): The name of the file where the downloaded content will be saved.
        chunk_size (int, optional): The size of each chunk to be read during the download. Default is 1024 bytes.
    """
    if os.path.exists(filename):
        logging.info(f'File {filename} already exists. Skipping download.')
        return
    
    try:
        response = requests.get(url, stream=True, verify=False)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        last_logged_percentage = -1
        try:
            with open(filename, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    size = file.write(chunk)
                    downloaded_size += size
                    progress_percentage = (downloaded_size / total_size) * 100
                    
                    if int(progress_percentage) != last_logged_percentage:
                        last_logged_percentage = int(progress_percentage)
                        logging.info(f'Download progress: {progress_percentage:.2f}%')
        except:
            logging.critical(f'Error writing to file: {traceback.format_exc()}')
        logging.info(f'Downloaded {filename}')
    except requests.exceptions.RequestException as err:
        logging.error(f'Error downloading file: {err}')

# Function to unzip the gz file
def unzip_file(filename_gz, filename_unzipped):
    try:
        with gzip.open(filename_gz, 'rb') as f_in, open(filename_unzipped, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            logging.info(f'Unzipped {filename_unzipped}')
    except Exception as err:
        logging.error(f'Error unzipping file: {err}')
# Function to store the data in the database
def store_in_database(data, db_filename='amazon_reviews.db'):
    logging.info("Database processing started")
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
    except:
        logging.error('Error connecting to the database.')
        logging.critical(traceback.format_exc())
        raise

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            reviewerID TEXT,
            asin TEXT,
            reviewerName TEXT,
            vote INTEGER,
            style TEXT,
            reviewText TEXT,
            overall FLOAT,
            summary TEXT,
            unixReviewTime INTEGER,
            reviewTime TEXT,
            image TEXT
        )
    ''')
    try:
        data.to_sql('reviews', conn, if_exists='replace', index=False)
        conn.commit()
    except:
        logging.error('Error storing data in the database.')
        logging.critical(traceback.format_exc())
        raise
    try:
        conn.close()
    except:
        logging.error('Error closing the database connection.')
        logging.critical(traceback.format_exc())
        raise
    else:
        logging.info("Database processing completed")

#read the json file 
def read_large_json(file_path):
    """Reads a large JSON file in chunks."""
    chunksize = 10**5  # Adjust chunk size as needed from config file 
    chunks = []
    for chunk in pd.read_json(file_path, lines=True, chunksize=chunksize):
        chunks.append(chunk)
        
    df = pd.concat(chunks, axis=0)
    return df

def send_email_report(df):
    """Sends a daily email report."""
    logging.info("Sending email report")
    hardcover_asins = df[(df['overall'] > 3) & (df['style'].str.strip() == 'Hardcover')]['asin'].unique().tolist()
    max_reviews_asin = df.groupby('asin')['reviewerID'].count().idxmax()
    
    # Prepare email content
    message = f"ASINs with overall > 3 and Format Hardcover: {hardcover_asins}\n\nASIN with most reviews: {max_reviews_asin}"
    logging.info(f"email message: {message}")
    # Set up & send email
    msg = MIMEMultipart()
    msg['From'] = config['from_email']
    msg['To'] = config['to_email']
    msg['Subject'] = config['subject']
    msg.attach(MIMEText(message))
    try:
        server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
        server.starttls()
        server.login(config['smtp_username'], config['smtp_password'])
        server.send_message(msg)
        server.quit()
    except:
        logging.error(f'Error sending email: {traceback.format_exc()}')
    else:
        logging.info("Email report sent successfully")
@lru_cache(maxsize=None)
def clean_text(text):
    """Cleans text by removing unnecessary characters, altering format, and removing invalid characters."""
    if pd.isnull(text):
        return ''
    
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove non-alphanumeric characters except whitespace
    text = text.lower() # convert to lower case 
    text = re.sub(r'\[.*?\]', '', text)  # Remove text in square brackets
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.S)  # Remove URLs
    text = re.sub(r'\@\w+|\#', '', text)  # Remove @mentions and hashtags
    text = re.sub(r'[^\x20-\x7E\xA0-\uD7FF\uE000-\uFDCF\uFDF0-\uFFFD]+', '', text) # removes non-printable ASCII characters
    text = re.sub(r'[^\w\s]', '', text)  # Remove remaining punctuations
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

#flatten the dictionary
def extract_format(style):
        if isinstance(style, dict):
            return style.get('Format:', np.nan)
        return np.nan
# Save to the file 
def save_df_to_csv(df, filename):
    logging.info(f'Saving DataFrame to CSV')
    try:
        df.to_csv(filename)
    except:
        logging.error(f'Error saving DataFrame to CSV: {traceback.format_exc()}')
    else:
        logging.info(f'DataFrame saved to {filename}')

# Flatten the list 
def join_if_list(x):
    return '; '.join(x) if isinstance(x, list) else x

def main():
    # Download gz file and unzip it
    logging.info('Starting...')
    logging.info(f'Downloading {config["url"]}')
    download_file(config['url'], config['filename_gz'])
    logging.info(f'Unzipping {config["filename_gz"]}')
    unzip_file(config['filename_gz'], config['filename_unzipped'])
    file_path = os.path.join(os.getcwd(), config['filename_unzipped'])
    logging.info(f'Reading JSON file')
    df = read_large_json(file_path)  # Read JSON file

    # # Prepare reviews DataFrame
    logging.info(f'Cleaning text')
    logging.info(f'Preparing DataFrame')

    with ThreadPoolExecutor() as executor:
        df['reviewText'] = list(executor.map(clean_text, df['reviewText']))
        df['summary'] = list(executor.map(clean_text, df['summary']))

    df = df.drop_duplicates(subset=['reviewerID', 'asin', 'overall','reviewerName'], keep='first')
    df.dropna(axis=0, how='all',inplace=True)

    with ThreadPoolExecutor() as executor:
        df['style'] = list(executor.map(extract_format, df['style']))
        df['image'] = list(executor.map(join_if_list, df['image']))

    with ThreadPoolExecutor(max_workers=3) as executor:
        future1 = executor.submit(store_in_database, df, config['db_filename'])
        future2 = executor.submit(save_df_to_csv, df, config['result_file'])
        future3 = executor.submit(send_email_report, df)

    # Wait for all futures to complete
    wait([future1, future2, future3])
    logging.info('Process finished successfully.')

if __name__ == "__main__":
   main() 