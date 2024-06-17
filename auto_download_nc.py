import requests
import schedule
import time
from bs4 import BeautifulSoup
import os
import urllib3
import logging

# 忽略不安全的SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# URL of the catalog page
catalog_url = "https://psl.noaa.gov/thredds/catalog/Datasets/noaa.oisst.v2.highres/catalog.html?dataset=Datasets/noaa.oisst.v2.highres/sst.day.mean.2024.nc"
download_dir = "recent"
max_retries = 5

# 设置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_file():
    retries = 0
    while retries < max_retries:
        try:
            # Fetch the catalog page
            response = requests.get(catalog_url, verify=False)
            response.raise_for_status()

            # Parse the catalog page
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the httpServer link
            httpserver_link = None
            for link in soup.find_all('a', href=True):
                if 'fileServer' in link['href']:
                    httpserver_link = link['href']
                    break

            if not httpserver_link:
                logger.error("httpServer link not found")
                return

            # Complete URL for the download
            download_url = "https://psl.noaa.gov" + httpserver_link

            # Create download directory if it doesn't exist
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)

            # Filename for saving the downloaded file
            local_filename = os.path.join(download_dir, download_url.split('/')[-1])

            # Remove existing file with the same name
            if os.path.exists(local_filename):
                os.remove(local_filename)

            # Download the file
            logger.info(f"Downloading {local_filename} from {download_url}...")
            with requests.get(download_url, stream=True, verify=False) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info(f"Downloaded {local_filename}.")
            return

        except requests.exceptions.RequestException as e:
            retries += 1
            logger.error(f"Error downloading file: {e}. Retrying {retries}/{max_retries}...")
            time.sleep(5)  # 等待几秒钟后再重试

    logger.error("Max retries exceeded. Failed to download the file.")


# Schedule the task to run daily at midnight
schedule.every().day.at("18:00").do(download_file)

# Start the scheduler
logger.info("Scheduler started. Waiting for the next scheduled download...")
while True:
    schedule.run_pending()
    time.sleep(1)
