import requests
import schedule
import time
from bs4 import BeautifulSoup
import os
import urllib3

# 忽略不安全的SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# URL of the catalog page
catalog_url = "https://psl.noaa.gov/thredds/catalog/Datasets/noaa.oisst.v2.highres/catalog.html?dataset=Datasets/noaa.oisst.v2.highres/sst.day.mean.2024.nc"
download_dir = "recent"
# 修改下载文件夹


def download_file():
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
        print("httpServer link not found")
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
    print(f"Downloading {local_filename} from {download_url}...")
    with requests.get(download_url, stream=True, verify=False) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {local_filename}.")


# Schedule the task to run daily at midnight
schedule.every().day.at("17:16").do(download_file)

# Start the scheduler
print("Scheduler started. Waiting for the next scheduled download...")
while True:
    schedule.run_pending()
    time.sleep(1)
