from datetime import datetime
import requests

def fetch_cards(url):
    response = requests.get(url)
    if response.status_code != 200:
        print("get_json: ", str(response.status_code), flush=True)
        print(datetime.now(), ": Request Error on Bulk Data", response.status_code, flush=True)
    data = response.json()

    # DOWNLOAD CARD DATA
    download = data["download_uri"]
    downloaded = requests.get(download, stream=True)
    if downloaded.status_code != 200:
        print(datetime.now(), ": Request Error on Download:", downloaded.status_code, flush=True)
    cards = downloaded.json()
    return cards