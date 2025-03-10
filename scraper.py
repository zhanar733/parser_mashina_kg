import requests
from bs4 import BeautifulSoup
import json
import os
import time
from tqdm import tqdm
from utils import row_aggregate
from config import URL, DATA_DIR


def fetch_links(num_page):
    sub_urls = []
    for page in tqdm(range(1, num_page + 1)):
        page_url = f"{URL}?page={page}"
        response = requests.get(page_url)
        soup = BeautifulSoup(response.text, "html.parser")

        for block_image in soup.find_all("div", class_="list-item list-label"):
            sub_urls.append(f"https://m.mashina.kg{block_image.find('a')['href']}")

    with open(f"{DATA_DIR}/links.json", "w") as f:
        json.dump(sub_urls, f, indent=4)


def fetch_offers():
    os.environ["TZ"] = "Asia/Bishkek"
    time.tzset()

    with open(f"{DATA_DIR}/links.json", "r") as f:
        sub_urls = json.load(f)

    with open(f"{DATA_DIR}/dataset.jsonl", "a", encoding="utf-8") as f:
        for url in tqdm(sub_urls, desc="Processing offers"):
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            row = row_aggregate(soup)
            row["link"] = url
            row["current_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
