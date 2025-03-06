import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
import os
import time

URL = "https://m.mashina.kg/search/all/"  # /?page=1989
num_page = int(input("How many pages to pars ?: "))

sub_urls = []
for page in tqdm(range(1, num_page + 1)):
    page_url = f"{URL}?page={page}"
    response = requests.get(page_url)
    soup = BeautifulSoup(response.text, "html.parser")

    for block_image in soup.find_all("div", class_="list-item list-label"):
        sub_urls.append(f"https://m.mashina.kg{block_image.find('a')['href']}")

with open("links.json", "w") as f:
    json.dump(sub_urls, f, indent=4)
