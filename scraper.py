import os
import json
import time
import logging
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utils import row_aggregate

# Настройки
URL = "https://m.mashina.kg/search/all/"
DATA_DIR = "data"
LINKS_FILE = f"{DATA_DIR}/links.json"
DATASET_FILE = f"{DATA_DIR}/dataset.jsonl"
REPORT_FILE = f"{DATA_DIR}/parsing_report.jsonl"
MAX_WORKERS = 10  # Количество потоков

# Создаем директорию, если ее нет
os.makedirs(DATA_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_links(num_page):
    """Собирает ссылки на объявления и записывает в файл."""
    os.environ["TZ"] = "Asia/Bishkek"
    time.tzset()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S")
    new_links = set()

    # Загружаем старые ссылки
    if os.path.exists(LINKS_FILE):
        with open(LINKS_FILE, "r") as f:
            try:
                saved_links = set(json.load(f))
            except json.JSONDecodeError:
                saved_links = set()
    else:
        saved_links = set()

    for page in tqdm(range(1, num_page + 1), desc="Fetching links"):
        try:
            page_url = f"{URL}?page={page}"
            response = requests.get(page_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            for block_image in soup.find_all("div", class_="list-item list-label"):
                link = block_image.find("a")
                if link and "href" in link.attrs:
                    full_link = f"https://m.mashina.kg{link['href']}"
                    if full_link not in saved_links:
                        new_links.add(full_link)

        except requests.RequestException as e:
            logging.error(f"Ошибка при запросе страницы {page}: {e}")

    # Обновляем файл ссылок
    saved_links.update(new_links)
    with open(LINKS_FILE, "w") as f:
        json.dump(list(saved_links), f, indent=4)

    # Записываем отчет
    report = {
        "date": start_time,
        "end_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "new_links": len(new_links),
        "total_links": len(saved_links),
    }
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(report, ensure_ascii=False) + "\n")

    logging.info(f"✅ Парсинг завершен. Найдено новых ссылок: {len(new_links)}")
    return new_links

def fetch_offer(url):
    """Загружает и обрабатывает данные объявления."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        row = row_aggregate(soup)
        row["link"] = url
        row["current_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return json.dumps(row, ensure_ascii=False) + "\n"

    except requests.RequestException as e:
        logging.error(f"Ошибка при загрузке {url}: {e}")
        return None

def fetch_offers():
    """Загружает все объявления по списку ссылок, пропуская уже загруженные."""
    os.environ["TZ"] = "Asia/Bishkek"
    time.tzset()

    # Загружаем ссылки
    if not os.path.exists(LINKS_FILE):
        logging.error("Файл links.json не найден! Сначала запустите fetch_links().")
        return

    with open(LINKS_FILE, "r") as f:
        sub_urls = json.load(f)

    # Загружаем уже обработанные ссылки
    processed_urls = set()
    if os.path.exists(DATASET_FILE):
        with open(DATASET_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    processed_urls.add(data["link"])
                except json.JSONDecodeError:
                    continue  # Игнорируем ошибки

    # Фильтруем ссылки (парсим только новые)
    new_urls = [url for url in sub_urls if url not in processed_urls]
    
    if not new_urls:
        logging.info("✅ Все объявления уже загружены!")
        return

    logging.info(f"🔹 Начинаем загрузку {len(new_urls)} новых объявлений...")

    start_time = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Многопоточное скачивание
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        with open(DATASET_FILE, "a", encoding="utf-8") as f:
            for result in tqdm(executor.map(fetch_offer, new_urls), total=len(new_urls), desc="Processing offers"):
                if result:
                    f.write(result)

    # Записываем отчет
    report = {
        "date": start_time,
        "end_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "processed_links": len(new_urls),
        "total_links": len(sub_urls),
    }
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(report, ensure_ascii=False) + "\n")

    logging.info("✅ Парсинг объявлений завершен!")
