from config import URL, DATA_DIR
from scraper import fetch_links, fetch_offers

num_page = int(input("How many pages to parse ?: "))


# Получение и сохранение ссылок
fetch_links(num_page)

# Парсинг данных по ссылкам
fetch_offers()

print("Parsing completed and data saved successfully!")
