from scraper import fetch_links, fetch_offers
import os

num_page = int(input("How many pages to parse ?: "))

if not os.path.isdir("data"):
    os.mkdir("data")


# Получение и сохранение ссылок
fetch_links(num_page)

# Парсинг данных по ссылкам
fetch_offers()


