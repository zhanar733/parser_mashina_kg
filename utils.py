from bs4 import BeautifulSoup
import logging

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_text_safe(element, class_name):
    """Безопасно извлекает текст из элемента."""
    tag = element.find(class_=class_name) if element else None
    return tag.get_text(strip=True) if tag else None

def head_left(soup: BeautifulSoup) -> dict:
    """Парсит основную информацию из блока head-left."""
    data = {}
    try:
        head_left_block = soup.find("div", class_="head-left")
        if not head_left_block:
            return data

        data["brand_"] = head_left_block.h1.get_text(strip=True).replace("Продажа", "").strip()
        data["upped_at"] = get_text_safe(head_left_block, "subblock upped-at")
        data["views"] = get_text_safe(head_left_block, "listing-icons views")
        data["heart"] = get_text_safe(head_left_block, "listing-icons heart")
        data["comments"] = get_text_safe(head_left_block, "listing-icons comments")

    except Exception as e:
        logging.error(f"Ошибка в head_left: {e}")
    return data

def get_price(soup: BeautifulSoup) -> dict:
    """Парсит цену в USD и KGS."""
    data = {}
    try:
        sep_main = soup.find("div", class_="sep main")
        if sep_main:
            data["USD_price"] = get_text_safe(sep_main, "price-dollar")
            data["KGS_price"] = get_text_safe(sep_main, "price-som")
    except Exception as e:
        logging.error(f"Ошибка в get_price: {e}")
    return data

def get_personal(soup: BeautifulSoup) -> dict:
    """Парсит данные о продавце."""
    data = {}
    try:
        personal_info = soup.find("div", class_="personal-info details-phone-wrap clr")
        if not personal_info:
            return data

        data["name"] = get_text_safe(personal_info, "i-name")
        data["number"] = get_text_safe(personal_info, "number")
        data["activity"] = get_text_safe(personal_info, "i-counter")

    except Exception as e:
        logging.error(f"Ошибка в get_personal: {e}")
    return data

def get_tab_pane(soup: BeautifulSoup) -> dict:
    """Парсит характеристики машины из вкладки."""
    data = {}
    try:
        tab_pane = soup.find("div", class_="tab-pane fade in active")
        if not tab_pane:
            return data

        for field in tab_pane.find_all("div", class_="field-row clr"):
            label = field.find("div", class_="field-label")
            value = field.find("div", class_="field-value")
            if label and value:
                data[label.get_text(strip=True)] = value.get_text(strip=True)
                
    except Exception as e:
        logging.error(f"Ошибка в get_tab_pane: {e}")
    return data

def get_configuration(soup: BeautifulSoup) -> dict:
    """Парсит дополнительные параметры автомобиля."""
    data = {}
    try:
        configuration = soup.find("div", class_="configuration")
        if configuration:
            categories = configuration.find_all("div", class_="name")
            values = configuration.find_all("div", class_="value")

            for cat, val in zip(categories, values):
                data[cat.get_text(strip=True)] = [p.get_text(strip=True) for p in val.find_all("p")]

    except Exception as e:
        logging.error(f"Ошибка в get_configuration: {e}")
    return data

def row_aggregate(soup: BeautifulSoup) -> dict:
    """Собирает все данные в один словарь."""
    data_dict = {}
    try:
        data_dict.update(head_left(soup))
        data_dict.update(get_price(soup))
        data_dict.update(get_personal(soup))
        data_dict.update(get_tab_pane(soup))
        data_dict.update(get_configuration(soup))
    except Exception as e:
        logging.error(f"Ошибка в row_aggregate: {e}")

    return data_dict
