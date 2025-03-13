import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров с магазина озон.

    Args:
        last_id (str): id последнего продукта из предыдущего запроса.
        client_id (str): id клиента необходимый для аутентификации запроса.
        seller_token (str): Токен продавца для аутентификации запроса.

    Return:
        dict: Словарь, содержащий список продуктов.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Args:
        client_id (str): id клиента необходимый для аутентификации запроса.
        seller_token (str): Токен продавца для аутентификации запроса.

    Return:
        list: Список ids из магазина ozon.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.

    Examples:
        >>> seller_token = env.str("SELLER_TOKEN")
        >>> client_id = env.str("CLIENT_ID")
        >>> get_offer_ids(client_id, seller_token)
        ['101', '102', '103']
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

    Args:
        prices (list): Список цен на товары
        client_id (str): id клиента необходимый для аутентификации запроса.
        seller_token (str): Токен продавца для аутентификации запроса.
    
    Returns:
        dict: Словарь с ответом api в формате json.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

    Args: 
        stocks (list): Список остатков из магазина casio.
        client_id (str): id клиента необходимый для аутентификации запроса.
        seller_token (str): Токен продавца для аутентификации запроса.

    Returns:
        dict: Словарь с ответом api в формате json.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать и обработать файл ostatki с сайта casio.

    Делает запрос на сайт casio и качает оттуда
    архив с информацией об остатках, записывается
    в watch_remnants и архив удаляется.

    Return:
        list[dict]: Список словарей, где каждый словарь представляет собой 
                    запись о товаре.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.

    Examples:
        >>> download_stock()
        [
            {
                'Код': '101', 
                'Наименование товара': 'Часы_1',
                'Цена': "5'990.00 руб.", 
                'Количество': '5'
            }
        ]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков.

    Args:
        watch_remnants (list[dict]): Список словарей с данными о часах.
        offer_ids (list[str]): Список ids, для которых нужно создать остатки.

    Return:
        list[dict]: Список словарей с id и остатками товаров.

    Examples:
        Корректное исполнение:
            >>> watch_remnants = 
            [
                {
                    'Код': '101', 
                    'Наименование товара': 'Часы_1',
                    'Цена': "5'990.00 руб.", 
                    'Количество': '5'
                }
            ]
            >>> offer_ids = ['101']
            >>> create_stocks(watch_remnants, offer_ids)
            [{'offer_id': '101', 'stock': 5}]

        Некорректное исполнение:
             >>> watch_remnants = 
            [
                {
                    'Код': '101', 
                    'Наименование товара': 'Часы_1',
                    'Цена': "5'990.00 руб.", 
                    'Количество': 'null'
                }
            ]
            >>> offer_ids = ['101']
            >>> create_stocks(watch_remnants, offer_ids)
            ValueError: invalid literal for int() with base 10: 'null'
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для товаров, которые присутствуют в offer_ids.

    Args:
        watch_remnants (list[dict]): Список словарей с остатками товаров 
                                     из магазине casio.
        offer_ids (list[dict]): Список ids товаров из ozon, 
                                для которых создаются цены.

    Return:
        list[dict]: Список словарей с информацией о ценах.

    Examples:
        Корректное исполнение:
            >>> watch_remnants = [{'Код': '101', 'Цена': '5990'}]
            >>> offer_ids = ['101']
            >>> create_prices(watch_remnants, offer_ids)
            [
                {
                    'auto_action_enabled': 'UNKNOWN', 
                    'currency_code': 'RUB',
                    'offer_id': '101',
                    'old_price': '0', 
                    'price': '5990'
                }
            ]

        Некорректное исполнение:
            >>> watch_remnants = [{'Код': '101', 'Цена': '5990'}]
            >>> offer_ids = ['999']
            >>> create_prices(watch_remnants, offer_ids)
            []
            (если ни один из товаров не найден в `offer_ids`, 
            возвращается пустой список)
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices 


def price_conversion(price: str) -> str:
    """Функция преобразовывает cтроку price в строку, состоящую только из цифр.

    Args:
        price (str): Строка, представляющая цену. Помимо цифр может содержать 
                    символы валют, разделители.

    Return:
        str: Строка, содержащая только цифры.

    Examples:
        Корректное исполнение:
            >>> price = "5'990.00 руб."
            >>> price_conversion(pricе)
            "5990"

        Некорректное исполнение:
            >>> price = "price"
            >>> price_conversion(pricе)
            ""
            (если не переданы цифры, то код удалит всё и оставит пустую строку)

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список на части по n элементов.

    Args:
        lst (list): Список, который нужно разделить.
        n (int): По сколько элементов будет одна часть.

    Returns:
        lst: Одна часть списка собранного по n элементов.

    Examples:
        >>> lst = ['101', '102']
        >>> n = 1
        >>> divide(lst, n)
        '101' 
        >>> divide(lst, n)
        '102'
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает и обновляет цены товаров в Ozon.

    Функция получает ids товаров. Cоздает цены на основе переданных 
    данных об остатках товаров. Разделяет цены на партии по 1000 элементов и 
    обновляет их.

    Args:
        watch_remnants (list): Список остатков товаров.
        client_id (str): id клиента необходимый для аутентификации запроса.
        seller_token (str): Токен продавца для аутентификации запроса.

    Returns:
        list: Список всех созданных цен.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает и обновляет остатки товаров для Ozon.

    Функция получает ids товаров, cоздает остаткb товаров, разделяет остатки 
    на партии по 100 элементов и обновляет их. Фильтрует остатки, оставляя 
    только те, у которых количество товаров не равно нулю.

    Args:
        watch_remnants (list): Список остатков товаров.
        client_id (str): id клиента необходимый для аутентификации запроса.
        seller_token (str): Токен продавца для аутентификации запроса.


    Returns:
        tuple: Кортеж из двух элементов:
            not_empty (list): Список остатков с ненулевым количеством товаров.
            stocks (list): Все остатки товаров, включая нулевые.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
