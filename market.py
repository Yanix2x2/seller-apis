import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
     """Получить список товаров.

    Args:
        page (int): Номер страницы, которая должна быть получена.
        campaign_id (str): id компании необходимого для аутентификации запроса.
        access_token (str): Токен продавца для аутентификации запроса.

    Returns:
        dict: Словарь с ответом api в формате json.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки. 

    Args:
        stocks (list): Список остатков
        campaign_id (str): id компании необходимого для аутентификации запроса.
        access_token (str): Токен продавца для аутентификации запроса.
    
    Returns:
        dict: Словарь с ответом api в формате json.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены на товары для Яндекс Маркета.

    Args:
        prices (list): Список цен на товары.
        campaign_id (str): id компании необходимого для аутентификации запроса.
        access_token (str): Токен продавца для аутентификации запроса.

    Returns:
        dict: Словарь с ответом api в формате json.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров магазина Яндекс Маркет.
    
    Args:
        campaign_id (str): id компании необходимого для аутентификации запроса.
        market_token (str): Токен продавца для аутентификации запроса.

    Returns:
        list: Список ids из магазина Яндекс Маркет.

    Raises:
        HTTPError: Ответ с кодом 4xx или 5xx.
        ConnectionError: Проблемы подключения к серверу.
    
    Example:
        >>> market_token = env.str("MARKET_TOKEN")
        >>> campaign_fbs_id = env.str("FBS_ID")
        >>> get_offer_ids(campaign_id, market_token)
        ['101', '102']
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать список остатков.

    Args:
        watch_remnants (list[dict]): Список словарей с данными о часах.
        offer_ids (list): Список ids продуктов, для которых нужно создать остатки.
        warehouse_id (str): id склада.
        
    Return:
        list[dict]: Список словарей с информацией об остатках товаров.

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
            >>> warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID") 
            >>> create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
            [
                {
                    'sku': 'WATCH-BL-101', 
                    'warehouseId': 'WAREHOUSE_FBS_ID, 
                    'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2025-02-06T08:53:21Z'}]
                } 
            ]   

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
            >>> warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
            >>> create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
            ValueError: invalid literal for int() with base 10: 'null' 
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для товаров по ids из Яндекс Маркета.

    Args:
        watch_remnants (dict): Данные о товаре из магазина Casio.
        offer_ids (list): Список ids продуктов из Яндекс Маркета.

    Returns:
        list[dict]: Список словарей с дынными о ценах.

    Example:
        Корректное использование:
            >>> watch_remnants = [{'Код': '101', 'Цена': '5990'}]
            >>> offer_ids = ['101']
            >>> create_prices(watch_remnants, offer_ids)
            [
                {
                    "id": "101",
                    "price": {
                        "value": 5990,
                        "currencyId": "RUR",
                    }
                }
            ]
        Некорректное исполнение:
            >>> watch_remnants = [{'Код': '101', "Цена": "null"}]
            >>> offer_ids = ['101']
            >>> create_prices(watch_remnants, offer_ids)
            ValueError: invalid literal for int() with base 10: 'null' 
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно загружает цены товаров на маркетплейс.

    Функция получает ids продуктов из Яндекс Маркета,
    создаёт список цен для товаров из `watch_remnants`, и загружает их на маркетплейс
    частями по 500 товаров за раз.

    Args:
        watch_remnants (list): Остатки товаров.
        campaign_id (str): id компании необходимого для аутентификации запроса.
        market_token (str): Токен продавца для аутентификации запроса.

    Returns:
        list: Список всех созданных цен.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """ Загружает остатки товаров на склад и обновляет их Яндекс Маркете.

    Функция получает id товаров для Яндекс Маркета, создаёт остатки товаров,
    разделяет остатки на партии по 2000 элементов и обновляет их в Яндекс Маркете.
    Затем остатки фильтруются, оставляя только те, 
    у которых количество товаров не равно нулю.

    Args:
        watch_remnants (list): Список остатков товаров для обработки.
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен для доступа к API маркетплейса.
        warehouse_id (str): Идентификатор склада.

    Returns:
        tuple: Кортеж из двух элементов:
            not_empty (list): Список остатков с ненулевым количеством товаров.
            stocks (list): Все остатки товаров, включая нулевые.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
