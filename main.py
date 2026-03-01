import os
import re
import time
import logging
import pandas as pd
from curl_cffi import requests

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class LentaAppParser:
    def __init__(self, city_name: str, store_alias: str, headers: dict):
        self.city = city_name
        self.expected_store = int(store_alias)
        self.headers = headers
        self.url_items = "https://api.lenta.com/v1/catalog/items"
        self.session = requests.Session(impersonate="chrome110")
        self.brand_list = []

    def _extract_brands(self, data: dict) -> list:
        """Извлекает список доступных брендов из фильтров категории для матчинга"""
        filters = data.get("filters", {}).get("multicheckbox",[])
        for f in filters:
            if f.get("key") == "brand":
                vals =[v.get("value") for v in f.get("values", []) if v.get("value")]
                # Сортируем по убыванию длины для точного совпадения подстрок
                return sorted(vals, key=len, reverse=True)
        return []

    def _match_brand(self, name: str) -> str:
        """Сопоставляет название товара со списком брендов с учетом границ слов"""
        if not name:
            return "Не указан"
        name_up = name.upper()
        for b in self.brand_list:
            # Используем регулярку, чтобы избежать ложных срабатываний на коротких брендах
            # Например, бренд "МАК" не должен срабатывать на "МАКАРОНЫ"
            pattern = rf'\b{re.escape(b.upper())}\b'
            if re.search(pattern, name_up):
                return b
        return "Неизвестно"

    def parse(self, category_id: int) -> list:
        """Основной метод сбора товаров из заданной категории"""
        parsed_items = []
        offset = 0
        limit = 26  # Стандарт приложения
        max_requests = 400  # Защита от бесконечного цикла (запас на ~10к товаров)

        logging.info(f"Старт сбора. Город: {self.city} (Ожидаемый магазин: {self.expected_store})")

        for _ in range(max_requests):
            payload = {
                "categoryId": category_id,
                "filters": {"multicheckbox": [], "checkbox": [], "range": []},
                "sort": {"type": "popular", "order": "desc"},
                "limit": limit,
                "offset": offset
            }

            resp = None
            for attempt in range(3):
                try:
                    resp = self.session.post(self.url_items, json=payload, headers=self.headers, timeout=30)

                    if resp.status_code == 200:
                        break  # Успех, выходим из цикла attempt

                    if resp.status_code == 500:
                        logging.warning(f"[{self.city}] Сервер 500. Попытка {attempt + 1}...")
                        time.sleep(5)
                        continue

                    if resp.status_code in [401, 403]:
                        logging.error(f"[{self.city}] Ошибка {resp.status_code}. Токены не работают!")
                        return parsed_items

                except Exception as e:
                    logging.warning(f"Ошибка сети: {e}. Попытка {attempt + 1}")
                    time.sleep(5)
            else:
                # Если за 3 попытки не получили 200 OK
                logging.error(f"[{self.city}] Не удалось получить данные для offset {offset}. Пропускаем город.")
                break

            try:
                resp_data = resp.json()

                # Инициализируем бренды один раз
                if not self.brand_list:
                    self.brand_list = self._extract_brands(resp_data)

                items = resp_data.get("items", [])
                if not items:
                    logging.info(f"[{self.city}] Товары в категории закончились.")
                    break

                # Проверка, что сессия не переключилась на другой город
                if items[0].get("storeId") != self.expected_store:
                    logging.error(f"[{self.city}] Критическая ошибка: API отдает данные другого магазина!")
                    break

                for itm in items:
                    # ПРОВЕРКА НАЛИЧИЯ:
                    # Исключаем товары, заблокированные к продаже
                    if itm.get("features", {}).get("isBlockedForSale"):
                        continue

                    # Проверка фактического наличия
                    if itm.get("count", 0) <= 0:
                        continue

                    brand_name = self._match_brand(itm.get("name"))

                    parsed_items.append({
                        "id": str(itm.get("id")),
                        "name": itm.get("name"),
                        "regular_price": itm.get("prices", {}).get("priceRegular", 0) / 100,
                        "promo_price": itm.get("prices", {}).get("price", 0) / 100,
                        "brand": brand_name,
                        "city": self.city
                    })

                offset += limit
                time.sleep(2)  # Во избежание бана от Qrator


            except Exception as e:
                logging.error(f"[{self.city}] Ошибка при парсинге JSON на offset {offset}: {e}")
                break

        return parsed_items


if __name__ == "__main__":
    # Если скрипт выдает 401/403 ошибку, необходимо обновить headers:

    headers_msk = {
        "ADID": "4ef36ab8f60736e6b77ea0ab2037f0ce",
        "AdvertisingId": "1a1b3c92-fa38-4316-bb62-55ad1308f32c",
        "App-Version": "6.72.0",
        "Client": "android_9_6.72.0_rustore",
        "Connection": "Keep-Alive",
        "Content-Type": "application/json; charset=utf-8",
        "DeviceId": "A-7e6b7279-fa36-472b-89a9-77cc747d3646",
        "LocalTime": "2026-03-01T13:53:47.793+03:00",
        "Qrator-Token": "1b774151048141fc3a355c9922f7cc07",
        "SessionToken": "B52EE9F535BEA2C8573D73580E4917A9",
        "Timestamp": "1772362427",
        "User-Agent": "lo, 6.72.0",
        "X-Delivery-Mode": "pickup",
        "X-Device-Brand": "Xiaomi",
        "X-Device-ID": "A-7e6b7279-fa36-472b-89a9-77cc747d3646",
        "X-Device-Name": "Xiaomi",
        "X-Device-OS": "Android",
        "X-Device-OS-Version": "28",
        "X-Organization-ID": "",
        "X-Platform": "omniapp",
        "X-Retail-Brand": "lo"
    }

    headers_spb = {
        "ADID": "4ef36ab8f60736e6b77ea0ab2037f0ce",
        "AdvertisingId": "1a1b3c92-fa38-4316-bb62-55ad1308f32c",
        "App-Version": "6.72.0",
        "Client": "android_9_6.72.0_rustore",
        "Connection": "Keep-Alive",
        "Content-Type": "application/json; charset=utf-8",
        "DeviceId": "A-89bbbeff-a63e-4c62-8326-e3b117aa689b",
        "LocalTime": "2026-03-01T14:07:51.522+03:00",
        "Qrator-Token": "4493b6bed1df4b7b4db667b5dc40ef93",
        "SessionToken": "A2E9554B1A6ECC70CC4F0D20DE14E38F",
        "Timestamp": "1772363271",
        "User-Agent": "lo, 6.72.0",
        "X-Delivery-Mode": "pickup",
        "X-Device-Brand": "Xiaomi",
        "X-Device-ID": "A-89bbbeff-a63e-4c62-8326-e3b117aa689b",
        "X-Device-Name": "Xiaomi",
        "X-Device-OS": "Android",
        "X-Device-OS-Version": "28",
        "X-Organization-ID": "",
        "X-Platform": "omniapp",
        "X-Retail-Brand": "lo"
    }

    STORE_ALIAS_SPB = "0724"
    STORE_ALIAS_MSK = "1453"
    CAT_ID = 1028  # ID категории: "Сладости

    logging.info("Начинаем парсинг Москвы...")
    msk_res = LentaAppParser("Москва", STORE_ALIAS_MSK, headers_msk).parse(CAT_ID)

    logging.info("Начинаем парсинг Санкт-Петербурга...")
    spb_res = LentaAppParser("Санкт-Петербург", STORE_ALIAS_SPB, headers_spb).parse(CAT_ID)

    all_res = msk_res + spb_res

    # Сохранение и аналитика
    if all_res:
        df = pd.DataFrame(all_res)
        os.makedirs("data", exist_ok=True)
        file_path = "data/data_lenta.xlsx"

        # Сохранение на разные листы (Исправлено)
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for city_name in df['city'].unique():
                # Фильтруем данные по городу
                city_df = df[df['city'] == city_name].copy()
                # Удаляем лишний столбец "город"
                city_df = city_df.drop(columns=['city'])
                # Записываем на лист с названием города
                city_df.to_excel(writer, sheet_name=city_name, index=False)

        logging.info(f"\nДанные успешно сохранены в {file_path}. Всего строк: {len(df)}")

        # Анализ расхождения цен между городами
        if df['city'].nunique() > 1:
            try:
                pivot = df.pivot_table(index='id', columns='city', values='regular_price').dropna()
                diff = pivot[pivot.iloc[:, 0] != pivot.iloc[:, 1]]
                logging.info(f"Аналитика: найдено {len(diff)} товаров с различающейся ценой в регионах.")
            except Exception as e:
                pass
    else:
        logging.error("\nСПИСОК ТОВАРОВ ПУСТ. Проверьте актуальность headers.")