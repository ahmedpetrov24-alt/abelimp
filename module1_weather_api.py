import requests
import mysql.connector
from mysql.connector import Error
import time

DB_CONFIG = {
    'host': 'localhost',      # или IP сервера
    'user': 'your_username',  # логин MySQL
    'password': 'your_password', # пароль MySQL
    'database': 'winii',
    'raise_on_warnings': True
}

START_DATE = "2025-12-01"
END_DATE = "2025-12-31"
API_URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_and_save_weather():
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # 1. Получаем список городов с координатами
        cursor.execute("SELECT country_id, latitude, longitude FROM countries")
        countries = cursor.fetchall()

        if not countries:
            print("⚠️ В таблице countries нет записей.")
            return

        print(f"📦 Найдено городов: {len(countries)}")

        # 2. SQL-запрос для вставки
        insert_sql = """
            INSERT IGNORE INTO weathercountry (country_id, data_temp, temp)
            VALUES (%s, %s, %s)
        """

        # 3. Проходим по каждому городу
        for idx, city in enumerate(countries, start=1):
            cid = city['country_id']
            lat = city['latitude']
            lon = city['longitude']
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": START_DATE,
                "end_date": END_DATE,
                "daily": "temperature_2m_min",
                "timezone": "auto"
            }

            try:
                response = requests.get(API_URL, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                daily = data.get("daily", {})
                dates = daily.get("time", [])
                temps = daily.get("temperature_2m_min", [])

                # Фильтруем даты, где температура не пришла (null)
                rows_to_insert = [
                    (cid, date, temp) 
                    for date, temp in zip(dates, temps) 
                    if temp is not None
                ]

                if rows_to_insert:
                    cursor.executemany(insert_sql, rows_to_insert)
                    conn.commit()
                    print(f"  ✅ [{idx}/{len(countries)}] country_id {cid}: добавлено {len(rows_to_insert)} записей")
                else:
                    print(f"  ⚠️ [{idx}/{len(countries)}] country_id {cid}: API не вернул данных о температуре")

            except requests.exceptions.RequestException as e:
                print(f"  ❌ [{idx}/{len(countries)}] Ошибка запроса API для {cid}: {e}")
                continue

        print("\n🎉 Все данные успешно обработаны!")

    except Error as db_err:
        print(f"\n❌ Ошибка подключения/запроса к БД: {db_err}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("🔌 Соединение с базой данных закрыто.")

if __name__ == "__main__":
    fetch_and_save_weather()