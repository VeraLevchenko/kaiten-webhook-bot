import os
from datetime import datetime, timedelta
from collections import defaultdict
import urllib3

from fastapi import FastAPI, Request
from dotenv import load_dotenv
import requests

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

app = FastAPI()

# Структура: {"2025-12-10": {"Иванов": {("Бэклог", "В работе"): 3}}}
moves_by_date = {}

# Словарь: ID колонки -> Название колонки
COLUMN_NAMES = {
    5474955: "Входящая проверка",
    5474956: "ЕГРН + ТОПО + права",
    5524513: "Ожидание ТОПО",
    5474972: "Запросы РСО",
    5485743: "WIP ОЖИДАНИЕ",
    5474973: "Подготовка ГПЗУ",
    5474974: "Проверка начальника отдела",
    5474975: "Финализация (добавление РСО,регистрация)",
    5474976: "Подписание",
    5474977: "ГИСОГД",
    5474978: "Внести в ИСОГД и архив",
}


def today_str() -> str:
    """Получить текущую дату в формате YYYY-MM-DD."""
    return datetime.now().strftime("%Y-%m-%d")


def yesterday_str() -> str:
    """Получить вчерашнюю дату в формате YYYY-MM-DD."""
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_column_name(column_id: int) -> str:
    """
    Получить название колонки по её ID из словаря COLUMN_NAMES.
    Если ID не найден в словаре, возвращает "Колонка {ID}".
    """
    return COLUMN_NAMES.get(column_id, f"Колонка {column_id}")


@app.post("/gradplan_process")
async def kaiten_webhook(request: Request):
    """
    Обработчик вебхука от Kaiten.
    Принимает уведомления о перемещении карточек и собирает статистику.
    """
    body = await request.json()

    # Нас интересуют только обновления карточек
    if body.get("event") != "card:update":
        return {"ok": True}

    data = body.get("data", {})
    old = data.get("old", {})
    changes = data.get("changes", {})
    author = data.get("author", {})

    # Если изменился column_id — значит, карточку передвинули
    if "column_id" in changes:
        # Получаем имя пользователя
        user_name = author.get("full_name") or author.get("username") or "Неизвестный пользователь"
        date_key = today_str()

        # Получаем ID колонок
        old_column_id = old.get("column_id")
        new_column_id = changes["column_id"]

        # Получаем названия колонок из словаря
        old_column_name = get_column_name(old_column_id)
        new_column_name = get_column_name(new_column_id)

        # Инициализируем структуру данных, если её ещё нет
        if date_key not in moves_by_date:
            moves_by_date[date_key] = {}
        
        if user_name not in moves_by_date[date_key]:
            moves_by_date[date_key][user_name] = defaultdict(int)

        # Считаем перемещения по уникальному маршруту (из колонки -> в колонку)
        route = (old_column_name, new_column_name)
        moves_by_date[date_key][user_name][route] += 1

        # Логируем перемещение
        print(f"[MOVE] {date_key} | {user_name}: {old_column_name} (ID:{old_column_id}) → {new_column_name} (ID:{new_column_id})")

    return {"ok": True}


@app.get("/daily_report")
def daily_report():
    """
    Сформировать и отправить отчёт за вчерашний день в Telegram.
    Вызывается вручную или через cron.
    """
    y = yesterday_str()
    stats = moves_by_date.get(y, {})

    if not stats:
        text = f"📊 Отчёт за {y}\n\nПеремещений карточек не было."
    else:
        lines = [f"📊 Отчёт за {y}\n"]
        
        # Сортируем пользователей по общему количеству перемещений (от большего к меньшему)
        users_sorted = sorted(
            stats.items(),
            key=lambda x: sum(x[1].values()),
            reverse=True
        )
        
        for user, routes in users_sorted:
            # Считаем общее количество перемещений у пользователя
            total_moves = sum(routes.values())
            lines.append(f"👤 {user}: {total_moves} перемещений")
            
            # Сортируем маршруты по количеству перемещений (от большего к меньшему)
            routes_sorted = sorted(routes.items(), key=lambda x: -x[1])
            
            # Выводим каждый уникальный маршрут
            for (from_col, to_col), count in routes_sorted:
                # Правильное склонение слова "перемещение"
                if count == 1:
                    plural = "е"
                elif 2 <= count <= 4:
                    plural = "я"
                else:
                    plural = "й"
                lines.append(f"  • {count} перемещени{plural}: {from_col} → {to_col}")
            
            lines.append("")  # Пустая строка между пользователями для читаемости
        
        text = "\n".join(lines)

    # Отправляем отчёт в Telegram
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://149.154.167.220/bot{TELEGRAM_TOKEN}/sendMessage"
        headers = {"Host": "api.telegram.org"}
        
        # ОТЛАДКА
        print(f"[DEBUG] ========== ОТПРАВКА В TELEGRAM ==========")
        print(f"[DEBUG] TELEGRAM_TOKEN загружен: {TELEGRAM_TOKEN is not None}")
        print(f"[DEBUG] TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID} (тип: {type(TELEGRAM_CHAT_ID)})")
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Длина текста отчёта: {len(text)} символов")
        print(f"[DEBUG] Первые 200 символов текста: {text[:200]}")
        
        try:
            response = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                headers=headers,
                timeout=10,
                verify=False
            )
            print(f"[TELEGRAM] HTTP статус: {response.status_code}")
            print(f"[TELEGRAM] Ответ API: {response.text[:500]}")  # Первые 500 символов ответа
            
            if response.status_code == 200:
                print(f"[TELEGRAM] ✅ Отчёт успешно отправлен!")
            else:
                print(f"[TELEGRAM] ❌ Ошибка при отправке")
                
        except Exception as e:
            print(f"[ERROR] Исключение при отправке в Telegram: {e}")
    else:
        print(f"[WARNING] ========== TELEGRAM НЕ НАСТРОЕН ==========")
        print(f"[WARNING] TELEGRAM_TOKEN существует: {TELEGRAM_TOKEN is not None}")
        print(f"[WARNING] TELEGRAM_TOKEN значение: {TELEGRAM_TOKEN[:20] if TELEGRAM_TOKEN else 'None'}...")
        print(f"[WARNING] TELEGRAM_CHAT_ID значение: {TELEGRAM_CHAT_ID}")

    # Очищаем статистику за вчера, чтобы не дублировать при повторных вызовах
    if y in moves_by_date:
        moves_by_date[y] = {}

    return {"ok": True, "report": text}


# Этот блок позволяет запускать сервер через python3 app.py
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)