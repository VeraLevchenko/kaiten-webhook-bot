import os
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from dotenv import load_dotenv
import requests

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

app = FastAPI()

# {"2025-12-09": {"Иванов": 3, "Петров": 5}}
moves_by_date = {}


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def yesterday_str() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


@app.post("/gradplan_process")
async def kaiten_webhook(request: Request):
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
        user_name = author.get("full_name") or author.get("username") or "Неизвестный пользователь"
        date_key = today_str()

        if date_key not in moves_by_date:
            moves_by_date[date_key] = {}

        moves_by_date[date_key][user_name] = moves_by_date[date_key].get(user_name, 0) + 1

        print(f"[MOVE] {date_key} | {user_name}: {old.get('column_id')} → {changes['column_id']}")

    return {"ok": True}


@app.get("/daily_report")
def daily_report():
    """Отправить отчёт за вчерашний день в Telegram."""
    y = yesterday_str()
    stats = moves_by_date.get(y, {})

    if not stats:
        text = f"Отчёт за {y}: перемещений карточек не было."
    else:
        lines = [f"Отчёт по перемещениям за {y}:"]
        for user, cnt in sorted(stats.items(), key=lambda x: -x[1]):
            lines.append(f"— {user}: {cnt} перемещений")
        text = "\n".join(lines)

    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text})

    # очищаем статистику за вчера, чтобы не дублировать
    moves_by_date[y] = {}

    return {"ok": True}
