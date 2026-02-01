#!/usr/bin/env python3
"""
Kaiten Webhook Bot для учёта перемещений карточек и формирования управленческих отчётов.

Основные функции:
- Приём webhook-уведомлений от Kaiten о перемещении карточек
- Сохранение ВСЕХ перемещений с полной информацией в JSON
- Формирование управленческих отчётов с учётом уникальности карточек
- Автоматическая отправка отчётов в Telegram в рабочие дни в 08:30
- Ручная генерация отчётов через CLI или HTTP endpoints
"""

import os
import json
import sys
import threading
import time
import argparse
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict
from monthly_reports import start_monthly_reports_thread


import urllib3
import requests
from fastapi import FastAPI, Request
from dotenv import load_dotenv
import uvicorn

# ====================================================================
# НАСТРОЙКИ
# ====================================================================

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Загружаем переменные окружения
load_dotenv()

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Часовой пояс Новокузнецка (UTC+7)
NOVOKUZNETSK_TZ = ZoneInfo("Asia/Novokuznetsk")

# Хранение данных
DATA_FILE = os.getenv("DATA_FILE", "/home/user1/projects/kaiten-webhook-bot/moves_by_date.json")
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))

# Праздники
def load_holidays() -> set[str]:
    raw = os.getenv("HOLIDAYS", "").strip()
    if not raw:
        return set()
    return {p.strip() for p in raw.split(",") if p.strip()}

HOLIDAYS = load_holidays()

# ====================================================================
# ИНИЦИАЛИЗАЦИЯ
# ====================================================================

app = FastAPI()
data_lock = threading.Lock()

# Глобальное хранилище:
# {
#   "YYYY-MM-DD": {
#     "moves": [
#       {
#         "card_id": "12345",
#         "card_title": "Заявка #123",
#         "user": "Иван Иванов",
#         "from_column_id": 5474973,
#         "from_column_name": "Подготовить ГПЗУ",
#         "to_column_id": 5474974,
#         "to_column_name": "Проверка начальника отдела",
#         "timestamp": "2025-12-31T14:30:45+07:00"
#       }
#     ]
#   }
# }
store: dict = {}

# ====================================================================
# СВОЙСТВА КАРТОЧЕК - РАСШИФРОВКА
# ====================================================================

# ID кастомных полей в Kaiten
PROPERTY_IDS = {
    "person_type": "id_270916",      # Тип лица (юр/физ)
    "submission_method": "id_270924", # Способ подачи
}

# Расшифровка значений для "Тип лица" (id_270916)
PERSON_TYPE_VALUES = {
    93406: "Физическое лицо",
    93407: "Юридическое лицо",
}

# Расшифровка значений для "Способ подачи" (id_270924)
SUBMISSION_METHOD_VALUES = {
    93413: "ЕПГУ",
    93414: "МФЦ",
    93415: "Личный приём",
}

def decode_person_type(value) -> str:
    """
    Расшифровать тип лица.
    
    Args:
        value: значение из properties (может быть список [93406] или число 93406)
    
    Returns:
        Читаемое название типа лица или "Неизвестно"
    """
    if isinstance(value, list) and len(value) > 0:
        value = value[0]
    
    if not value:
        return "Не указано"
    
    try:
        value = int(value)
    except (ValueError, TypeError):
        return "Неизвестно"
    
    return PERSON_TYPE_VALUES.get(value, f"ID:{value}")

def decode_submission_method(value) -> str:
    """
    Расшифровать способ подачи.
    
    Args:
        value: значение из properties (может быть список [93415] или число 93415)
    
    Returns:
        Читаемое название способа подачи или "Неизвестно"
    """
    if isinstance(value, list) and len(value) > 0:
        value = value[0]
    
    if not value:
        return "Не указано"
    
    try:
        value = int(value)
    except (ValueError, TypeError):
        return "Неизвестно"
    
    return SUBMISSION_METHOD_VALUES.get(value, f"ID:{value}")

def decode_card_properties(properties: dict) -> dict:
    """
    Расшифровать свойства карточки в читаемый вид.
    
    Args:
        properties: словарь свойств из webhook
    
    Returns:
        Словарь с расшифрованными свойствами
    """
    result = {}
    
    # Тип лица
    person_type_id = PROPERTY_IDS["person_type"]
    if person_type_id in properties:
        result["person_type"] = decode_person_type(properties[person_type_id])
    else:
        result["person_type"] = "Не указано"
    
    # Способ подачи
    submission_id = PROPERTY_IDS["submission_method"]
    if submission_id in properties:
        result["submission_method"] = decode_submission_method(properties[submission_id])
    else:
        result["submission_method"] = "Не указано"
    
    return result

# ====================================================================
# СЛОВАРЬ КОЛОНОК KAITEN
# ====================================================================

COLUMN_NAMES = {
    # Основной поток градпланов
    5474955: "Входящая проверка",
    5474956: "Запросить ТОПО",
    5524513: "Ожидание ТОПО",
    5474972: "Запросить ТУ у РСО",
    5485743: "WIP ОЖИДАНИЕ",
    5474973: "Подготовить ГПЗУ",
    5474974: "Проверка начальника отдела",
    5474975: "Финализация (добавление РСО,регистрация)",
    5542289: "Проверка Габидулина Р.Р. (градпланы)",
    5474976: "Подписание",
    5577161: "Внести в ГИСОГД (градпланы)",
    5474978: "Внести в ИСОГД и архив",
    
    # Поток отказов
    5474950: "Подготовить отказ",
    5577124: "Проверка Габидулина Р.Р. (отказы)",
    5474965: "Внести в ГИСОГД (отказы)",
    5474969: "Внести в ИСОГД",
}

def get_column_name(col_id) -> str:
    """Получить название колонки по её ID."""
    try:
        return COLUMN_NAMES.get(int(col_id), f"Колонка {col_id}")
    except Exception:
        return f"Колонка {col_id}"

# ====================================================================
# УПРАВЛЕНЧЕСКИЕ МЕТРИКИ
# ====================================================================

REPORT_ORDER = [
    "primary_intake",
    "rso_requests_done",
    "gpzu_prepared",
    "refusals_prepared",
    "head_checked",
    "gabidullina_checked",
    "gpzu_signed",
    "isogd_gpzu",
    "isogd_refusals",
]

METRICS = {
    "primary_intake": {
        "name": "Принято в работу (первичная проверка)",
        "ids": {5474956, 5474950},
    },
    "rso_requests_done": {
        "name": "Выполнены запросы в РСО",
        "ids": {5485743},
    },
    "gpzu_prepared": {
        "name": "Подготовлены ГПЗУ",
        "ids": {5474974},
    },
    "refusals_prepared": {
        "name": "Подготовлены отказы",
        "ids": {5577124},
    },
    "head_checked": {
        "name": "Проверены ГПЗУ начальником отдела",
        "ids": {5474975},
    },
    "gabidullina_checked": {
        "name": "Проверены Габидулиной Р.Р.",
        "ids": {5542289, 5577124},
    },
    "gpzu_signed": {
        "name": "Подписаны ГПЗУ",
        "ids": {5577161},
    },
    "isogd_gpzu": {
        "name": "Внесено в ГИСОГД (градпланы)",
        "ids": {5474978},
    },
    "isogd_refusals": {
        "name": "Внесено в ГИСОГД (отказы)",
        "ids": {5474969},
    },
}

# ====================================================================
# РАБОТА С ДАТАМИ И КАЛЕНДАРЁМ
# ====================================================================

def now_nsk() -> datetime:
    """Текущее время в часовом поясе Новокузнецка."""
    return datetime.now(NOVOKUZNETSK_TZ)

def date_to_str(d: date) -> str:
    """Преобразовать дату в строку YYYY-MM-DD."""
    return d.strftime("%Y-%m-%d")

def is_weekend(d: date) -> bool:
    """Проверить, является ли дата выходным (суббота или воскресенье)."""
    return d.weekday() >= 5

def is_holiday(d: date) -> bool:
    """Проверить, является ли дата праздником."""
    return date_to_str(d) in HOLIDAYS

def is_workday(d: date) -> bool:
    """Проверить, является ли дата рабочим днём."""
    return (not is_weekend(d)) and (not is_holiday(d))

def prev_workday(from_date: date) -> date:
    """Найти предыдущий рабочий день относительно указанной даты."""
    d = from_date - timedelta(days=1)
    while not is_workday(d):
        d -= timedelta(days=1)
    return d

# ====================================================================
# ПЕРСИСТЕНТНОСТЬ - ЗАГРУЗКА И СОХРАНЕНИЕ
# ====================================================================

def _prune_old_dates(store_data: dict) -> None:
    """Удалить даты старше DATA_RETENTION_DAYS."""
    cutoff = now_nsk().date() - timedelta(days=DATA_RETENTION_DAYS)
    for date_str in list(store_data.keys()):
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:
            store_data.pop(date_str, None)

def load_store() -> dict:
    """Загрузить хранилище из JSON файла."""
    path = Path(DATA_FILE)
    if not path.exists():
        return {}
    
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[PERSIST] ⚠️ Не смог прочитать {DATA_FILE}: {e}")
        return {}
    
    if not isinstance(raw, dict):
        return {}
    
    # Валидация структуры
    result = {}
    for date_str, day_data in raw.items():
        if not isinstance(day_data, dict):
            continue
        
        moves = day_data.get("moves", [])
        if not isinstance(moves, list):
            continue
        
        result[date_str] = {"moves": moves}
    
    return result

def save_store(store_data: dict) -> None:
    """Сохранить хранилище в JSON файл атомарно."""
    path = Path(DATA_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Удаляем старые даты перед сохранением
    _prune_old_dates(store_data)
    
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(store_data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
        print(f"[PERSIST] ✅ Данные сохранены в {DATA_FILE}")
    except Exception as e:
        print(f"[PERSIST] ❌ Ошибка сохранения: {e}")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

# ====================================================================
# ИЗВЛЕЧЕНИЕ ИДЕНТИФИКАТОРА КАРТОЧКИ
# ====================================================================

def extract_card_key(data: dict) -> str:
    """
    Извлечь уникальный идентификатор карточки из webhook данных.
    
    В Kaiten webhook структура:
    - data.old.id - ID карточки (ОСНОВНОЙ ИСТОЧНИК!)
    - data.old.uid - UID карточки
    - data.card_id - ID карточки (в некоторых событиях)
    """
    # 1. ОСНОВНОЙ ИСТОЧНИК: old.id - всегда присутствует при card:update
    old = data.get("old", {})
    if "id" in old and old["id"]:
        return str(old["id"])
    
    # 2. old.uid - уникальный UUID карточки
    if "uid" in old and old["uid"]:
        return f"uid_{old['uid']}"
    
    # 3. data.card_id - в некоторых событиях (например tag:add)
    if "card_id" in data and data["card_id"]:
        return str(data["card_id"])
    
    # 4. data.id
    if "id" in data and data["id"]:
        return str(data["id"])
    
    # 5. changes.card_id
    changes = data.get("changes", {})
    if "card_id" in changes and changes["card_id"]:
        return str(changes["card_id"])
    
    # 6. Если есть объект card (маловероятно, но проверим)
    card = data.get("card", {})
    if card:
        if "id" in card and card["id"]:
            return str(card["id"])
        if "uid" in card and card["uid"]:
            return str(card["uid"])
    
    # 7. ВРЕМЕННОЕ РЕШЕНИЕ (не должно использоваться)
    print("[ERROR] ❌ Не найден ID карточки ни в old.id, ни где-либо ещё!")
    return None

def extract_card_properties(data: dict) -> dict:
    """
    Извлечь свойства (properties) карточки.
    
    В Kaiten webhook:
    - data.old.properties - кастомные поля карточки
    
    Returns:
        Словарь со свойствами карточки
    """
    old = data.get("old", {})
    properties = old.get("properties", {})
    
    if not isinstance(properties, dict):
        return {}
    
    return properties

def extract_card_title(data: dict) -> str:
    """
    Извлечь название карточки.
    
    В Kaiten webhook:
    - data.old.title - название карточки
    """
    old = data.get("old", {})
    if "title" in old and old["title"]:
        return str(old["title"])[:200]
    
    # Fallback: ищем в других местах
    card = data.get("card", {})
    if "title" in card and card["title"]:
        return str(card["title"])[:200]
    
    changes = data.get("changes", {})
    if "title" in changes and changes["title"]:
        return str(changes["title"])[:200]
    
    return "Без названия"

# ====================================================================
# WEBHOOK ОБРАБОТЧИК
# ====================================================================

@app.post("/gradplan_process")
async def kaiten_webhook(request: Request):
    """
    Обработчик webhook-уведомлений от Kaiten.
    
    Принимает уведомления о перемещении карточек и сохраняет
    полную информацию о каждом перемещении.
    """
    body = await request.json()
    
    # Обрабатываем только события обновления карточек
    if body.get("event") != "card:update":
        return {"ok": True}
    
    data = body.get("data", {})
    changes = data.get("changes", {})
    old = data.get("old", {})
    author = data.get("author", {})
    
    # Интересуют только перемещения между колонками
    if "column_id" not in changes:
        return {"ok": True}
    
    new_column_id = changes.get("column_id")
    old_column_id = old.get("column_id")
    
    # Извлекаем данные
    card_id = extract_card_key(data)
    card_title = extract_card_title(data)
    card_properties = extract_card_properties(data)
    card_decoded = decode_card_properties(card_properties)
    user_name = author.get("full_name") or author.get("username") or "Неизвестный пользователь"
    
    if card_id is None:
        print(f"[ERROR] ❌ Не удалось извлечь card_id!")
        return {"ok": True}
    
    print(
        f"[INFO] ✅ card_id: {card_id} | title: {card_title} | "
        f"тип: {card_decoded.get('person_type', 'N/A')} | "
        f"подача: {card_decoded.get('submission_method', 'N/A')}"
    )
    
    # Выводим raw properties для диагностики (только если нужно)
    if card_properties:
        person_type_raw = card_properties.get("id_270916", "нет")
        submission_raw = card_properties.get("id_270924", "нет")
        print(f"[DEBUG] raw: id_270916={person_type_raw}, id_270924={submission_raw}")
    
    # Преобразуем ID колонок в числа
    try:
        new_column_id = int(new_column_id)
        old_column_id = int(old_column_id) if old_column_id else None
    except Exception:
        print("[WARN] ⚠️ Ошибка преобразования column_id")
        return {"ok": True}
    
    # Получаем названия колонок
    from_column_name = get_column_name(old_column_id) if old_column_id else "Неизвестно"
    to_column_name = get_column_name(new_column_id)
    
    # Текущая дата и время
    now = now_nsk()
    date_key = date_to_str(now.date())
    timestamp = now.isoformat()
    
    # Формируем запись о перемещении
    move_record = {
        "card_id": card_id,
        "title": card_title,
        "person_type": card_decoded.get("person_type", "Не указано"),
        "submission_method": card_decoded.get("submission_method", "Не указано"),
        "from_column_id": old_column_id,
        "from_column": from_column_name,
        "to_column_id": new_column_id,
        "to_column": to_column_name,
        "user": user_name,
        "timestamp": timestamp,
    }
    
    # Сохраняем
    with data_lock:
        day = store.setdefault(date_key, {"moves": []})
        moves = day.setdefault("moves", [])
        moves.append(move_record)
        
        save_store(store)
    
    print(
        f"[MOVE] {date_key} | {user_name}: {from_column_name} (ID:{old_column_id}) "
        f"→ {to_column_name} (ID:{new_column_id}) | card={card_id} | title={card_title[:50]}"
    )
    
    return {"ok": True}

# ====================================================================
# ПОСТРОЕНИЕ ОТЧЁТОВ
# ====================================================================

def build_report_totals(date_str: str) -> dict:
    """
    Построить итоговые значения метрик за указанную дату.
    
    Учитывается уникальность: одна карточка учитывается в каждой метрике только один раз,
    даже если она проходила через целевую колонку несколько раз.
    """
    day = store.get(date_str, {})
    moves = day.get("moves", [])
    
    if not isinstance(moves, list):
        moves = []
    
    # Для каждой метрики собираем множество уникальных card_id,
    # которые попали в колонки этой метрики
    metric_cards = {}
    for metric_key in REPORT_ORDER:
        metric_cards[metric_key] = set()
    
    # Проходим по всем перемещениям
    for move in moves:
        if not isinstance(move, dict):
            continue
        
        card_id = move.get("card_id")
        to_column_id = move.get("to_column_id")
        
        if not card_id or not to_column_id:
            continue
        
        # Проверяем каждую метрику
        for metric_key in REPORT_ORDER:
            target_ids = METRICS[metric_key]["ids"]
            if to_column_id in target_ids:
                metric_cards[metric_key].add(card_id)
    
    # Подсчитываем количество уникальных карточек для каждой метрики
    totals = {}
    for metric_key in REPORT_ORDER:
        totals[metric_key] = len(metric_cards[metric_key])
    
    return totals

def render_report(date_str: str, totals: dict) -> str:
    """Сформировать текст отчёта для Telegram."""
    lines = [f"📊 Поток за {date_str}\n"]
    
    # Показываем ВСЕ метрики, даже с нулевыми значениями
    for metric_key in REPORT_ORDER:
        value = totals.get(metric_key, 0)
        metric_name = METRICS[metric_key]["name"]
        lines.append(f"• {metric_name}: {value}")
    
    return "\n".join(lines)

# ====================================================================
# TELEGRAM
# ====================================================================

def send_telegram(text: str) -> bool:
    """Отправить сообщение в Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] ⚠️ Telegram не настроен")
        return False
    
    url = f"https://149.154.167.220/bot{TELEGRAM_TOKEN}/sendMessage"
    headers = {"Host": "api.telegram.org"}
    
    try:
        response = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            headers=headers,
            timeout=10,
            verify=False
        )
        if response.status_code == 200:
            print("[TELEGRAM] ✅ Отчёт отправлен")
            return True
        print(f"[TELEGRAM] ❌ Ошибка: HTTP {response.status_code}")
        return False
    except Exception as e:
        print(f"[TELEGRAM] ❌ Исключение: {e}")
        return False

# ====================================================================
# АВТОМАТИЧЕСКАЯ ОТПРАВКА ОТЧЁТОВ
# ====================================================================

def auto_send_daily_reports():
    """
    Автоматическая отправка отчётов каждый рабочий день в 08:35 (Новокузнецк).

    Правило:
    - Отправляем только по рабочим дням.
    - Отчёт формируем за предыдущий рабочий день:
      * если сегодня вторник–пятница → отчёт за вчера
      * если сегодня понедельник → отчёт за пятницу
    """
    print("[AUTO-REPORT] 🤖 08:35 (Новокузнецк), только рабочие дни. Выходные/праздники пропускаем.")
    print(f"[AUTO-REPORT] DATA_FILE={DATA_FILE}, retention={DATA_RETENTION_DAYS} days, HOLIDAYS={len(HOLIDAYS)}")

    while True:
        try:
            now = now_nsk()

            # Ждём до 08:35
            target = now.replace(hour=8, minute=35, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)

            time.sleep((target - now).total_seconds())

            # ВАЖНО: пересчитываем время ПОСЛЕ ожидания (чтобы today был реальным днём отправки)
            now = now_nsk()
            today = now.date()

            # Отправляем только в рабочий день
            if not is_workday(today):
                print(f"[AUTO-REPORT] ⏭️ {date_to_str(today)} - выходной/праздник, пропускаем")
                continue

            # Дата отчёта — предыдущий рабочий день (в т.ч. в понедельник за пятницу)
            report_date = prev_workday(today)
            report_date_str = date_to_str(report_date)

            print(f"[AUTO-REPORT] ⏰ Время отправки отчёта за {report_date_str}!")

            totals = build_report_totals(report_date_str)
            report_text = render_report(report_date_str, totals)
            send_telegram(report_text)

        except Exception as e:
            print(f"[AUTO-REPORT] ❌ Ошибка: {e}")
            time.sleep(60)


# ====================================================================
# HTTP ENDPOINTS
# ====================================================================

@app.get("/")
async def root():
    """Корневой эндпоинт."""
    return {
        "status": "ok",
        "service": "Kaiten Webhook Bot",
        "data_file": DATA_FILE,
        "retention_days": DATA_RETENTION_DAYS,
    }

@app.get("/test_report")
async def test_report():
    """Тестовый отчёт за вчера."""
    yesterday = prev_workday(now_nsk().date())
    date_str = date_to_str(yesterday)
    
    totals = build_report_totals(date_str)
    report_text = render_report(date_str, totals)
    
    return {
        "date": date_str,
        "report": report_text,
        "totals": totals,
    }

@app.get("/stats")
async def stats():
    """Статистика по хранилищу."""
    with data_lock:
        total_moves = sum(len(day.get("moves", [])) for day in store.values())
        dates = sorted(store.keys())
        
        return {
            "total_dates": len(dates),
            "total_moves": total_moves,
            "date_range": {
                "from": dates[0] if dates else None,
                "to": dates[-1] if dates else None,
            },
        }

# ====================================================================
# STARTUP
# ====================================================================

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске."""
    global store
    
    # Загружаем данные
    store = load_store()
    print(f"[STARTUP] ✅ Загружено дат: {len(store)}")
    print(f"[STARTUP] DATA_FILE={DATA_FILE}")
    print(f"[STARTUP] DATA_RETENTION_DAYS={DATA_RETENTION_DAYS}")
    print(f"[STARTUP] HOLIDAYS={len(HOLIDAYS)}")
    
    # Запускаем фоновый поток для автоотправки
    thread = threading.Thread(target=auto_send_daily_reports, daemon=True)
    thread.start()
    print("[STARTUP] ✅ Автоотправка отчётов запущена")

    # Ежемесячные отчёты (1-й рабочий день месяца)
    start_monthly_reports_thread(
        load_store_func=load_store,
        send_telegram_func=send_telegram,
        data_file_path=DATA_FILE,
        holidays=HOLIDAYS,
    )
    print("[STARTUP] ✅ Ежемесячные отчёты запущены")

# ====================================================================
# CLI
# ====================================================================

def cli_report(date_str: str, send: bool = False):
    """Сформировать отчёт через CLI."""
    global store
    store = load_store()
    
    totals = build_report_totals(date_str)
    report_text = render_report(date_str, totals)
    
    print(report_text)
    
    if send:
        sent = send_telegram(report_text)
        print(f"sent: {sent}")

def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description="Kaiten Webhook Bot")
    parser.add_argument("--report", type=str, help="Дата для отчёта (YYYY-MM-DD)")
    parser.add_argument("--send", action="store_true", help="Отправить отчёт в Telegram")
    
    args = parser.parse_args()
    
    if args.report:
        # Режим CLI: сформировать отчёт
        cli_report(args.report, args.send)
    else:
        # Режим сервера
        print("[STARTUP] 🚀 Запуск сервера...")
        uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()