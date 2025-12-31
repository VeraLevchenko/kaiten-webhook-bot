import os
import json
from pathlib import Path
from datetime import datetime, timedelta, date
from collections import defaultdict
import urllib3
import threading
import time
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from dotenv import load_dotenv
import requests

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

app = FastAPI()

# Часовой пояс Новокузнецка (UTC+7)
NOVOKUZNETSK_TZ = ZoneInfo("Asia/Novokuznetsk")

# ==========================
#  ПЕРСИСТЕНТНОСТЬ (JSON, 90 дней)
# ==========================

DATA_FILE = os.getenv("DATA_FILE", "./moves_by_date.json")
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))

data_lock = threading.Lock()

# Структура в памяти:
# moves_by_date["YYYY-MM-DD"]["ФИО"][(from_col, to_col)] = count
moves_by_date: dict = {}


def _prune_old_dates(store: dict, retention_days: int) -> None:
    """Удаляет даты старше retention_days (по календарю Новокузнецка)."""
    cutoff = (datetime.now(NOVOKUZNETSK_TZ).date() - timedelta(days=retention_days))
    for dstr in list(store.keys()):
        try:
            d = datetime.strptime(dstr, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:
            store.pop(dstr, None)


def load_moves_from_json() -> dict:
    """Загружает moves_by_date из JSON (если есть)."""
    path = Path(DATA_FILE)
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print(f"[PERSIST] ⚠️ Не смог прочитать {DATA_FILE}: {e}")
        return {}

    store = {}
    if isinstance(raw, dict):
        for dstr, users in raw.items():
            if not isinstance(users, dict):
                continue
            store[dstr] = {}
            for user, routes in users.items():
                if not isinstance(routes, dict):
                    continue
                dd = defaultdict(int)
                for route_str, count in routes.items():
                    if not isinstance(route_str, str):
                        continue
                    if "→" in route_str:
                        from_col, to_col = [p.strip() for p in route_str.split("→", 1)]
                    else:
                        from_col, to_col = route_str.strip(), ""
                    try:
                        dd[(from_col, to_col)] += int(count)
                    except Exception:
                        pass
                store[dstr][user] = dd

    _prune_old_dates(store, DATA_RETENTION_DAYS)
    return store


def save_moves_to_json(store: dict) -> None:
    """Сохраняет moves_by_date в JSON атомарно."""
    path = Path(DATA_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)

    raw = {}
    for dstr, users in store.items():
        raw[dstr] = {}
        for user, routes in users.items():
            raw[dstr][user] = {}
            for (from_col, to_col), count in routes.items():
                raw[dstr][user][f"{from_col} → {to_col}"] = int(count)

    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
    except Exception as e:
        print(f"[PERSIST] ❌ Ошибка сохранения {DATA_FILE}: {e}")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def persist_now() -> None:
    _prune_old_dates(moves_by_date, DATA_RETENTION_DAYS)
    save_moves_to_json(moves_by_date)


# ==========================
#  КАЛЕНДАРЬ (выходные/праздники)
# ==========================

# Праздники берём из env HOLIDAYS="YYYY-MM-DD,YYYY-MM-DD,..."
def load_holidays() -> set[str]:
    raw = os.getenv("HOLIDAYS", "").strip()
    if not raw:
        return set()
    return {p.strip() for p in raw.split(",") if p.strip()}


HOLIDAYS = load_holidays()


def now_nsk() -> datetime:
    return datetime.now(NOVOKUZNETSK_TZ)


def date_to_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # 5=Saturday,6=Sunday


def is_holiday(d: date) -> bool:
    return date_to_str(d) in HOLIDAYS


def is_workday(d: date) -> bool:
    return (not is_weekend(d)) and (not is_holiday(d))


def prev_workday(from_date: date) -> date:
    """Последний рабочий день ДО from_date (не включая from_date)."""
    d = from_date - timedelta(days=1)
    while not is_workday(d):
        d -= timedelta(days=1)
    return d


def today_str() -> str:
    return now_nsk().strftime("%Y-%m-%d")


# ==========================
#  СЛОВАРЬ КОЛОНОК
# ==========================

COLUMN_NAMES = {
    # Основной поток
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
    5474977: "Внести в ГИСОГД (градпланы)",
    5474978: "Внести в ИСОГД и архив",

    # Отказы
    5474950: "Подготовить отказ",
    5577124: "Проверка Габидулина Р.Р. (отказы)",
    5474965: "Внести в ГИСОГД (отказы)",
    5474969: "Внести в ИСОГД",
}


def get_column_name(column_id: int) -> str:
    return COLUMN_NAMES.get(column_id, f"Колонка {column_id}")


# ==========================
#  TELEGRAM
# ==========================

def send_telegram_report(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARNING] ========== TELEGRAM НЕ НАСТРОЕН ==========")
        print(f"[WARNING] TELEGRAM_TOKEN существует: {TELEGRAM_TOKEN is not None}")
        print(f"[WARNING] TELEGRAM_CHAT_ID значение: {TELEGRAM_CHAT_ID}")
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
        print(f"[TELEGRAM] ❌ Ошибка: HTTP {response.status_code} | {response.text[:300]}")
        return False
    except Exception as e:
        print(f"[ERROR] Исключение при отправке в Telegram: {e}")
        return False


# ==========================
#  УПРАВЛЕНЧЕСКИЙ ОТЧЁТ (Поток)
# ==========================

METRICS_ORDER = [
    "primary_intake",
    "rso_requests_done",
    "gpzu_prepared",
    "refusals_prepared",
    "head_checked",
    "gabidullina_checked",
    "gpzu_signed",
    "isogd_added",
]

METRICS = {
    # 1. внесены в кайтен и проведена первичная проверка:
    # попадание карточки в "Запросить ТОПО" ИЛИ "Подготовить отказ" (объединяем)
    "primary_intake": {
        "name": "Принято в работу (первичная проверка)",
        "column_ids": [5474956, 5474950],
    },

    # 2. выполнены запросы в РСО: попадание в WIP ОЖИДАНИЕ
    "rso_requests_done": {
        "name": "Выполнены запросы в РСО",
        "column_ids": [5485743],
    },

    # 3. подготовлены градпланы: попадание в "Проверка начальника отдела"
    "gpzu_prepared": {
        "name": "Подготовлены ГПЗУ",
        "column_ids": [5474974],
    },

    # 4. подготовлены отказы: попадание в "Проверка Габидулина Р.Р. (отказы)"
    "refusals_prepared": {
        "name": "Подготовлены отказы",
        "column_ids": [5577124],
    },

    # 5. проверены ГПЗУ начальником отдела: попадание в "Финализация..."
    "head_checked": {
        "name": "Проверены ГПЗУ начальником отдела",
        "column_ids": [5474975],
    },

    # 6. проверены Габидулиной (после начальника отдела): градпланы и отказы
    # Важно: 5577124 участвует и как "подготовлены отказы". Поэтому используем exclusive=True.
    "gabidullina_checked": {
        "name": "Проверены Габидулиной Р.Р.",
        "column_ids": [5542289, 5577124],
    },

    # 7. подписаны градпланы: попадание в "Внести в ГИСОГД (градпланы)" (id 5474977)
    "gpzu_signed": {
        "name": "Подписаны ГПЗУ",
        "column_ids": [5474977],
    },

    # 8. внесено в ГИСОГД: градпланы и отказы раздельно
    "isogd_added": {
        "name": "Внесено в ГИСОГД",
        "column_ids": {
            "gpzu": [5474978],     # Внести в ИСОГД и архив
            "refusals": [5474969], # Внести в ИСОГД (отказы)
        },
    },
}


def build_metric_targets(column_names: dict, metrics: dict):
    """Метрики (ID) -> целевые НАЗВАНИЯ колонок (routes у нас по названиям)."""
    targets = {}
    for key, spec in metrics.items():
        col_ids = spec["column_ids"]
        if isinstance(col_ids, dict):
            sub = {}
            for subkey, ids in col_ids.items():
                sub[subkey] = {column_names[i] for i in ids if i in column_names}
            targets[key] = sub
        else:
            targets[key] = {column_names[i] for i in col_ids if i in column_names}
    return targets


def count_metrics_for_date(
    date_str: str,
    moves_store: dict,
    column_names: dict,
    metrics: dict,
    metrics_order: list,
    exclusive: bool = True
):
    """
    Считаем метрики за дату.
    Событие = попадание карточки в целевую колонку (to_col_name).

    exclusive=True — одно событие учитывается только в одной метрике (по приоритету metrics_order).
    """
    stats = moves_store.get(date_str, {})
    metric_targets = build_metric_targets(column_names, metrics)

    totals_flat = defaultdict(int)
    totals_nested = defaultdict(lambda: defaultdict(int))

    for _, routes in stats.items():
        for (_, to_col), count in routes.items():
            matched = False
            for metric_key in metrics_order:
                targets = metric_targets.get(metric_key)

                if isinstance(targets, dict):
                    for subkey, target_set in targets.items():
                        if to_col in target_set:
                            totals_nested[metric_key][subkey] += count
                            matched = True
                            break
                else:
                    if to_col in targets:
                        totals_flat[metric_key] += count
                        matched = True

                if matched and exclusive:
                    break

    result_totals = {}
    for key in metrics_order:
        if key in totals_nested:
            result_totals[key] = dict(totals_nested[key])
        else:
            result_totals[key] = int(totals_flat.get(key, 0))

    return result_totals


def render_flow_report(date_str: str, totals: dict, metrics: dict) -> str:
    """Telegram-текст отчёта по потоку (без сотрудников)."""
    lines = [f"📊 Поток за {date_str}\n"]

    def add_if_nonzero(metric_key: str):
        value = totals.get(metric_key, 0)
        if isinstance(value, dict):
            return
        if value:
            lines.append(f"• {metrics[metric_key]['name']}: {value}")

    for key in [
        "primary_intake",
        "rso_requests_done",
        "gpzu_prepared",
        "refusals_prepared",
        "head_checked",
        "gabidullina_checked",
        "gpzu_signed",
    ]:
        add_if_nonzero(key)

    isogd = totals.get("isogd_added", {})
    if isinstance(isogd, dict):
        gpzu = int(isogd.get("gpzu", 0))
        refusals = int(isogd.get("refusals", 0))
        if gpzu or refusals:
            lines.append("• Внесено в ГИСОГД:")
            if gpzu:
                lines.append(f"  – градпланы: {gpzu}")
            if refusals:
                lines.append(f"  – отказы: {refusals}")

    # Если по всем метрикам нули — показать сообщение
    if len(lines) == 2:
        lines.append("Действий по метрикам не было.")

    return "\n".join(lines)


def generate_report_text_for_day(report_day: date) -> str:
    date_str = date_to_str(report_day)
    with data_lock:
        totals = count_metrics_for_date(
            date_str=date_str,
            moves_store=moves_by_date,
            column_names=COLUMN_NAMES,
            metrics=METRICS,
            metrics_order=METRICS_ORDER,
            exclusive=True
        )
    return render_flow_report(date_str, totals, METRICS)


# ==========================
#  АВТО-ОТПРАВКА (рабочие дни, 08:30; в понедельник — за пятницу)
# ==========================

def auto_send_daily_reports():
    print("[AUTO-REPORT] 🤖 Авто-отправка отчётов включена: 08:30 (Новокузнецк), только рабочие дни.")
    if HOLIDAYS:
        print(f"[AUTO-REPORT] Праздники из HOLIDAYS: {len(HOLIDAYS)}")

    while True:
        now = now_nsk()
        today_d = now.date()

        if now.hour == 8 and now.minute == 30:
            # Не отправляем в выходные и праздники
            if not is_workday(today_d):
                print(f"[AUTO-REPORT] ⛔ {date_to_str(today_d)} выходной/праздник — отчёт не отправляем.")
                time.sleep(120)
                continue

            report_day = prev_workday(today_d)
            text = generate_report_text_for_day(report_day)

            success = send_telegram_report(text)

            if success:
                print(f"[AUTO-REPORT] ✅ Отчёт за {date_to_str(report_day)} отправлен")
                # Очищаем только отправленный день (и сохраняем)
                with data_lock:
                    dstr = date_to_str(report_day)
                    if dstr in moves_by_date:
                        moves_by_date[dstr] = {}
                    persist_now()
            else:
                print(f"[AUTO-REPORT] ❌ Ошибка отправки отчёта за {date_to_str(report_day)}")

            time.sleep(120)
        else:
            time.sleep(30)


# ==========================
#  WEBHOOK
# ==========================

@app.post("/gradplan_process")
async def kaiten_webhook(request: Request):
    """
    Обработчик вебхука от Kaiten.
    Принимает уведомления о перемещении карточек и собирает статистику.
    """
    body = await request.json()

    if body.get("event") != "card:update":
        return {"ok": True}

    data = body.get("data", {})
    old = data.get("old", {})
    changes = data.get("changes", {})
    author = data.get("author", {})

    if "column_id" in changes:
        user_name = author.get("full_name") or author.get("username") or "Неизвестный пользователь"
        date_key = today_str()

        old_column_id = old.get("column_id")
        new_column_id = changes["column_id"]

        old_column_name = get_column_name(old_column_id)
        new_column_name = get_column_name(new_column_id)

        with data_lock:
            if date_key not in moves_by_date:
                moves_by_date[date_key] = {}

            if user_name not in moves_by_date[date_key]:
                moves_by_date[date_key][user_name] = defaultdict(int)

            route = (old_column_name, new_column_name)
            moves_by_date[date_key][user_name][route] += 1

            persist_now()

        print(f"[MOVE] {date_key} | {user_name}: {old_column_name} (ID:{old_column_id}) → {new_column_name} (ID:{new_column_id})")

    return {"ok": True}


# ==========================
#  РУЧНОЙ ОТЧЁТ
# ==========================

@app.get("/daily_report")
def daily_report():
    """
    Отправить отчёт за последний рабочий день.
    Если сегодня выходной/праздник — не отправляем.
    """
    today_d = now_nsk().date()
    if not is_workday(today_d):
        msg = f"⛔ Сегодня {date_to_str(today_d)} выходной/праздник — отчёт не отправлен."
        print(f"[DAILY_REPORT] {msg}")
        return {"ok": True, "skipped": True, "message": msg}

    report_day = prev_workday(today_d)
    text = generate_report_text_for_day(report_day)

    success = send_telegram_report(text)

    if success:
        with data_lock:
            dstr = date_to_str(report_day)
            if dstr in moves_by_date:
                moves_by_date[dstr] = {}
            persist_now()

    return {"ok": success, "report_day": date_to_str(report_day), "report": text}


@app.get("/test_report")
def test_report():
    """
    Тестовый отчёт: показывает наличие данных за последние 3 календарных дня.
    """
    print("[TEST-REPORT] 🧪 Отправка тестового отчёта")

    lines = ["🧪 ТЕСТОВЫЙ ОТЧЁТ\n"]
    now = now_nsk()
    lines.append(f"🕐 Текущее время: {now.strftime('%Y-%m-%d %H:%M:%S')} (Новокузнецк)")

    dates = [(now.date() - timedelta(days=i)) for i in range(3)]
    with data_lock:
        for d in dates:
            dstr = date_to_str(d)
            stats = moves_by_date.get(dstr, {})
            if stats:
                total = sum(sum(routes.values()) for routes in stats.values())
                lines.append(f"\n📅 {dstr}: всего перемещений: {total}")
                for user, routes in stats.items():
                    lines.append(f"  👤 {user}: {sum(routes.values())}")
            else:
                lines.append(f"\n📅 {dstr}: нет данных")

    text = "\n".join(lines)
    success = send_telegram_report(text)
    return {"ok": success, "report": text}


# ==========================
#  STARTUP
# ==========================

@app.on_event("startup")
async def startup_event():
    global moves_by_date
    with data_lock:
        moves_by_date = load_moves_from_json()

    report_thread = threading.Thread(target=auto_send_daily_reports, daemon=True)
    report_thread.start()

    print("[STARTUP] ✅ Сервер запущен!")
    print(f"[STARTUP] DATA_FILE={DATA_FILE}, retention={DATA_RETENTION_DAYS} days")
    if HOLIDAYS:
        print(f"[STARTUP] HOLIDAYS={len(HOLIDAYS)} дат")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
