import os
import json
import threading
import time
from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import urllib3
import requests
from fastapi import FastAPI, Request
from dotenv import load_dotenv

# --- SSL warnings off (как у тебя было) ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Время Новокузнецка (UTC+7)
NOVOKUZNETSK_TZ = ZoneInfo("Asia/Novokuznetsk")

# Хранилище: ОДИН ФАЙЛ
DATA_FILE = os.getenv("DATA_FILE", "./moves_by_date.json")
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))

# Праздники в .env: HOLIDAYS=2026-01-01,2026-01-02,...
def load_holidays() -> set[str]:
    raw = os.getenv("HOLIDAYS", "").strip()
    if not raw:
        return set()
    return {p.strip() for p in raw.split(",") if p.strip()}

HOLIDAYS = load_holidays()

app = FastAPI()
data_lock = threading.Lock()

# ==========================
#  COLUMN NAMES (для читаемости логов)
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

    # ВАЖНО: новая колонка вместо удалённой 5474977
    5577161: "Внести в ГИСОГД (градпланы)",

    5474978: "Внести в ИСОГД и архив",

    # Отказы
    5474950: "Подготовить отказ",
    5577124: "Проверка Габидулина Р.Р. (отказы)",
    5474965: "Внести в ГИСОГД (отказы)",
    5474969: "Внести в ИСОГД",
}

def get_column_name(col_id) -> str:
    try:
        return COLUMN_NAMES.get(int(col_id), f"Колонка {col_id}")
    except Exception:
        return f"Колонка {col_id}"

# ==========================
#  УПРАВЛЕНЧЕСКИЕ МЕТРИКИ (по ID колонок)
#  ЛОГИКА: одна карточка в одной колонке за день считается 1 раз
# ==========================
METRICS = {
    "primary_intake": {
        "name": "Принято в работу (первичная проверка)",
        "ids": {5474956, 5474950},  # объединяем
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
        "ids": {5542289, 5577124},  # градпланы + отказы
    },
    "gpzu_signed": {
        "name": "Подписаны ГПЗУ",
        "ids": {5577161},  # новая колонка
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

# ==========================
#  ВРЕМЯ / РАБОЧИЕ ДНИ
# ==========================
def now_nsk() -> datetime:
    return datetime.now(NOVOKUZNETSK_TZ)

def date_to_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # Sat/Sun

def is_holiday(d: date) -> bool:
    return date_to_str(d) in HOLIDAYS

def is_workday(d: date) -> bool:
    return (not is_weekend(d)) and (not is_holiday(d))

def prev_workday(from_date: date) -> date:
    """Последний рабочий день ДО from_date (не включая)."""
    d = from_date - timedelta(days=1)
    while not is_workday(d):
        d -= timedelta(days=1)
    return d

# ==========================
#  ХРАНЕНИЕ (ОДИН moves_by_date.json)
#
#  Новый формат:
#  {
#    "YYYY-MM-DD": {
#      "cards": {
#        "CARD_KEY": [col_id, col_id, ...]  # уникальный список (set сохраняем как list)
#      },
#      "legacy": {...}  # опционально: старый журнал "пользователь -> маршруты" (если уже был)
#    }
#  }
#
#  ВАЖНО:
#  - Отчёт строится ТОЛЬКО по "cards"
#  - "legacy" можно оставить для истории/отладки
# ==========================
def _prune_old_dates(store: dict) -> None:
    cutoff = (now_nsk().date() - timedelta(days=DATA_RETENTION_DAYS))
    for dstr in list(store.keys()):
        try:
            d = datetime.strptime(dstr, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:
            store.pop(dstr, None)

def load_store() -> dict:
    p = Path(DATA_FILE)
    if not p.exists():
        return {}

    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[PERSIST] ⚠️ Не смог прочитать {DATA_FILE}: {e}")
        return {}

    store = {}
    if not isinstance(raw, dict):
        return {}

    for dstr, payload in raw.items():
        # Если файл уже в новом формате
        if isinstance(payload, dict) and isinstance(payload.get("cards"), dict):
            cards = {}
            for card_key, cols in payload["cards"].items():
                if isinstance(cols, list):
                    s = set()
                    for x in cols:
                        try:
                            s.add(int(x))
                        except Exception:
                            pass
                    cards[str(card_key)] = s
            store[dstr] = {"cards": cards}
            if "legacy" in payload:
                store[dstr]["legacy"] = payload.get("legacy")
        else:
            # Старый формат (например: user -> route -> count). Сохраняем как legacy.
            store[dstr] = {"cards": {}, "legacy": payload}

    _prune_old_dates(store)
    return store

def save_store(store: dict) -> None:
    _prune_old_dates(store)
    out = {}

    for dstr, payload in store.items():
        cards_out = {}
        cards = payload.get("cards", {})
        if isinstance(cards, dict):
            for card_key, col_set in cards.items():
                if isinstance(col_set, set):
                    cards_out[str(card_key)] = sorted(list(col_set))
                elif isinstance(col_set, list):
                    # на всякий случай
                    fixed = set()
                    for x in col_set:
                        try:
                            fixed.add(int(x))
                        except Exception:
                            pass
                    cards_out[str(card_key)] = sorted(list(fixed))

        out[dstr] = {"cards": cards_out}

        # legacy сохраняем как есть (для истории)
        if "legacy" in payload:
            out[dstr]["legacy"] = payload.get("legacy")

    p = Path(DATA_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

# Глобальное хранилище в памяти
store: dict = {}

# ==========================
#  TELEGRAM
# ==========================
def send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] ⚠️ Не настроен TELEGRAM_TOKEN/TELEGRAM_CHAT_ID")
        return False

    # как у тебя было: запрос на IP + Host header
    url = f"https://149.154.167.220/bot{TELEGRAM_TOKEN}/sendMessage"
    headers = {"Host": "api.telegram.org"}

    try:
        resp = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            headers=headers,
            timeout=15,
            verify=False,
        )
        if resp.status_code == 200:
            return True
        print(f"[TELEGRAM] ❌ HTTP {resp.status_code}: {resp.text[:300]}")
        return False
    except Exception as e:
        print(f"[TELEGRAM] ❌ Exception: {e}")
        return False

# ==========================
#  ОТЧЁТ (уникальные карточки)
# ==========================
def count_any(day_cards: dict, ids: set[int]) -> int:
    # day_cards: card_key -> set(col_id)
    return sum(1 for _, cols in day_cards.items() if cols.intersection(ids))

def build_report_totals(date_str: str) -> dict:
    day = store.get(date_str, {})
    day_cards = day.get("cards", {})
    if not isinstance(day_cards, dict):
        day_cards = {}

    totals = {}
    for key in REPORT_ORDER:
        totals[key] = count_any(day_cards, METRICS[key]["ids"])
    return totals

def render_report(date_str: str, totals: dict) -> str:
    lines = [f"📊 Поток за {date_str}\n"]

    # всегда показываем все метрики (включая 0)
    lines.append(f"• {METRICS['primary_intake']['name']}: {totals.get('primary_intake', 0)}")
    lines.append(f"• {METRICS['rso_requests_done']['name']}: {totals.get('rso_requests_done', 0)}")
    lines.append(f"• {METRICS['gpzu_prepared']['name']}: {totals.get('gpzu_prepared', 0)}")
    lines.append(f"• {METRICS['refusals_prepared']['name']}: {totals.get('refusals_prepared', 0)}")
    lines.append(f"• {METRICS['head_checked']['name']}: {totals.get('head_checked', 0)}")
    lines.append(f"• {METRICS['gabidullina_checked']['name']}: {totals.get('gabidullina_checked', 0)}")
    lines.append(f"• {METRICS['gpzu_signed']['name']}: {totals.get('gpzu_signed', 0)}")

    lines.append("• Внесено в ГИСОГД:")
    lines.append(f"  – градпланы: {totals.get('isogd_gpzu', 0)}")
    lines.append(f"  – отказы: {totals.get('isogd_refusals', 0)}")

    return "\n".join(lines)

def generate_report_text(date_str: str) -> str:
    with data_lock:
        totals = build_report_totals(date_str)
    return render_report(date_str, totals)

# ==========================
#  АВТООТЧЁТ 08:30 (Новокузнецк)
#  - в выходные/праздники НЕ отправляет
#  - в понедельник отправляет за пятницу (за последний рабочий день)
# ==========================
def auto_send_daily_reports():
    print("[AUTO-REPORT] 🤖 08:30 (Новокузнецк), только рабочие дни. Выходные/праздники пропускаем.")
    print(f"[AUTO-REPORT] DATA_FILE={DATA_FILE}, retention={DATA_RETENTION_DAYS} days, HOLIDAYS={len(HOLIDAYS)}")

    while True:
        now = now_nsk()
        today_d = now.date()

        if now.hour == 8 and now.minute == 30:
            if not is_workday(today_d):
                print(f"[AUTO-REPORT] ⛔ {date_to_str(today_d)} выходной/праздник — не отправляем.")
                time.sleep(120)
                continue

            report_day = prev_workday(today_d)
            report_str = date_to_str(report_day)

            text = generate_report_text(report_str)
            ok = send_telegram(text)
            if ok:
                print(f"[AUTO-REPORT] ✅ Отправлено за {report_str}")
            else:
                print(f"[AUTO-REPORT] ❌ Ошибка отправки за {report_str}")

            time.sleep(120)
        else:
            time.sleep(30)

# ==========================
#  WEBHOOK: фиксируем уникальный заход карточки в колонку за день
# ==========================
def extract_card_key(data: dict) -> str | None:
    """
    Kaiten часто НЕ присылает card.id там, где ожидаем.
    Нам нужен стабильный ключ, чтобы:
      - одна и та же карточка распознавалась всегда одинаково
      - можно было дедуплицировать "карточка уже была в этой колонке сегодня"

    Приоритет:
      1) card.id / card.uid / card.external_id / card.number
      2) card.url (идеально, если приходит)
      3) title (как крайний вариант, но может быть не уникальным)
      4) data.card_id / data.id
    """
    card = data.get("card", {})
    if isinstance(card, dict):
        for key in ("id", "uid", "external_id", "number"):
            if card.get(key) is not None:
                return str(card.get(key))

        if card.get("url"):
            return str(card["url"])

        if card.get("title"):
            return f"title::{card['title']}"

    for key in ("card_id", "id"):
        if data.get(key) is not None:
            return str(data.get(key))

    return None

@app.post("/gradplan_process")
async def kaiten_webhook(request: Request):
    body = await request.json()

    if body.get("event") != "card:update":
        return {"ok": True}

    data = body.get("data", {})
    changes = data.get("changes", {})
    old = data.get("old", {})
    author = data.get("author", {})

    if "column_id" not in changes:
        return {"ok": True}

    new_column_id = changes.get("column_id")
    old_column_id = old.get("column_id")

    user_name = author.get("full_name") or author.get("username") or "Неизвестный пользователь"
    date_key = date_to_str(now_nsk().date())

    card_key = extract_card_key(data)
    if card_key is None:
        # Без ключа нельзя дедуплицировать "по карточке"
        print("[WARN] card_key not found; пропускаю учёт cards")
        return {"ok": True}

    try:
        new_column_id = int(new_column_id)
    except Exception:
        print("[WARN] new_column_id is not int; пропускаю")
        return {"ok": True}

    old_name = get_column_name(old_column_id)
    new_name = get_column_name(new_column_id)

    with data_lock:
        day = store.setdefault(date_key, {"cards": {}})
        cards = day.setdefault("cards", {})

        # 1) Уникальные попадания (главное для отчёта)
        col_set = cards.setdefault(str(card_key), set())
        col_set.add(new_column_id)

        # 2) Legacy-журнал маршрутов (для истории/отладки, если хочешь сохранить)
        legacy = day.setdefault("legacy", {})
        legacy.setdefault(user_name, {})
        route_key = f"{old_name} → {new_name}"
        legacy[user_name][route_key] = int(legacy[user_name].get(route_key, 0)) + 1

        # чистим и сохраняем
        _prune_old_dates(store)
        save_store(store)

    print(
        f"[MOVE] {date_key} | {user_name}: {old_name} (ID:{old_column_id}) → {new_name} (ID:{new_column_id})"
        f" | card_key={card_key}"
    )

    return {"ok": True}

# ==========================
#  РУЧНЫЕ ЭНДПОИНТЫ
# ==========================
@app.get("/report/{date_str}")
def report_for_date(date_str: str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return {"ok": False, "error": "Неверный формат даты. Нужно YYYY-MM-DD"}

    text = generate_report_text(date_str)
    ok = send_telegram(text)
    return {"ok": ok, "report_day": date_str, "report": text}

@app.get("/daily_report")
def daily_report():
    """Ручной запуск «как авто»: за последний рабочий день (если сегодня рабочий)."""
    today_d = now_nsk().date()
    if not is_workday(today_d):
        msg = f"⛔ Сегодня {date_to_str(today_d)} выходной/праздник — отчёт не отправлен."
        return {"ok": True, "skipped": True, "message": msg}

    report_day = prev_workday(today_d)
    report_str = date_to_str(report_day)
    text = generate_report_text(report_str)
    ok = send_telegram(text)
    return {"ok": ok, "report_day": report_str, "report": text}

@app.get("/test_report")
def test_report():
    """Тест: сколько карточек фиксировалось за последние 3 дня (по новому формату)."""
    now = now_nsk()
    lines = ["🧪 ТЕСТОВЫЙ ОТЧЁТ\n", f"🕐 {now.strftime('%Y-%m-%d %H:%M:%S')} (Новокузнецк)"]

    with data_lock:
        for i in range(3):
            d = (now.date() - timedelta(days=i))
            dstr = date_to_str(d)
            day_cards = store.get(dstr, {}).get("cards", {})
            lines.append(f"\n📅 {dstr}: карточек с событиями: {len(day_cards) if isinstance(day_cards, dict) else 0}")

    text = "\n".join(lines)
    ok = send_telegram(text)
    return {"ok": ok, "report": text}

# ==========================
#  STARTUP
# ==========================
@app.on_event("startup")
async def startup_event():
    global store
    with data_lock:
        store = load_store()
        save_store(store)  # подрежем старое/нормализуем формат

    t = threading.Thread(target=auto_send_daily_reports, daemon=True)
    t.start()

    print("[STARTUP] ✅ Сервер запущен")
    print(f"[STARTUP] DATA_FILE={DATA_FILE}")
    print(f"[STARTUP] DATA_RETENTION_DAYS={DATA_RETENTION_DAYS}")
    print(f"[STARTUP] HOLIDAYS={len(HOLIDAYS)}")

# ==========================
#  CLI: посмотреть/отправить отчёт (НЕ запускает сервер)
# ==========================
def cli_report(date_str: str, send: bool) -> int:
    global store
    with data_lock:
        store = load_store()

    text = generate_report_text(date_str)
    print(text)

    if send:
        ok = send_telegram(text)
        print("sent:", ok)
        return 0 if ok else 2
    return 0

if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser()
    parser.add_argument("--report", help="Показать отчёт за дату YYYY-MM-DD (без запуска сервера)")
    parser.add_argument("--send", action="store_true", help="С отправкой в Telegram (использовать вместе с --report)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.report:
        try:
            datetime.strptime(args.report, "%Y-%m-%d")
        except ValueError:
            print("Ошибка: дата должна быть в формате YYYY-MM-DD")
            raise SystemExit(1)
        raise SystemExit(cli_report(args.report, args.send))

    uvicorn.run(app, host=args.host, port=args.port)
