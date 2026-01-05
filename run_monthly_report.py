#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from datetime import date, timedelta

import app

from monthly_reports import (
    build_month_excel_report,
    aggregates_path_for,
    RU_MONTHS,
)


def month_range(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def main():
    p = argparse.ArgumentParser(description="Тест: сформировать Excel отчёт (3 вкладки) и отправить в Telegram.")
    p.add_argument("month", help="Месяц в формате YYYY-MM (например 2026-01)")
    p.add_argument("--send", action="store_true", help="Отправить Excel в Telegram.")
    p.add_argument("--data-file", default=getattr(app, "DATA_FILE", None),
                   help="Путь к moves_by_date.json (по умолчанию DATA_FILE из app.py).")
    args = p.parse_args()

    if not args.data_file:
        raise SystemExit("Не найден DATA_FILE в app.py и не передан --data-file")

    try:
        y, m = map(int, args.month.split("-"))
        if not (1 <= m <= 12):
            raise ValueError
    except Exception:
        raise SystemExit("Неверный формат месяца. Нужно YYYY-MM")

    app.DATA_FILE = args.data_file
    store = app.load_store()

    m_start, m_end = month_range(y, m)

    aggr_path = aggregates_path_for(args.data_file)
    filename, xlsx_bytes = build_month_excel_report(store, m_start, m_end, aggr_path)

    if args.send:
        app.send_telegram(f"Отчёт готов за {RU_MONTHS[m]} {y}.")
        ok = app.monthly_reports.send_telegram_document(xlsx_bytes, filename) if hasattr(app, "monthly_reports") else None
        # если app.monthly_reports не подцеплён, отправим через импорт из monthly_reports
        if ok is None:
            from monthly_reports import send_telegram_document
            ok = send_telegram_document(xlsx_bytes, filename)

        if not ok:
            raise SystemExit("Не удалось отправить Excel в Telegram.")
        print("✅ Excel отправлен в Telegram.")
    else:
        print(f"Файл сформирован: {filename}")
        print(f"Агрегаты обновлены: {aggr_path}")


if __name__ == "__main__":
    main()
