#!/usr/bin/env python3
"""
Бот уведомлений о заказах GGSel в Telegram-группу (версия для Railway / сервера).

Ключи в этот файл НЕ вписываются. Они задаются переменными окружения:
    GGSEL_SELLER_ID  - ID продавца GGSel
    GGSEL_API_KEY    - API-ключ GGSel
    TG_TOKEN         - токен бота от BotFather
    TG_CHAT_ID       - ID группы (с минусом)
"""

import functools
import hashlib
import html
import json
import os
import sys
import time

import requests

print = functools.partial(print, flush=True)  # чтобы логи появлялись сразу

SELLER_ID = os.getenv("GGSEL_SELLER_ID", "").strip()
API_KEY = os.getenv("GGSEL_API_KEY", "").strip()
TG_TOKEN = os.getenv("TG_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

CHECK_EVERY = int(os.getenv("CHECK_EVERY", "30"))  # секунд между проверками
TOP = int(os.getenv("TOP", "20"))                   # сколько последних продаж брать
API_BASE = "https://seller.ggsel.com/api_sellers/api"
STATE_FILE = os.getenv("STATE_FILE", "seen_orders.json")
TOKEN_LIFETIME = 60 * 60

_token = None
_token_time = 0.0


def hide(text):
    """Убирает токен и ключ из текста ошибок."""
    text = str(text)
    for secret in (TG_TOKEN, API_KEY):
        if secret:
            text = text.replace(secret, "***")
    return text


def login():
    global _token, _token_time
    ts = int(time.time())
    sign = hashlib.sha256((API_KEY + str(ts)).encode("utf-8")).hexdigest()
    r = requests.post(
        f"{API_BASE}/apilogin",
        json={"seller_id": int(SELLER_ID), "timestamp": ts, "sign": sign},
        headers={"Accept": "application/json"},
        timeout=20,
    )
    token = None
    try:
        token = r.json().get("token")
    except Exception:
        pass
    if not (r.ok and token):
        raise RuntimeError(f"вход в API GGSel не удался, код {r.status_code}: {r.text[:300]}")
    _token = token
    _token_time = time.time()


def get_token():
    if _token is None or time.time() - _token_time > TOKEN_LIFETIME:
        login()
    return _token


def get_last_sales():
    global _token
    r = requests.get(
        f"{API_BASE}/seller-last-sales",
        params={"token": get_token(), "seller_id": SELLER_ID, "top": TOP},
        headers={"Accept": "application/json"},
        timeout=20,
    )
    if r.status_code in (401, 403):
        _token = None
        raise RuntimeError("токен GGSel отклонён, получу новый")
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    return data.get("sales") or []


def sale_id(sale):
    return str(sale.get("invoice_id") or sale.get("id") or sale.get("inv") or "")


def get_order_info(invoice_id):
    """Подробности заказа: реальная сумма, выплата, количество. None при ошибке."""
    try:
        r = requests.get(
            f"{API_BASE}/purchase/info/{invoice_id}",
            params={"token": get_token()},
            headers={"Accept": "application/json"},
            timeout=20,
        )
        if not r.ok:
            print(f"Заказ {invoice_id}: подробности не получены, код {r.status_code}: {r.text[:200]}")
            return None
        return r.json().get("content") or None
    except Exception as e:
        print(f"Заказ {invoice_id}: подробности не получены:", hide(e)[:200])
        return None


def money(value, currency):
    sign = {"RUB": "₽", "RUR": "₽", "WMR": "₽", "USD": "$", "WMZ": "$",
            "EUR": "€", "WME": "€"}.get(str(currency or "").upper(), str(currency or ""))
    return f"{value} {sign}".strip()


def format_sale(sale):
    product = sale.get("product") or {}
    info = get_order_info(sale_id(sale)) or {}
    name = info.get("name") or product.get("name") or sale.get("name") or "Товар"
    lines = ["🛒 <b>Новый заказ на GGSel</b>", f"Товар: {html.escape(str(name))}"]

    if info.get("cnt_goods"):
        unit = info.get("unit_goods") or ""
        lines.append(f"Количество: {html.escape(str(info['cnt_goods']))} {html.escape(str(unit))}".strip())

    currency = info.get("currency_type")
    if info.get("amount") is not None:
        lines.append(f"Сумма заказа: {money(info['amount'], currency)}")
        if info.get("profit") is not None:
            lines.append(f"Ваша выплата: {money(info['profit'], currency)}")
    else:
        # подробности не получены - показываем хотя бы цену карточки
        for key in ("price_rub", "price_usd"):
            price = product.get(key)
            if price:
                suffix = {"price_rub": " ₽", "price_usd": " $"}[key]
                lines.append(f"Цена карточки: {price}{suffix}")
                break

    lines.append(f"Заказ №: {sale_id(sale)}")
    if sale.get("date"):
        lines.append(f"Дата: {html.escape(str(sale['date']))}")
    if product.get("id"):
        lines.append(f"ID товара: {product['id']}")
    return "\n".join(lines)


def send_telegram(text):
    r = requests.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
        json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
        timeout=20,
    )
    if not r.ok:
        raise RuntimeError(f"Telegram не принял сообщение: {r.text[:300]}")


def load_seen():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_seen(seen):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(seen[-1000:], f)
    except Exception:
        pass


def main():
    missing = [n for n, v in (
        ("GGSEL_SELLER_ID", SELLER_ID), ("GGSEL_API_KEY", API_KEY),
        ("TG_TOKEN", TG_TOKEN), ("TG_CHAT_ID", TG_CHAT_ID)) if not v]
    if missing:
        print("НЕ ЗАДАНЫ ПЕРЕМЕННЫЕ:", ", ".join(missing))
        sys.exit(1)

    try:
        sales = get_last_sales()
    except Exception as e:
        print("НЕ УДАЛОСЬ ПОЛУЧИТЬ ЗАКАЗЫ GGSEL:", hide(e))
        sys.exit(1)
    print(f"GGSel: связь есть, получено продаж: {len(sales)}.")

    try:
        send_telegram("✅ Бот заказов GGSel запущен. Жду новые заказы.")
    except Exception as e:
        print("НЕТ СВЯЗИ С TELEGRAM:", hide(e)[:300])
        sys.exit(1)
    print("Telegram: сообщение о запуске отправлено.")

    seen = load_seen()
    if seen is None:
        seen = [sale_id(s) for s in sales if sale_id(s)]
        save_seen(seen)

    print("Работаю.")
    while True:
        try:
            sales = get_last_sales()
            new = [s for s in sales if sale_id(s) and sale_id(s) not in seen]
            for sale in reversed(new):
                send_telegram(format_sale(sale))
                seen.append(sale_id(sale))
                save_seen(seen)
                print("Отправлен заказ", sale_id(sale))
        except Exception as e:
            print("Ошибка (попробую снова):", hide(e)[:200])
        time.sleep(CHECK_EVERY)


if __name__ == "__main__":
    main()
