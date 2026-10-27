#!/usr/bin/env python3
"""
Бот уведомлений о заказах GGSel в Telegram-группу (версия для Railway / сервера).

Ключи в этот файл НЕ вписываются. Они задаются переменными окружения:
    GGSEL_SELLER_ID  - ID продавца GGSel
    GGSEL_API_KEY    - API-ключ GGSel
    TG_TOKEN         - токен бота от BotFather
    TG_CHAT_ID       - ID группы (с минусом)

Автовыдача (см. autodelivery.py и products.json):
    AUTODELIVERY     - on / off
    EPINBY_API_KEY   - X-API-Key из бота EpinBy
    DRY_RUN          - 1 = тест без покупок (по умолчанию), 0 = боевой режим
    STATE_DIR        - папка Volume на Railway, чтобы состояние не терялось при перезапуске
"""

import functools
import hashlib
import html
import json
import os
import sys
import time

import requests

import autodelivery

print = functools.partial(print, flush=True)  # чтобы логи появлялись сразу

SELLER_ID = os.getenv("GGSEL_SELLER_ID", "").strip()
API_KEY = os.getenv("GGSEL_API_KEY", "").strip()
TG_TOKEN = os.getenv("TG_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

CHECK_EVERY = int(os.getenv("CHECK_EVERY", "60"))  # секунд между проверками
# Пауза после ошибки: растёт 1 → 2 → 4 → ... до 15 минут, чтобы не долбить GGSel
BACKOFF_START = 60
BACKOFF_MAX = 15 * 60
TOP = int(os.getenv("TOP", "20"))                   # сколько последних продаж брать
API_BASE = "https://seller.ggsel.com/api_sellers/api"
STATE_FILE = os.getenv("STATE_FILE") or os.path.join(os.getenv("STATE_DIR", "."), "seen_orders.json")
TOKEN_LIFETIME = 60 * 60

_token = None
_token_time = 0.0


def hide(text):
    """Убирает токен и ключ из текста ошибок."""
    text = str(text)
    for secret in (TG_TOKEN, API_KEY, autodelivery.EPINBY_KEY):
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


def send_chat(invoice_id, text):
    """Сообщение покупателю в чат заказа GGSel."""
    r = requests.post(
        f"{API_BASE}/debates/v2",
        params={"token": get_token(), "id_i": invoice_id},
        json={"message": text},
        headers={"Accept": "application/json"},
        timeout=20,
    )
    if not r.ok:
        raise RuntimeError(f"GGSel не принял сообщение, код {r.status_code}: {r.text[:200]}")


def sale_product_id(sale):
    product = sale.get("product") or {}
    return str(product.get("id") or sale.get("product_id") or sale.get("id_goods") or "")


def format_sale(sale, info):
    product = sale.get("product") or {}
    info = info or {}
    # название берём из списка продаж: в подробностях заказа вместо него бывает код
    name = product.get("name") or sale.get("name") or info.get("name") or "Товар"
    lines = ["🛒 <b>Новый заказ на GGSel</b>", f"Товар: {html.escape(str(name))}"]

    if info.get("cnt_goods"):
        cnt = str(info["cnt_goods"])
        if cnt.endswith(".0"):
            cnt = cnt[:-2]
        unit = info.get("unit_goods") or ""
        lines.append(f"Количество: {html.escape(cnt)} {html.escape(str(unit))}".strip())

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


def notify(text):
    """Служебное сообщение в группу. Не падает, если Telegram недоступен."""
    try:
        send_telegram(text)
        return True
    except Exception as e:
        print("Telegram:", hide(e)[:300])
        return False


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

    # Раньше при ошибке бот завершался, Railway тут же перезапускал его,
    # и вход в API шёл каждые пару секунд — из-за этого GGSel и блокировал.
    # Теперь бот не падает, а ждёт с растущей паузой.
    delay = BACKOFF_START
    warned = False
    while True:
        try:
            sales = get_last_sales()
            break
        except Exception as e:
            print(f"НЕ УДАЛОСЬ ПОЛУЧИТЬ ЗАКАЗЫ GGSEL (жду {delay // 60} мин):", hide(e)[:300])
            if not warned:
                warned = notify("⚠️ Бот GGSel не может подключиться к API GGSel. "
                                "Пробую снова, заказы пока проверяйте в кабинете.")
            time.sleep(delay)
            delay = min(delay * 2, BACKOFF_MAX)
    print(f"GGSel: связь есть, получено продаж: {len(sales)}.")

    if notify("✅ Бот заказов GGSel запущен, связь с GGSel есть. Жду новые заказы."):
        print("Telegram: сообщение о запуске отправлено.")

    autodelivery.setup(notify=notify, send_chat=send_chat, hide=hide)

    seen = load_seen()
    if seen is None:
        seen = [sale_id(s) for s in sales if sale_id(s)]
        save_seen(seen)

    print("Работаю.")
    delay = BACKOFF_START
    errors = 0
    while True:
        try:
            sales = get_last_sales()
            if errors >= 3:
                notify("✅ Связь с GGSel восстановлена.")
            errors, delay = 0, BACKOFF_START
            new = [s for s in sales if sale_id(s) and sale_id(s) not in seen]
            for sale in reversed(new):
                info = get_order_info(sale_id(sale))
                send_telegram(format_sale(sale, info))
                seen.append(sale_id(sale))
                save_seen(seen)
                print("Отправлен заказ", sale_id(sale))
                if info is not None:
                    pid = sale_product_id(sale) or str(info.get("item_id") or info.get("id_goods") or "")
                    if autodelivery.start(sale, info, sale_id(sale), pid):
                        print("Автовыдача: заказ", sale_id(sale), "взят в работу")
            time.sleep(CHECK_EVERY)
        except Exception as e:
            errors += 1
            print(f"Ошибка (попробую снова через {delay // 60} мин):", hide(e)[:300])
            if errors == 3:
                notify("⚠️ Бот GGSel потерял связь с API GGSel. "
                       "Пробую снова, заказы пока проверяйте в кабинете.")
            time.sleep(delay)
            delay = min(delay * 2, BACKOFF_MAX)


if __name__ == "__main__":
    main()