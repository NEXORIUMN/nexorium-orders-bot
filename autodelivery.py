"""
Автовыдача заказов GGSel через EpinBy Public API.

Как работает:
  1. main.py видит новый заказ и вызывает start(sale, info).
  2. Если товар есть в products.json, из опций заказа берутся количество UC и ID игрока.
  3. Количество UC раскладывается на пакеты EpinBy по минимальной цене (985 = 660 + 325).
  4. ID проверяется в EpinBy, затем по каждому пакету создаётся заказ.
     Не прошёл основной товар - пробуется запасной из того же списка "ids".
  5. Бот ждёт статус COMPLETED, пишет покупателю в чат GGSel и отчитывается в группу.
  6. Если что-то не получилось - сообщение в группу, заказ доделывается вручную.

Защита от двойной выдачи: каждый заказ в EpinBy создаётся с ключом идемпотентности,
собранным из номера заказа GGSel. Повторный запрос с тем же ключом не создаёт новый заказ,
а возвращает уже существующий. Поэтому перезапуск бота посреди выдачи безопасен.

Переменные окружения:
    EPINBY_API_KEY  - X-API-Key из бота EpinBy (обязательно для автовыдачи)
    AUTODELIVERY    - "on" чтобы включить (по умолчанию выключено)
    LOW_BALANCE     - порог в $, ниже которого бот напоминает пополнить баланс (по умолчанию 20)
    DRY_RUN         - "1" (по умолчанию): ничего не покупать, только показать в группе план.
                      "0": боевой режим, деньги списываются.
    STATE_DIR       - папка для файла состояния (на Railway - путь к подключённому Volume)
    PACKS_DRY_RUN   - "1" (по умолчанию): товары "kind": "epinby_mix" (UC + наборы, Prime,
                      Elite Pass по названию) в тестовом режиме. "0": боевой режим.
    BUYER_ERROR_MESSAGES - "on": при ошибках писать покупателю в чат GGSel.
                      По умолчанию выключено: при ошибке пишется только в группу.

Второй поставщик — G2Bulk (см. g2bulk.py). Товары с "kind": "g2bulk" в products.json.
    G2BULK_API_KEY  - X-API-Key из кабинета g2bulk.com
    G2BULK_DRY_RUN  - "1" (по умолчанию): товары G2Bulk работают в тестовом режиме,
                      даже если DRY_RUN=0. "0": боевой режим для G2Bulk.
    G2BULK_LOW_BALANCE - порог баланса G2Bulk в $ (по умолчанию 20)
"""

import functools
import json
import os
import re
import threading
import time

import requests

import g2bulk

print = functools.partial(print, flush=True)

EPINBY_API = "https://epinby.com/api/v1"
EPINBY_KEY = os.getenv("EPINBY_API_KEY", "").strip()
ENABLED = os.getenv("AUTODELIVERY", "off").strip().lower() in ("on", "1", "true", "yes")
DRY_RUN = os.getenv("DRY_RUN", "1").strip().lower() not in ("0", "off", "false", "no")
STATE_DIR = os.getenv("STATE_DIR", ".").strip() or "."
STATE_FILE = os.path.join(STATE_DIR, "deliveries.json")
PRODUCTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.json")

LOW_BALANCE = float(os.getenv("LOW_BALANCE", "20") or 20)  # предупреждать, если баланс ниже ($)
G2_DRY_RUN = DRY_RUN or os.getenv("G2BULK_DRY_RUN", "1").strip().lower() not in ("0", "off", "false", "no")
BUYER_ERROR_MESSAGES = os.getenv("BUYER_ERROR_MESSAGES", "off").strip().lower() in ("on", "1", "true", "yes")
# новые товары EpinBy (наборы, Prime, Elite Pass): отдельный тестовый режим
MIX_DRY_RUN = DRY_RUN or os.getenv("PACKS_DRY_RUN", "1").strip().lower() not in ("0", "off", "false", "no")
G2_LOW_BALANCE = float(os.getenv("G2BULK_LOW_BALANCE", "20") or 20)
POLL_EVERY = 15          # секунд между проверками статуса заказа в EpinBy
POLL_TIMEOUT = 20 * 60   # сколько ждать выполнения одного пакета

# Тексты покупателю
MSG_DONE = (
    "Ваш заказ готов!\n\n"
    "🎉 Спасибо за покупку!\n\n"
    "Рады, что выбрали нас💙\n"
    "Если всё прошло отлично — будем благодарны за ⭐ отзыв!\n\n"
    "🎁 За оставленный отзыв вы получите промокод на следующую покупку со скидкой "
    "от 3% до 10% — постоянным клиентам максимальная скидка🔥\n"
    "❗Для того чтобы его получить - оставьте отзыв и напишите менеджеру в чат \"ХОЧУ ПРОМО\"\n\n"
    "Спасибо за доверие! ❤️\n"
    "Покупай выгодно, возвращайся снова"
)
MSG_BAD_ID = (
    "Здравствуйте! Не удалось найти аккаунт с ID {player_id}. "
    "Пожалуйста, проверьте ID и напишите его в этот чат — менеджер выполнит заказ."
)
MSG_BAD_USERNAME = (
    "Здравствуйте! Не удалось найти Telegram-аккаунт {username}. "
    "Пожалуйста, проверьте username (вида @username) и напишите его в этот чат — менеджер выполнит заказ."
)
MSG_MANUAL = (
    "Здравствуйте! Ваш заказ принят. Автоматическая выдача задерживается, "
    "менеджер выполнит его в ближайшее время. Спасибо за ожидание!"
)

# Заполняется из main.py при запуске
_hooks = {"notify": None, "send_chat": None, "hide": None}
_lock = threading.Lock()
_config = None


# ---------------------------------------------------------------- состояние

def _load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state):
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        # храним последние 2000 заказов
        items = sorted(state.items(), key=lambda kv: kv[1].get("ts", 0))[-2000:]
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dict(items), f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print("Автовыдача: не удалось сохранить состояние:", e)


def _set_status(invoice_id, **fields):
    with _lock:
        state = _load_state()
        rec = state.get(invoice_id, {"ts": time.time()})
        rec.update(fields)
        state[invoice_id] = rec
        _save_state(state)


def _get_record(invoice_id):
    with _lock:
        return _load_state().get(invoice_id)


# ---------------------------------------------------------------- настройка

def load_config():
    global _config
    with open(PRODUCTS_FILE, "r", encoding="utf-8") as f:
        _config = json.load(f)
    return _config


def setup(notify, send_chat, hide):
    """Вызывается из main.py один раз при запуске."""
    _hooks.update(notify=notify, send_chat=send_chat, hide=hide)
    if not ENABLED:
        print("Автовыдача выключена (AUTODELIVERY не равно on).")
        return
    if not EPINBY_KEY and not g2bulk.KEY:
        print("Автовыдача: не задан ни EPINBY_API_KEY, ни G2BULK_API_KEY, выключаю.")
        notify("⚠️ Автовыдача не запущена: не задан ключ ни EpinBy, ни G2Bulk.")
        return
    load_config()
    mode = "🧪 ТЕСТОВЫЙ режим (ничего не покупаю)" if DRY_RUN else "🟢 БОЕВОЙ режим"
    products = len([k for k in _config.get("ggsel_products", {}) if not k.startswith("_")])
    text = f"🤖 Автовыдача включена. {mode}.\nТоваров на автомате: {products}."
    if EPINBY_KEY:
        try:
            text += f"\nБаланс EpinBy: ${epinby_get('/getMe').get('balance')}"
        except Exception as e:
            print("Автовыдача: EpinBy не ответил:", _hide(e))
    report = _mix_startup_report()
    if report:
        text += "\n\n" + report
    report = _g2_startup_report()
    if report:
        text += "\n\n" + report
    notify(text)
    _resume_unfinished()


def is_on():
    return ENABLED and bool(EPINBY_KEY or g2bulk.KEY) and _config is not None


def _hide(text):
    fn = _hooks.get("hide")
    text = str(text)
    for secret in (EPINBY_KEY, g2bulk.KEY):
        if secret:
            text = text.replace(secret, "***")
    return fn(text) if fn else text


def _notify(text):
    fn = _hooks.get("notify")
    if fn:
        fn(text)


# ---------------------------------------------------------------- EpinBy API

class EpinbyError(Exception):
    def __init__(self, code, message, http_status=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.http_status = http_status


def _epinby(method, path, body=None, idem_key=None):
    headers = {"X-API-KEY": EPINBY_KEY, "Accept": "application/json"}
    if idem_key:
        headers["X-Idempotency-Key"] = idem_key
    r = requests.request(method, EPINBY_API + path, json=body, headers=headers, timeout=30)
    try:
        data = r.json()
    except Exception:
        raise EpinbyError("BAD_RESPONSE", f"код {r.status_code}: {r.text[:200]}", r.status_code)
    if not data.get("success"):
        err = data.get("error") or {}
        raise EpinbyError(err.get("code", "UNKNOWN"), err.get("message", str(data)[:200]), r.status_code)
    return data.get("data")


def epinby_get(path):
    return _epinby("GET", path)


def epinby_post(path, body, idem_key=None):
    return _epinby("POST", path, body, idem_key)


def _balance_line():
    """Строка с актуальным балансом EpinBy (и предупреждение, если денег мало)."""
    try:
        bal = float(epinby_get("/getMe").get("balance"))
    except Exception as e:
        print("Баланс EpinBy не получен:", _hide(e)[:150])
        return "\n💰 Баланс EpinBy: не удалось получить"
    line = f"\n💰 Баланс EpinBy: ${bal:.2f}"
    if bal < LOW_BALANCE:
        line += f"\n⚠️ Баланс ниже ${LOW_BALANCE:.0f} — пора пополнить!"
    return line


# ---------------------------------------------------------------- разбор заказа

def _options(info):
    """Опции заказа GGSel -> список (название, значение)."""
    raw = info.get("options") or []
    result = []
    if isinstance(raw, dict):
        raw = [{"name": k, "value": v} for k, v in raw.items()]
    for opt in raw:
        if not isinstance(opt, dict):
            continue
        name = str(opt.get("name") or opt.get("label") or opt.get("title") or "")
        value = opt.get("value")
        if value is None:
            value = opt.get("user_data") or opt.get("variant") or opt.get("text") or ""
        if isinstance(value, dict):
            value = value.get("text") or value.get("name") or value.get("value") or ""
        result.append((name, str(value)))
    return result


def _find_option(options, keyword):
    kw = keyword.lower()
    for name, value in options:
        if kw in name.lower():
            return value
    return None


def parse_uc_amount(text):
    """'3850 UC (+4575.0 RUB)' -> 3850"""
    if not text:
        return None
    m = re.search(r"(\d[\d\s]*)\s*(?:UC|ЮС|юси|ЮСИ)", text, re.IGNORECASE)
    if not m:
        m = re.search(r"\d+", text)
        return int(m.group(0)) if m else None
    return int(re.sub(r"\s", "", m.group(1)))


def parse_player_id(text):
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    return digits or None


def split_uc(amount, packs):
    """
    Раскладывает amount UC на пакеты так, чтобы сумма совпала точно, а цена была минимальной.
    packs: {размер: цена}. Возвращает список размеров (от большего к меньшему) или None.
    """
    sizes = sorted(packs)
    step = 5
    if amount <= 0 or amount % step or any(s % step for s in sizes):
        return None
    n = amount // step
    INF = float("inf")
    best = [(0.0, 0)] + [(INF, 0)] * n   # (цена, кол-во пакетов)
    choice = [0] * (n + 1)
    for i in range(1, n + 1):
        for s in sizes:
            u = s // step
            if u <= i and best[i - u][0] < INF:
                cand = (best[i - u][0] + packs[s], best[i - u][1] + 1)
                if cand < best[i]:
                    best[i] = cand
                    choice[i] = s
    if best[n][0] == INF:
        return None
    result, i = [], n
    while i > 0:
        result.append(choice[i])
        i -= choice[i] // step
    return sorted(result, reverse=True)


# ---------------------------------------------------------------- выдача

def start(sale, info, invoice_id, product_id):
    """Вызывается из main.py для каждого нового заказа. Возвращает True, если заказ взят в работу."""
    if not is_on():
        return False
    product = _config.get("ggsel_products", {}).get(str(product_id))
    if not product:
        return False
    needs_g2 = product.get("kind") == "g2bulk"
    if (needs_g2 and not g2bulk.KEY) or (not needs_g2 and not EPINBY_KEY):
        _notify(f"⚠️ Заказ {invoice_id}: нет ключа поставщика для «{product.get('title', '')}», "
                f"выдайте вручную.")
        return False
    if _get_record(invoice_id):
        return True  # уже обрабатывали
    _set_status(invoice_id, status="new", product_id=str(product_id))
    info = dict(info or {})
    prod = (sale or {}).get("product") or {}
    info["_product_name"] = str(prod.get("name") or (sale or {}).get("name") or info.get("name") or "")
    t = threading.Thread(target=_deliver_safe, args=(invoice_id, product, info), daemon=True)
    t.start()
    return True


def _resume_unfinished():
    """После перезапуска доделывает заказы, которые были в работе (повтор безопасен)."""
    with _lock:
        state = _load_state()
    for inv, rec in state.items():
        if rec.get("status") in ("new", "in_progress") and rec.get("plan"):
            print("Автовыдача: продолжаю заказ", inv)
            product = _config.get("ggsel_products", {}).get(rec.get("product_id", ""))
            if product:
                threading.Thread(target=_deliver_safe, args=(inv, product, None, rec),
                                 daemon=True).start()


def _deliver_safe(invoice_id, product, info, resume=None):
    try:
        if product.get("kind") == "tg_stars":
            _deliver_stars(invoice_id, product, info, resume)
        elif product.get("kind") == "g2bulk":
            _deliver_g2bulk(invoice_id, product, info, resume)
        elif product.get("kind") == "epinby_mix":
            _deliver_mix(invoice_id, product, info, resume)
        else:
            _deliver(invoice_id, product, info, resume)
    except Exception as e:
        _set_status(invoice_id, status="error", error=_hide(e)[:300])
        _notify(f"❗ Автовыдача, заказ {invoice_id}: внутренняя ошибка, выдайте вручную.\n"
                f"{_hide(e)[:300]}")


def _deliver(invoice_id, product, info, resume=None):
    title = product.get("title", "")
    if resume:
        player_id = resume["player_id"]
        plan = resume["plan"]
        amount = resume.get("amount")
    else:
        options = _options(info or {})
        amount = parse_uc_amount(_find_option(options, product["amount_option"]))
        player_id = parse_player_id(_find_option(options, product["id_option"]))
        if not amount or not player_id:
            _set_status(invoice_id, status="manual", error="не разобраны опции")
            _notify(f"❗ Автовыдача, заказ {invoice_id} ({title}): не смог прочитать опции заказа, "
                    f"выдайте вручную.\nОпции: {options or info.get('options')}")
            return
        packs = {int(k): v["price"] for k, v in _config["uc_packs"].items() if not k.startswith("_")}
        plan = split_uc(amount, packs)
        if not plan:
            _set_status(invoice_id, status="manual", error=f"нельзя собрать {amount} UC")
            _notify(f"❗ Автовыдача, заказ {invoice_id}: {amount} UC нельзя собрать из пакетов, "
                    f"выдайте вручную. ID: {player_id}")
            return
        _set_status(invoice_id, status="new", player_id=player_id, amount=amount, plan=plan)

    plan_text = " + ".join(str(p) for p in plan)
    est = sum(_config["uc_packs"][str(p)]["price"] for p in plan)

    # проверка игрока
    first_pid = _config["uc_packs"][str(plan[0])]["ids"][0]
    nickname = None
    try:
        v = epinby_post("/validate-player", {"product_id": first_pid, "player_id": player_id})
        nickname = v.get("nickname") or v.get("player_name")
    except EpinbyError as e:
        if e.code == "VALIDATION_ERROR":
            _set_status(invoice_id, status="manual", error="неверный ID")
            if not DRY_RUN:
                _send_error_chat(invoice_id, MSG_BAD_ID.format(player_id=player_id))
            _notify(f"⚠️ Заказ {invoice_id}: EpinBy не нашёл игрока с ID {player_id}. "
                    f"Покупателю не писал, выдайте вручную.\n"
                    f"Ответ: {e.message}")
            return
        print(f"Заказ {invoice_id}: проверка игрока не удалась ({e.code}), продолжаю")

    who = f"{player_id}" + (f" ({nickname})" if nickname else "")
    if DRY_RUN:
        _set_status(invoice_id, status="dry_run")
        _notify(f"🧪 Тест. Заказ {invoice_id}: {amount} UC → игрок {who}.\n"
                f"Купил бы: {plan_text} (≈ ${est:.2f}).\n"
                f"Ничего не куплено (DRY_RUN=1)." + _balance_line())
        return

    _set_status(invoice_id, status="in_progress")
    done = []
    for idx, size in enumerate(plan):
        ok, used_id, err = _buy_pack(invoice_id, idx, size, player_id)
        if ok:
            done.append(size)
            continue
        _set_status(invoice_id, status="manual", error=err, done=done)
        rest = plan[idx:]
        _send_error_chat(invoice_id, MSG_MANUAL)
        _notify(f"❗ Заказ {invoice_id}: выдача остановилась.\n"
                f"Игрок: {who}\nВыдано: {' + '.join(map(str, done)) or 'ничего'}\n"
                f"Не выдано: {' + '.join(map(str, rest))} UC\nПричина: {err}\n"
                f"Доделайте вручную." + _balance_line())
        return

    _set_status(invoice_id, status="done", done=done)
    _send_chat(invoice_id, MSG_DONE)
    _notify(f"✅ Заказ {invoice_id} выдан автоматически: {amount} UC ({plan_text}) → {who}."
            + _balance_line())


def _buy_pack(invoice_id, idx, size, player_id):
    """Покупает один пакет UC, перебирая основной и запасные товары. -> (успех, product_id, ошибка)"""
    return _buy_ids(invoice_id, idx, f"{size} UC", _config["uc_packs"][str(size)]["ids"], player_id)


def _buy_ids(invoice_id, idx, label, ids, player_id):
    """Покупает одну часть заказа в EpinBy, перебирая основной и запасные товары."""
    size = label
    last_err = "нет товаров"
    for pid in ids:
        key = f"ggsel-{invoice_id}-{idx}-{pid}"
        body = {"product_id": pid, "qty": 1, "player_id": player_id}
        order = None
        for attempt in range(3):
            try:
                order = epinby_post("/order", body, idem_key=key)
                break
            except EpinbyError as e:
                if e.code == "INSUFFICIENT_FUNDS":
                    _notify("💸 На балансе EpinBy не хватает денег! Пополните баланс.")
                    return False, pid, "мало денег на балансе EpinBy"
                last_err = f"{size} (товар {pid}): {e.code} {e.message}"
                break
            except Exception as e:
                # сеть: заказ мог создаться, поэтому повторяем с ТЕМ ЖЕ ключом - дубля не будет
                last_err = f"{size} (товар {pid}): сеть: {_hide(e)[:150]}"
                time.sleep(5)
        if order is None:
            if "сеть" in last_err:
                # не знаем, создан ли заказ - запасной брать нельзя, иначе возможна двойная выдача
                return False, pid, last_err
            print(f"Заказ {invoice_id}: {last_err}, пробую запасной")
            continue

        status = _wait_order(order.get("order_id"), order.get("status"))
        if status == "COMPLETED":
            print(f"Заказ {invoice_id}: {size} выдано (товар {pid}, EpinBy #{order.get('order_id')})")
            return True, pid, None
        last_err = f"{size} (товар {pid}, EpinBy #{order.get('order_id')}): статус {status}"
        if status == "TIMEOUT":
            # заказ ещё может выполниться - запасной не берём, чтобы не выдать дважды
            return False, pid, last_err + " (долго выполняется, проверьте в EpinBy)"
        print(f"Заказ {invoice_id}: {last_err}, пробую запасной")
    return False, None, last_err


def _wait_order(order_id, status):
    deadline = time.time() + POLL_TIMEOUT
    status = (status or "").upper()
    while status not in ("COMPLETED", "CANCELED", "FAILED"):
        if time.time() > deadline:
            return "TIMEOUT"
        time.sleep(POLL_EVERY)
        try:
            status = (epinby_get(f"/order/{order_id}").get("status") or "").upper()
        except Exception as e:
            print(f"EpinBy #{order_id}: статус не получен:", _hide(e)[:150])
    return status


def _send_error_chat(invoice_id, text):
    """Сообщение покупателю об ошибке. По умолчанию выключено (BUYER_ERROR_MESSAGES=on - включить)."""
    if BUYER_ERROR_MESSAGES:
        _send_chat(invoice_id, text)


def _send_chat(invoice_id, text):
    fn = _hooks.get("send_chat")
    if not fn:
        return
    try:
        fn(invoice_id, text)
    except Exception as e:
        _notify(f"⚠️ Заказ {invoice_id}: не смог написать покупателю в чат GGSel: {_hide(e)[:200]}")


# ---------------------------------------------------------------- Telegram Stars

def parse_username(text):
    """'@user', 'user', 't.me/user', 'https://t.me/user' -> '@user'. None, если не похоже на username."""
    if not text:
        return None
    t = text.strip()
    m = re.search(r"(?:t\.me/|telegram\.me/)([A-Za-z0-9_]{4,32})", t)
    if m:
        name = m.group(1)
    else:
        m = re.search(r"@?([A-Za-z][A-Za-z0-9_]{3,31})", t)
        if not m:
            return None
        name = m.group(1)
    return "@" + name


def parse_stars_amount(text):
    """'500 звёзд (+700 RUB)' -> 500"""
    if not text:
        return None
    m = re.search(r"\d[\d\s]*", text)
    return int(re.sub(r"\s", "", m.group(0))) if m else None


def _deliver_stars(invoice_id, product, info, resume=None):
    title = product.get("title", "Telegram Stars")
    if resume:
        username = resume["player_id"]
        amount = resume["amount"]
    else:
        options = _options(info or {})
        username = parse_username(_find_option(options, product["username_option"]))
        src = product.get("amount_option")
        if src == "quantity":
            amount = parse_stars_amount(str((info or {}).get("cnt_goods") or ""))
        else:
            amount = parse_stars_amount(_find_option(options, src))
        amount = (amount or 0) * int(product.get("multiplier", 1)) or None
        if not amount or not username:
            _set_status(invoice_id, status="manual", error="не разобраны опции")
            _notify(f"❗ Автовыдача, заказ {invoice_id} ({title}): не смог прочитать опции заказа, "
                    f"выдайте вручную.\nОпции: {options or (info or {}).get('options')}\n"
                    f"Количество: {(info or {}).get('cnt_goods')}")
            return
        _set_status(invoice_id, status="new", player_id=username, amount=amount, plan=[amount])

    # проверка получателя
    try:
        epinby_post("/telegram-stars/recipient", {"telegram_username": username})
    except EpinbyError as e:
        if e.code in ("VALIDATION_ERROR", "RECIPIENT_NOT_FOUND", "NOT_FOUND"):
            _set_status(invoice_id, status="manual", error="неверный username")
            if not DRY_RUN:
                _send_error_chat(invoice_id, MSG_BAD_USERNAME.format(username=username))
            _notify(f"⚠️ Заказ {invoice_id}: EpinBy не нашёл Telegram {username}. "
                    f"Покупателю не писал, выдайте вручную.\n"
                    f"Ответ: {e.message}")
            return
        print(f"Заказ {invoice_id}: проверка получателя не удалась ({e.code}), продолжаю")

    try:
        quote = epinby_post("/telegram-stars/quote", {"stars_amount": amount})
        price = quote.get("reseller_price_usd")
    except EpinbyError as e:
        _set_status(invoice_id, status="manual", error=f"{e.code} {e.message}")
        _notify(f"❗ Заказ {invoice_id}: {amount} ⭐ для {username} — EpinBy не дал цену: "
                f"{e.code} {e.message}. Выдайте вручную." + _balance_line())
        return

    if DRY_RUN:
        _set_status(invoice_id, status="dry_run")
        _notify(f"🧪 Тест. Заказ {invoice_id}: {amount} ⭐ → {username}.\n"
                f"Стоимость: ${price}. Ничего не куплено (DRY_RUN=1)." + _balance_line())
        return

    _set_status(invoice_id, status="in_progress")
    body = {"telegram_username": username, "stars_amount": amount}
    order, err = None, None
    for attempt in range(3):
        try:
            order = epinby_post("/telegram-stars/order", body, idem_key=f"ggsel-{invoice_id}-stars")
            break
        except EpinbyError as e:
            if e.code == "INSUFFICIENT_FUNDS":
                _notify("💸 На балансе EpinBy не хватает денег! Пополните баланс.")
            err = f"{e.code} {e.message}"
            break
        except Exception as e:
            err = f"сеть: {_hide(e)[:150]}"
            time.sleep(5)

    status = None
    if order:
        status = _wait_stars(order.get("order_id"), order.get("status"))
        if status == "COMPLETED":
            _set_status(invoice_id, status="done")
            _send_chat(invoice_id, MSG_DONE)
            _notify(f"✅ Заказ {invoice_id} выдан автоматически: {amount} ⭐ → {username} (${price})."
                    + _balance_line())
            return
        err = f"EpinBy #{order.get('order_id')}: статус {status}"

    _set_status(invoice_id, status="manual", error=err)
    _send_error_chat(invoice_id, MSG_MANUAL)
    _notify(f"❗ Заказ {invoice_id}: {amount} ⭐ для {username} не выдано.\nПричина: {err}\n"
            f"Проверьте в EpinBy и доделайте вручную." + _balance_line())


def _wait_stars(order_id, status):
    deadline = time.time() + POLL_TIMEOUT
    status = (status or "").upper()
    while status not in ("COMPLETED", "CANCELED", "FAILED"):
        if time.time() > deadline:
            return "TIMEOUT"
        time.sleep(POLL_EVERY)
        try:
            status = (epinby_get(f"/telegram-stars/order/{order_id}").get("status") or "").upper()
        except Exception as e:
            print(f"EpinBy Stars #{order_id}: статус не получен:", _hide(e)[:150])
    return status


# ---------------------------------------------------------------- G2Bulk

MSG_BAD_ID_SERVER = (
    "Здравствуйте! Не удалось найти аккаунт с ID {player_id} (сервер {server_id}). "
    "Пожалуйста, проверьте ID и сервер и напишите их в этот чат — менеджер выполнит заказ."
)


def _clean(text):
    """'🔥ПРЕМИУМ 30 ДНЕЙ🔥 (+0.0 RUB)' -> 'премиум 30 дней'. Без смайлов, цены и регистра."""
    text = str(text or "").split("(")[0]
    text = re.sub(r"[^\w\s+]", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def _parse_number(text):
    """'86 алмазов (+150 RUB)' -> 86, '50+1 ЗОЛОТА' -> 51. Число берётся до скобки с ценой."""
    if not text:
        return None
    # число может быть с пробелом-разделителем тысяч: «10 000», и с бонусом: «50+1»
    num = r"\d{1,3}(?:[ \u00a0]\d{3})+(?!\d)|\d+"
    m = re.search(rf"(?:{num})(?:\s*\+\s*(?:{num}))*", str(text).split("(")[0])
    if not m:
        return None
    return sum(int(re.sub(r"\D", "", x)) for x in re.findall(num, m.group(0)))


def _parse_g2_id(product, text):
    if product.get("id_format") == "raw":
        # ID с буквами и дефисами, например Oxide: 0F-FJW-3N
        value = re.sub(r"\s", "", str(text or "")).strip(".,;:")
        return value.upper() or None
    return parse_player_id(text)


NO_WORDS = ("нет", "не нужен", "не нужно", "без", "no", "none")


def _g2_packs(product):
    """{количество: (название в каталоге, цена)} для товара."""
    return g2bulk.denominations(product["game"], product.get("unit", ""))


def _catalogue_index(game):
    """{очищенное название: название в каталоге}."""
    return {_clean(i.get("name")): str(i.get("name")) for i in g2bulk.catalogue(game)}


def _flat_names(parts):
    for part in parts:
        for n in _alts(part):
            yield n


def _g2_balance_line():
    try:
        bal = g2bulk.balance()
    except Exception as e:
        print("Баланс G2Bulk не получен:", _hide(e)[:150])
        return "\n💰 Баланс G2Bulk: не удалось получить"
    line = f"\n💰 Баланс G2Bulk: ${bal:.2f}"
    if bal < G2_LOW_BALANCE:
        line += f"\n⚠️ Баланс G2Bulk ниже ${G2_LOW_BALANCE:.0f} — пора пополнить!"
    return line


def _g2_startup_report():
    """Проверяет товары G2Bulk при запуске: есть ли игра, нужные поля и пакеты."""
    items = [(k, v) for k, v in _config.get("ggsel_products", {}).items()
             if not k.startswith("_") and v.get("kind") == "g2bulk"]
    if not items:
        return ""
    if not g2bulk.KEY:
        return "⚠️ G2Bulk: не задан G2BULK_API_KEY, товары G2Bulk выдаются вручную."
    mode = "🧪 тест" if G2_DRY_RUN else "🟢 боевой"
    lines = [f"G2Bulk ({mode}):"]
    try:
        lines.append(f"Баланс: ${g2bulk.balance():.2f}")
    except Exception as e:
        lines.append(f"⚠️ Ключ не принят или G2Bulk недоступен: {_hide(e)[:150]}")
    for gid, p in items:
        title = p.get("title", gid)
        try:
            names = set(_catalogue_index(p["game"]).values())
        except Exception as e:
            lines.append(f"❗ {title}: каталог «{p.get('game')}» не получен: {_hide(e)[:120]}")
            continue
        try:
            need = g2bulk.fields(p["game"])
            if "serverid" in need and not p.get("server_option"):
                lines.append(f"❗ {title}: G2Bulk просит сервер, а в товаре нет поля сервера")
        except Exception:
            pass
        if p.get("mode") == "catalogue":
            text = f"• {title}: товар по названию из каталога ({len(names)} позиций)"
            if p.get("variants"):
                missing = sorted({n for v in p["variants"].values()
                                  for n in _flat_names([v] if isinstance(v, str) else v)
                                  if n not in names})
                text += f"; ❗ нет в каталоге: {', '.join(missing)}" if missing else ""
            lines.append(text)
        else:
            packs = _g2_packs(p)
            if not packs:
                lines.append(f"❗ {title}: в каталоге нет пакетов вида «число {p.get('unit', '')}». "
                             f"Выдача будет ручной.")
                continue
            lines.append(f"• {title}: пакеты {', '.join(str(q) for q in sorted(packs))}")
            alt = {**(p.get("prefer") or {}), **(p.get("fallback") or {})}
            bad = [v for v in alt.values() if v not in names]
            if alt:
                lines.append(f"  запасные/бонусные пакеты: {len(alt) - len(bad)} из {len(alt)}"
                             + (f"; ❗ нет в каталоге: {', '.join(bad)}" if bad else ""))
            for amount in p.get("check_amounts") or []:
                if not g2bulk.split(int(amount), {q: v[1] for q, v in packs.items()}):
                    lines.append(f"  ❗ {amount} нельзя собрать из этих пакетов")
        if p.get("extra_option"):
            lines.append(f"  + доп. товар из поля «{p['extra_option']}»")
    return "\n".join(lines)


class _Manual(Exception):
    """Заказ нельзя выдать автоматически: причина уходит в группу."""


def _g2_plan_amount(product, chosen, info):
    """Режим «количество»: монеты/золото раскладываются на пакеты. -> (план, описание)"""
    if product.get("item_option") == "quantity":
        amount = _parse_number(str((info or {}).get("cnt_goods") or ""))
    else:
        amount = _parse_number(chosen)
    if not amount:
        raise _Manual(f"не понял количество в «{chosen}»")
    amount *= int(product.get("multiplier", 1))
    try:
        packs = _g2_packs(product)
    except Exception as e:
        raise _Manual(f"каталог G2Bulk не получен: {_hide(e)[:150]}")
    sizes = g2bulk.split(amount, {q: v[1] for q, v in packs.items()})
    if not sizes:
        raise _Manual(f"{amount} нельзя собрать из пакетов G2Bulk")
    prefer = product.get("prefer") or {}       # сначала другой пакет, потом этот
    fallback = product.get("fallback") or {}   # сначала этот, потом запасной
    plan = []
    for q in sizes:
        name = packs[q][0]
        if name in prefer:
            plan.append([prefer[name], name])
        elif name in fallback:
            plan.append([name, fallback[name]])
        else:
            plan.append(name)
    return plan, f"{amount} {product.get('unit_title') or product.get('unit', '')}".strip()


def _g2_plan_named(product, chosen):
    """Режим «по названию»: выбранный вариант = товар каталога. -> (план, описание)"""
    text = _clean(chosen)
    variants = product.get("variants") or {}
    best = None
    for key, parts in variants.items():
        k = _clean(key)
        if k and k in text and (best is None or len(k) > len(best[0])):
            best = (k, [parts] if isinstance(parts, str) else list(parts))
    if best:
        return best[1], text
    try:
        index = _catalogue_index(product["game"])
    except Exception as e:
        raise _Manual(f"каталог G2Bulk не получен: {_hide(e)[:150]}")
    if text in index:
        return [index[text]], text
    raise _Manual(f"вариант «{chosen.strip()}» не найден в каталоге G2Bulk")


def _g2_extra(product, options):
    """Доп. товар из отдельного поля (например, «Нужен пропуск?»). -> список частей плана"""
    if not product.get("extra_option"):
        return []
    chosen = _find_option(options, product["extra_option"])
    text = _clean(chosen)
    if not text or any(text == w or text.startswith(w + " ") for w in NO_WORDS):
        return []
    try:
        index = _catalogue_index(product["game"])
    except Exception as e:
        raise _Manual(f"каталог G2Bulk не получен: {_hide(e)[:150]}")
    if text in index:
        return [index[text]]
    raise _Manual(f"доп. товар «{chosen.strip()}» не найден в каталоге G2Bulk")


def _deliver_g2bulk(invoice_id, product, info, resume=None):
    title = product.get("title", "")
    if resume:
        _g2_execute(invoice_id, product, resume["player_id"], resume.get("server_id"),
                    resume.get("amount"), resume["plan"])
        return
    options = _options(info or {})
    item_key = product.get("item_option") or product.get("amount_option")
    chosen = _find_option(options, item_key) if item_key != "quantity" else ""
    player_id = _parse_g2_id(product, _find_option(options, product["id_option"]))
    server_id = None
    if product.get("server_option"):
        server_id = parse_player_id(_find_option(options, product["server_option"]))
    try:
        if not player_id or (product.get("server_option") and not server_id):
            raise _Manual("не нашёл ID игрока" + (" или сервер" if product.get("server_option") else ""))
        if item_key != "quantity" and not chosen:
            raise _Manual("не нашёл выбранный товар")
        if product.get("mode") == "catalogue":
            plan, what = _g2_plan_named(product, chosen)
        else:
            plan, what = _g2_plan_amount(product, chosen, info)
        extra = _g2_extra(product, options)
        if extra:
            plan += extra
            what += " + " + " + ".join(extra)
    except _Manual as e:
        _set_status(invoice_id, status="manual", error=str(e))
        _notify(f"❗ Автовыдача, заказ {invoice_id} ({title}): {e}. Выдайте вручную.\n"
                f"Опции: {options or (info or {}).get('options')}")
        return
    _set_status(invoice_id, status="new", player_id=player_id, server_id=server_id,
                amount=what, plan=plan, supplier="g2bulk")
    _g2_execute(invoice_id, product, player_id, server_id, what, plan)


def _g2_execute(invoice_id, product, player_id, server_id, amount, plan):
    title = product.get("title", "")
    game = product["game"]
    plan_text = " + ".join(_alts(n)[0] for n in plan)
    who = f"{player_id}" + (f" / сервер {server_id}" if server_id else "")

    valid, nickname = g2bulk.check_player(game, player_id, server_id)
    if valid is False:
        _set_status(invoice_id, status="manual", error="неверный ID")
        if not G2_DRY_RUN:
            msg = (MSG_BAD_ID_SERVER.format(player_id=player_id, server_id=server_id) if server_id
                   else MSG_BAD_ID.format(player_id=player_id))
            _send_error_chat(invoice_id, msg)
        _notify(f"⚠️ Заказ {invoice_id} ({title}): G2Bulk не нашёл игрока {who}. "
                f"Покупателю не писал, выдайте вручную.")
        return
    if nickname:
        who += f" ({nickname})"

    if G2_DRY_RUN:
        try:
            prices = {str(i.get("name")): float(i.get("amount") or 0) for i in g2bulk.catalogue(game)}
            est = f" (≈ ${sum(prices.get(_alts(n)[0], 0) for n in plan):.2f})"
        except Exception:
            est = ""
        _set_status(invoice_id, status="dry_run")
        _notify(f"🧪 Тест G2Bulk. Заказ {invoice_id}: {amount} → {who}.\n"
                f"Купил бы: {plan_text}{est}.\nНичего не куплено (G2BULK_DRY_RUN=1)."
                + _g2_balance_line())
        return

    _set_status(invoice_id, status="in_progress")
    done = []
    for idx, names in enumerate(plan):
        ok, got, err = _g2_buy_part(invoice_id, idx, game, _alts(names), player_id, server_id)
        if ok:
            done.append(got)
            continue
        note = (product.get("no_fallback") or {}).get(_alts(names)[0])
        if note:
            err = f"{err}\n{note}"
        _set_status(invoice_id, status="manual", error=err, done=done)
        _send_error_chat(invoice_id, MSG_MANUAL)
        _notify(f"❗ Заказ {invoice_id} ({title}): выдача через G2Bulk остановилась.\n"
                f"Игрок: {who}\nВыдано: {' + '.join(done) or 'ничего'}\n"
                f"Не выдано: {' + '.join(_alts(n)[-1] for n in plan[idx:])}\nПричина: {err}\n"
                f"Доделайте вручную." + _g2_balance_line())
        return

    _set_status(invoice_id, status="done", done=done)
    _send_chat(invoice_id, MSG_DONE)
    _notify(f"✅ Заказ {invoice_id} выдан автоматически (G2Bulk): {amount} ({' + '.join(done)}) → {who}."
            + _g2_balance_line())


def _alts(entry):
    """Часть плана: одно название или список [предпочтительный, запасной, ...]."""
    return [entry] if isinstance(entry, str) else list(entry)


def _g2_buy_part(invoice_id, idx, game, names, player_id, server_id):
    """
    Покупает одну часть заказа, по очереди пробуя варианты (например, сначала WEB BONUS,
    потом обычный пакет). К следующему варианту переходит, только если предыдущий точно
    не выдан: G2Bulk отказал сразу или заказ FAILED (деньги вернулись).
    -> (успех, что выдано, ошибка)
    """
    err = "нет вариантов"
    for alt, name in enumerate(names):
        ok, err, can_next = _g2_buy_one(invoice_id, f"{idx}-{alt}", game, name, player_id, server_id)
        if ok:
            return True, name, None
        if not can_next or alt == len(names) - 1:
            return False, None, err
        print(f"Заказ {invoice_id}: {err} — пробую запасной вариант {names[alt + 1]}")
        _notify(f"ℹ️ Заказ {invoice_id}: {name} не прошёл, беру {names[alt + 1]}.")
    return False, None, err


def _g2_buy_one(invoice_id, slot, game, name, player_id, server_id):
    """Одна покупка. Повтор после перезапуска не покупает второй раз. -> (успех, ошибка, можно_запасной)"""
    remark = f"ggsel-{invoice_id}-{slot}"
    parts = (_get_record(invoice_id) or {}).get("g2_parts") or {}
    idx = slot
    part = parts.get(str(idx)) or {}
    order_id = part.get("order_id")

    if not order_id and part.get("attempted"):
        # бот мог упасть сразу после покупки: ищем заказ в истории G2Bulk
        try:
            found = g2bulk.find_order_by_remark(game, remark)
        except Exception as e:
            return False, f"{name}: не смог проверить историю G2Bulk ({_hide(e)[:120]}), проверьте вручную", False
        if found:
            order_id = found.get("order_id")
        elif time.time() - part.get("ts", 0) > 25 * 60:
            return False, f"{name}: покупка прервалась, проверьте в истории G2Bulk (подпись {remark})", False

    if not order_id:
        parts[str(idx)] = {"attempted": True, "ts": part.get("ts") or time.time()}
        _set_status(invoice_id, g2_parts=parts)
        key = g2bulk.idem_key("ggsel", invoice_id, slot)
        order, err = None, None
        for attempt in range(3):
            try:
                order = g2bulk.place_order(game, name, player_id, server_id, remark=remark, key=key)
                break
            except g2bulk.G2Error as e:
                if e.no_money:
                    _notify("💸 На балансе G2Bulk не хватает денег! Пополните баланс.")
                    return False, "мало денег на балансе G2Bulk", False
                err = f"{name}: {_hide(e.message)[:200]}"
                if not e.network:
                    # G2Bulk отказал - заказ точно не создан
                    parts[str(idx)] = {}
                    _set_status(invoice_id, g2_parts=parts)
                    return False, err, True
                time.sleep(5)   # повтор с тем же ключом - дубля не будет
        if not order:
            return False, (err or f"{name}: нет ответа") + " (проверьте в истории G2Bulk)", False
        order_id = order.get("order_id")
        parts[str(idx)] = {"attempted": True, "ts": parts[str(idx)]["ts"], "order_id": order_id}
        _set_status(invoice_id, g2_parts=parts)

    status = _g2_wait(order_id)
    if status == "COMPLETED":
        print(f"Заказ {invoice_id}: {name} выдано (G2Bulk #{order_id})")
        return True, None, False
    if status == "TIMEOUT":
        return False, f"{name} (G2Bulk #{order_id}): долго выполняется, проверьте в G2Bulk", False
    return False, f"{name} (G2Bulk #{order_id}): статус {status}, деньги вернулись на баланс G2Bulk", True


def _g2_wait(order_id):
    deadline = time.time() + POLL_TIMEOUT
    while True:
        try:
            status = (g2bulk.get_order(order_id).get("status") or "").upper()
        except Exception as e:
            status = ""
            print(f"G2Bulk #{order_id}: статус не получен:", _hide(e)[:150])
        if status in ("COMPLETED", "FAILED"):
            return status
        if time.time() > deadline:
            return "TIMEOUT"
        time.sleep(POLL_EVERY)


# ---------------------------------------------------------------- EpinBy: UC + наборы по названию

_ep_catalogue = {"ts": 0, "items": []}
EP_CATALOGUE_TTL = 10 * 60
_PRICE_TAIL = re.compile(r"\(\s*[+\-−]?\s*\d[\d\s.,]*\s*(?:rub|руб|₽|usd|\$|eur|€|uah|kzt)?\.?\s*\)\s*$", re.I)
_UC_RE = re.compile(r"^(\d[\d\s]*)\s*(?:uc|юс|юси)$", re.I)


def _ep_clean(text):
    """'⚡Prime (1 Month)⚡ (+10.0 RUB)' -> 'prime (1 month)'. Скобки внутри названия остаются."""
    t = _PRICE_TAIL.sub("", str(text or "").strip())
    t = re.sub(r"[^\w\s()+\-.]", " ", t)
    return re.sub(r"\s+", " ", t).strip().lower()


def _ep_topups(fresh=False):
    """Все товары-пополнения EpinBy (с кэшем на 10 минут)."""
    if not fresh and _ep_catalogue["items"] and time.time() - _ep_catalogue["ts"] < EP_CATALOGUE_TTL:
        return _ep_catalogue["items"]
    items, page = [], 1
    while page <= 50:
        headers = {"X-API-KEY": EPINBY_KEY, "Accept": "application/json"}
        r = requests.get(EPINBY_API + "/products", params={"type": "topup", "per_page": 100, "page": page},
                         headers=headers, timeout=30)
        data = r.json()
        if not data.get("success"):
            raise EpinbyError((data.get("error") or {}).get("code", "UNKNOWN"),
                              (data.get("error") or {}).get("message", "каталог не получен"), r.status_code)
        items += data.get("data") or []
        meta = data.get("meta") or {}
        if page >= int(meta.get("last_page") or 1):
            break
        page += 1
    _ep_catalogue.update(ts=time.time(), items=items)
    return items


def _ep_category(product):
    """Категория EpinBy: category_id, category_name или та же, что у основного пакета UC (PUBG)."""
    if product.get("category_id"):
        return int(product["category_id"])
    if product.get("category_name"):
        want = str(product["category_name"]).strip().lower()
        for it in _ep_topups():
            if str(it.get("category") or "").strip().lower() == want:
                return int(it.get("category_id") or 0)
        return 0
    ref = int(product.get("category_from_product") or 0)
    if not ref:
        packs = _config.get("uc_packs", {})
        for k, v in packs.items():
            if not k.startswith("_") and v.get("ids"):
                ref = int(v["ids"][0])
                break
    for it in _ep_topups():
        if int(it.get("id") or 0) == ref:
            return int(it.get("category_id") or 0)
    return 0


def _ep_find(product, name):
    """Название варианта -> товар EpinBy. Возвращает (товар, ошибка)."""
    want = _ep_clean((product.get("aliases") or {}).get(_ep_clean(name), name))
    for k, v in (product.get("aliases") or {}).items():
        if _ep_clean(k) == _ep_clean(name):
            want = _ep_clean(v)
    found = [it for it in _ep_topups() if _ep_clean(it.get("name")) == want]
    if product.get("category_name") or product.get("category_id"):
        # категория задана явно — только она (у разных игр бывают одинаковые названия, например «100»)
        cat = _ep_category(product)
        found = [it for it in found if cat and int(it.get("category_id") or 0) == cat]
    if not found:
        return None, f"«{_PRICE_TAIL.sub('', str(name)).strip()}» нет в каталоге EpinBy"
    cat = _ep_category(product)
    same = [it for it in found if cat and int(it.get("category_id") or 0) == cat]
    if len(same) == 1:
        return same[0], None
    if len(found) == 1:
        return found[0], None
    cats = ", ".join(sorted({str(it.get("category")) for it in found}))
    return None, f"«{name}» есть в нескольких категориях EpinBy ({cats}), укажите category_id"


def _mix_dry(product):
    env = product.get("dry_run_env")
    if not env:
        return MIX_DRY_RUN
    default = str(product.get("dry_run_default", "1"))
    return DRY_RUN or os.getenv(env, default).strip().lower() not in ("0", "off", "false", "no")


def _ep_amount_packs(product):
    """{сколько получает покупатель: товар EpinBy} для пакетов валюты в категории товара."""
    result = {}
    for amount, name in (product.get("amount_packs") or {}).items():
        item, err = _ep_find(product, name)
        if item:
            result[int(amount)] = item
    return result


def _mix_startup_report():
    items = [(k, v) for k, v in _config.get("ggsel_products", {}).items()
             if not k.startswith("_") and v.get("kind") == "epinby_mix"]
    if not items or not EPINBY_KEY:
        return ""
    lines = ["EpinBy наборы:"]
    for gid, p in items:
        title = p.get("title", gid) + (" (🧪 тест)" if _mix_dry(p) else " (🟢 боевой)")
        try:
            cat = _ep_category(p)
            n = len([it for it in _ep_topups() if int(it.get("category_id") or 0) == cat])
        except Exception as e:
            lines.append(f"❗ {title}: каталог EpinBy не получен: {_hide(e)[:150]}")
            continue
        lines.append(f"• {title}: в категории EpinBy {n} товаров")
        if p.get("category_name") and not cat:
            lines.append(f"  ❗ категория «{p['category_name']}» не найдена в EpinBy")
            continue
        if p.get("amount_packs"):
            have = _ep_amount_packs(p)
            lost = [str(a) for a in p["amount_packs"] if int(a) not in have]
            lines.append(f"  пакеты: {', '.join(str(a) for a in sorted(have)) or 'нет'}"
                         + (f"; ❗ не найдены: {', '.join(lost)}" if lost else ""))
        missing = [nm for nm in p.get("check_names") or [] if _ep_find(p, nm)[0] is None]
        if missing:
            lines.append(f"  ❗ не найдены: {', '.join(missing)}")
        elif p.get("check_names"):
            lines.append(f"  проверено названий: {len(p['check_names'])}, все найдены")
    return "\n".join(lines)


def _mix_plan(product, options, id_name):
    """Опции заказа -> список частей [{"label", "ids", "price"}]. Каждая непустая опция, кроме ID, — товар."""
    ignore = [w.lower() for w in product.get("ignore_options") or []]
    plan = []
    for name, value in options:
        if name == id_name or any(w in name.lower() for w in ignore):
            continue
        text = _ep_clean(value)
        if not text or any(text == w or text.startswith(w + " ") for w in NO_WORDS):
            continue
        item, err = _ep_find(product, value)
        if item:
            plan.append({"label": str(item.get("name")), "ids": [int(item["id"])],
                         "price": float(item.get("price") or 0)})
            continue
        if product.get("amount_packs"):
            amount = _parse_number(value)
            if amount:
                packs = _ep_amount_packs(product)
                base = {int(n): int(a) for a, n in (product.get("amount_packs") or {}).items()
                        if str(n).isdigit()}
                if amount not in packs and amount in base:
                    amount = base[amount]   # «500 облигаций» = пакет 500 (+10 бонус)
                sizes = g2bulk.split(amount, {q: float(it.get("price") or 0) for q, it in packs.items()})
                if not sizes:
                    raise _Manual(f"{amount} нельзя собрать из пакетов EpinBy ({', '.join(map(str, sorted(packs)))})")
                unit = product.get("unit_title", "")
                for size in sizes:
                    it = packs[size]
                    plan.append({"label": f"{size} {unit}".strip(), "ids": [int(it["id"])],
                                 "price": float(it.get("price") or 0)})
                continue
        m = _UC_RE.match(text)
        if m:
            amount = int(re.sub(r"\s", "", m.group(1)))
            packs = {int(k): v["price"] for k, v in _config["uc_packs"].items() if not k.startswith("_")}
            sizes = split_uc(amount, packs)
            if not sizes:
                raise _Manual(f"{amount} UC нельзя собрать из пакетов")
            for size in sizes:
                plan.append({"label": f"{size} UC", "ids": _config["uc_packs"][str(size)]["ids"],
                             "price": packs[size]})
            continue
        raise _Manual(err)
    if not plan and product.get("amount_packs") and product.get("_title"):
        head = str(product["_title"]).split("|")[0]
        nums = re.findall(r"\d[\d\s]*(?:\+\s*\d+)?", head)
        if len(nums) == 1:
            return _mix_plan(product, [("_title", head)], id_name)
    if not plan:
        raise _Manual("в заказе не нашёл выбранных товаров")
    return plan


def _deliver_mix(invoice_id, product, info, resume=None):
    title = product.get("title", "")
    if resume:
        player_id, plan = resume["player_id"], resume["plan"]
    else:
        options = _options(info or {})
        id_name = next((n for n, _ in options if product["id_option"].lower() in n.lower()), None)
        player_id = parse_player_id(dict(options).get(id_name)) if id_name else None
        try:
            cnt = float(str((info or {}).get("cnt_goods") or 1).replace(",", "."))
            if cnt != 1:
                raise _Manual(f"количество в заказе {cnt:g}, автовыдача рассчитана на 1")
            if not player_id:
                raise _Manual("не нашёл ID игрока")
            plan = _mix_plan(dict(product, _title=(info or {}).get("_product_name", "")), options, id_name)
        except _Manual as e:
            _set_status(invoice_id, status="manual", error=str(e))
            _notify(f"❗ Автовыдача, заказ {invoice_id} ({title}): {e}. Выдайте вручную.\n"
                    f"Опции: {options or (info or {}).get('options')}")
            return
        except Exception as e:
            _set_status(invoice_id, status="manual", error="каталог EpinBy недоступен")
            _notify(f"❗ Заказ {invoice_id} ({title}): каталог EpinBy не получен, выдайте вручную.\n"
                    f"{_hide(e)[:200]}")
            return
        _set_status(invoice_id, status="new", player_id=player_id, plan=plan)

    plan_text = " + ".join(p["label"] for p in plan)
    est = sum(p.get("price") or 0 for p in plan)
    nickname = None
    try:
        v = epinby_post("/validate-player", {"product_id": plan[0]["ids"][0], "player_id": player_id})
        nickname = v.get("nickname") or v.get("player_name")
    except EpinbyError as e:
        if e.code == "VALIDATION_ERROR":
            _set_status(invoice_id, status="manual", error="неверный ID")
            _notify(f"⚠️ Заказ {invoice_id} ({title}): EpinBy не нашёл игрока с ID {player_id}. "
                    f"Покупателю не писал, выдайте вручную.\nОтвет: {e.message}")
            return
        print(f"Заказ {invoice_id}: проверка игрока не удалась ({e.code}), продолжаю")
    who = f"{player_id}" + (f" ({nickname})" if nickname else "")

    if _mix_dry(product):
        _set_status(invoice_id, status="dry_run")
        _notify(f"🧪 Тест. Заказ {invoice_id} ({title}) → игрок {who}.\n"
                f"Купил бы: {plan_text} (≈ ${est:.2f}).\nНичего не куплено ({product.get('dry_run_env') or 'PACKS_DRY_RUN'}=1)."
                + _balance_line())
        return

    _set_status(invoice_id, status="in_progress")
    done = []
    for idx, part in enumerate(plan):
        ok, _, err = _buy_ids(invoice_id, idx, part["label"], part["ids"], player_id)
        if ok:
            done.append(part["label"])
            continue
        _set_status(invoice_id, status="manual", error=err, done=done)
        _notify(f"❗ Заказ {invoice_id} ({title}): выдача остановилась.\n"
                f"Игрок: {who}\nВыдано: {' + '.join(done) or 'ничего'}\n"
                f"Не выдано: {' + '.join(p['label'] for p in plan[idx:])}\nПричина: {err}\n"
                f"Покупателю не писал, доделайте вручную." + _balance_line())
        return

    _set_status(invoice_id, status="done", done=done)
    _send_chat(invoice_id, MSG_DONE)
    _notify(f"✅ Заказ {invoice_id} выдан автоматически: {plan_text} → {who}." + _balance_line())
