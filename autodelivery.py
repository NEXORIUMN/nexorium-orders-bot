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
"""

import functools
import json
import os
import re
import threading
import time

import requests

print = functools.partial(print, flush=True)

EPINBY_API = "https://epinby.com/api/v1"
EPINBY_KEY = os.getenv("EPINBY_API_KEY", "").strip()
ENABLED = os.getenv("AUTODELIVERY", "off").strip().lower() in ("on", "1", "true", "yes")
DRY_RUN = os.getenv("DRY_RUN", "1").strip().lower() not in ("0", "off", "false", "no")
STATE_DIR = os.getenv("STATE_DIR", ".").strip() or "."
STATE_FILE = os.path.join(STATE_DIR, "deliveries.json")
PRODUCTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.json")

LOW_BALANCE = float(os.getenv("LOW_BALANCE", "20") or 20)  # предупреждать, если баланс ниже ($)
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
    if not EPINBY_KEY:
        print("Автовыдача: не задан EPINBY_API_KEY, выключаю.")
        notify("⚠️ Автовыдача не запущена: не задана переменная EPINBY_API_KEY.")
        return
    load_config()
    try:
        me = epinby_get("/getMe")
        balance = me.get("balance")
    except Exception as e:
        balance = None
        print("Автовыдача: EpinBy не ответил:", _hide(e))
    mode = "🧪 ТЕСТОВЫЙ режим (ничего не покупаю)" if DRY_RUN else "🟢 БОЕВОЙ режим"
    products = len([k for k in _config.get("ggsel_products", {}) if not k.startswith("_")])
    text = f"🤖 Автовыдача включена. {mode}.\nТоваров на автомате: {products}."
    if balance is not None:
        text += f"\nБаланс EpinBy: ${balance}"
    notify(text)
    _resume_unfinished()


def is_on():
    return ENABLED and bool(EPINBY_KEY) and _config is not None


def _hide(text):
    fn = _hooks.get("hide")
    text = str(text).replace(EPINBY_KEY, "***") if EPINBY_KEY else str(text)
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
    if _get_record(invoice_id):
        return True  # уже обрабатывали
    _set_status(invoice_id, status="new", product_id=str(product_id))
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
                _send_chat(invoice_id, MSG_BAD_ID.format(player_id=player_id))
            _notify(f"⚠️ Заказ {invoice_id}: EpinBy не нашёл игрока с ID {player_id}. "
                    f"Покупатель{' (тест: не уведомлён)' if DRY_RUN else ''} попрошен проверить ID.\n"
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
        _send_chat(invoice_id, MSG_MANUAL)
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
    """Покупает один пакет, перебирая основной и запасные товары. -> (успех, product_id, ошибка)"""
    last_err = "нет товаров"
    for pid in _config["uc_packs"][str(size)]["ids"]:
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
                last_err = f"{size} UC (товар {pid}): {e.code} {e.message}"
                break
            except Exception as e:
                # сеть: заказ мог создаться, поэтому повторяем с ТЕМ ЖЕ ключом - дубля не будет
                last_err = f"{size} UC (товар {pid}): сеть: {_hide(e)[:150]}"
                time.sleep(5)
        if order is None:
            if "сеть" in last_err:
                # не знаем, создан ли заказ - запасной брать нельзя, иначе возможна двойная выдача
                return False, pid, last_err
            print(f"Заказ {invoice_id}: {last_err}, пробую запасной")
            continue

        status = _wait_order(order.get("order_id"), order.get("status"))
        if status == "COMPLETED":
            print(f"Заказ {invoice_id}: {size} UC выдано (товар {pid}, EpinBy #{order.get('order_id')})")
            return True, pid, None
        last_err = f"{size} UC (товар {pid}, EpinBy #{order.get('order_id')}): статус {status}"
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


def _send_chat(invoice_id, text):
    fn = _hooks.get("send_chat")
    if not fn:
        return
    try:
        fn(invoice_id, text)
    except Exception as e:
        _notify(f"⚠️ Заказ {invoice_id}: не смог написать покупателю в чат GGSel: {_hide(e)[:200]}")
