"""
Клиент G2Bulk API (https://api.g2bulk.com/docs) для автовыдачи игровых пополнений.

Что важно про G2Bulk:
  * Ключ передаётся в заголовке X-API-Key.
  * Защита от двойной покупки (X-Idempotency-Key) работает только 30 минут
    и принимает только UUID из 36 символов. Поэтому дополнительно каждый заказ
    подписывается полем remark (номер заказа GGSel + номер части), и перед
    повторной покупкой бот ищет такой заказ в истории G2Bulk.
  * Пополнение возвращается со статусом PENDING и доходит до COMPLETED или FAILED.
    При FAILED деньги сами возвращаются на баланс.
  * Цены меняются вместе с курсом, поэтому каталог перечитывается перед каждой покупкой.

Переменные окружения:
    G2BULK_API_KEY  - X-API-Key из кабинета g2bulk.com (раздел API)
"""

import math
import os
import re
import time
import uuid

import requests

API = "https://api.g2bulk.com/v1"
KEY = os.getenv("G2BULK_API_KEY", "").strip()

# постоянное пространство имён: из него и номера заказа GGSel получается один и тот же UUID
_NS = uuid.UUID("6f1c2b8e-3d4a-5b6c-8d9e-0a1b2c3d4e5f")

_catalogue_cache = {}   # game -> (время, список)
CATALOGUE_TTL = 60      # секунд


class G2Error(Exception):
    def __init__(self, message, http_status=None):
        super().__init__(f"{http_status}: {message}" if http_status else str(message))
        self.message = str(message)
        self.http_status = http_status

    @property
    def no_money(self):
        return "balance" in self.message.lower()

    @property
    def network(self):
        return self.http_status is None or self.http_status >= 500


def _request(method, path, body=None, params=None, idem_key=None, auth=True):
    headers = {"Accept": "application/json"}
    if auth:
        headers["X-API-Key"] = KEY
    if idem_key:
        headers["X-Idempotency-Key"] = idem_key
    for attempt in range(3):
        try:
            r = requests.request(method, API + path, json=body, params=params,
                                 headers=headers, timeout=30)
        except requests.RequestException as e:
            raise G2Error(f"сеть: {type(e).__name__}") from None
        if r.status_code == 429 and attempt < 2:
            time.sleep(min(int(r.headers.get("Retry-After") or 5), 60))
            continue
        break
    try:
        data = r.json()
    except Exception:
        raise G2Error(f"непонятный ответ: {r.text[:200]}", r.status_code)
    if r.status_code >= 400 or (isinstance(data, dict) and data.get("success") is False):
        msg = data.get("message") if isinstance(data, dict) else None
        if not msg and isinstance(data, dict):
            detail = data.get("detail")
            msg = detail.get("message") if isinstance(detail, dict) else detail
        raise G2Error(msg or str(data)[:200], r.status_code)
    return data


# ---------------------------------------------------------------- справочники (без ключа)

def games():
    return _request("GET", "/games", auth=False).get("games") or []


def catalogue(game, fresh=False):
    """[{"name": "60 UC", "amount": 0.88, "id": 1}, ...] — amount это цена в $."""
    cached = _catalogue_cache.get(game)
    if cached and not fresh and time.time() - cached[0] < CATALOGUE_TTL:
        return cached[1]
    items = _request("GET", f"/games/{game}/catalogue", auth=False).get("catalogues") or []
    _catalogue_cache[game] = (time.time(), items)
    return items


def fields(game):
    data = _request("POST", "/games/fields", {"game": game}, auth=False)
    return (data.get("info") or {}).get("fields") or []


def check_player(game, user_id, server_id=None):
    """-> (True/False/None, ник). None — проверить не удалось."""
    body = {"game": game, "user_id": str(user_id)}
    if server_id:
        body["server_id"] = str(server_id)
    try:
        data = _request("POST", "/games/checkPlayerId", body, auth=False)
    except G2Error:
        return None, None
    valid = str(data.get("valid") or "").lower()
    name = data.get("name") or None
    if valid == "valid":
        return True, name
    if valid == "invalid":
        return False, None
    return None, name


# ---------------------------------------------------------------- с ключом

def balance():
    return float(_request("GET", "/getMe").get("balance"))


def idem_key(*parts):
    return str(uuid.uuid5(_NS, "-".join(str(p) for p in parts)))


def place_order(game, catalogue_name, player_id, server_id=None, remark=None, key=None):
    body = {"catalogue_name": catalogue_name, "player_id": str(player_id)}
    if server_id:
        body["server_id"] = str(server_id)
    if remark:
        body["remark"] = remark
    return _request("POST", f"/games/{game}/order", body, idem_key=key).get("order") or {}


def get_order(order_id):
    try:
        return _request("GET", f"/games/orders/{order_id}").get("order") or {}
    except G2Error as e:
        if e.http_status == 410:
            return {"order_id": order_id, "status": "FAILED", "message": e.message}
        raise


def find_order_by_remark(game, remark):
    """Ищет в истории G2Bulk заказ с нашей подписью. Нужен, если бот упал сразу после покупки."""
    data = _request("GET", "/games/orders", params={"search": remark, "game": game, "limit": 20})
    for o in data.get("orders") or []:
        if str(o.get("remark") or "") == remark:
            return o
    return None


# ---------------------------------------------------------------- пакеты

def _norm(s):
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def denominations(game, unit):
    """
    Пакеты из каталога, название которых имеет вид "<число> <unit>", например "60 UC".
    -> {количество: (название в каталоге, цена $)}. Пакеты с другими словами
    (пропуски, наборы, акции) не берутся, чтобы не выдать не то.
    """
    result = {}
    want = _norm(unit)
    for item in catalogue(game):
        name = str(item.get("name") or "")
        m = re.match(r"^\s*(\d[\d,\s]*)\s*(.*?)\s*$", name)
        if not m:
            continue
        rest = _norm(m.group(2))
        if want and rest != want:
            continue
        qty = int(re.sub(r"[,\s]", "", m.group(1)))
        price = item.get("amount")
        if qty <= 0 or price is None:
            continue
        price = float(price)
        if qty not in result or price < result[qty][1]:
            result[qty] = (name, price)
    return result


def split(amount, packs):
    """
    Раскладывает amount на пакеты так, чтобы сумма совпала точно, а цена была минимальной.
    packs: {размер: цена}. -> список размеров от большего к меньшему или None.
    """
    sizes = sorted(packs)
    if amount <= 0 or not sizes:
        return None
    step = 0
    for s in sizes + [amount]:
        step = math.gcd(step, s)
    n = amount // step
    if n > 200000:
        return None
    INF = float("inf")
    best = [(0.0, 0)] + [(INF, 0)] * n
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
