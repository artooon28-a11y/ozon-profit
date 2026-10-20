"""Official Ozon Seller API client (https://docs.ozon.ru/api/seller/).

Only read methods are used. Everything is converted to the same "lines" format
the web page builds from the accruals .xlsx report, so one calculation serves both.

Method versions change over time: if Ozon retires one, adjust the paths below
(see the README, section "Если Ozon поменял API").
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from collections import Counter

import httpx

API_BASE = os.environ.get("OZON_API_BASE", "https://api-seller.ozon.ru")

# /v3/finance/transaction/list was switched off in September 2026; accruals now come day by day
PATH_ACCRUALS_DAY = "/v1/finance/accrual/by-day"
PATH_ACCRUAL_TYPES = "/v1/finance/accrual/types"
PATH_PRODUCT_LIST = "/v3/product/list"
PATH_PRODUCT_INFO = "/v3/product/info/list"
PATH_PRICES = "/v5/product/info/prices"


class OzonError(Exception):
    """Error with a message that can be shown to the seller as is."""


class OzonClient:
    def __init__(self, client_id: str, api_key: str):
        self.headers = {"Client-Id": client_id.strip(), "Api-Key": api_key.strip(), "Content-Type": "application/json"}
        self.http = httpx.AsyncClient(base_url=API_BASE, headers=self.headers, timeout=60)

    async def close(self):
        await self.http.aclose()

    async def post(self, path: str, body: dict) -> dict:
        for attempt in range(4):
            try:
                r = await self.http.post(path, json=body)
            except httpx.HTTPError as e:
                if attempt == 3:
                    raise OzonError("Ozon API не отвечает. Попробуйте позже.") from e
                await asyncio.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 429:  # rate limit: wait and retry
                await asyncio.sleep(2 * (attempt + 1))
                continue
            if r.status_code in (401, 403):
                raise OzonError("Ozon не принял ключ. Проверьте Client-Id и API-ключ и что у ключа есть доступ к финансам и товарам.")
            if r.status_code >= 400:
                try:
                    msg = r.json().get("message") or r.text
                except ValueError:
                    msg = r.text
                raise OzonError(f"Ozon API вернул ошибку {r.status_code}: {msg[:300]}")
            return r.json()
        raise OzonError("Ozon API ограничил частоту запросов. Попробуйте через минуту.")

    # ---------- checks ----------
    async def check(self) -> None:
        await self.post(PATH_PRODUCT_LIST, {"filter": {"visibility": "ALL"}, "last_id": "", "limit": 1})
        await self.post(PATH_ACCRUAL_TYPES, {})  # the key must also see finance

    # ---------- products ----------
    async def products(self) -> dict:
        """offer_id, name, sku list and cost (net_price) for every product."""
        ids, offers, last_id = [], {}, ""
        for _ in range(200):
            j = await self.post(PATH_PRODUCT_LIST, {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000})
            res = j.get("result") or {}
            items = res.get("items") or []
            for it in items:
                ids.append(it.get("product_id"))
                offers[it.get("product_id")] = it.get("offer_id")
            last_id = res.get("last_id") or ""
            if not items or not last_id:
                break

        info: dict[str, dict] = {}   # offer_id -> {name, skus}
        sku_to_offer: dict[str, str] = {}
        for i in range(0, len(ids), 1000):
            j = await self.post(PATH_PRODUCT_INFO, {"product_id": ids[i:i + 1000]})
            items = j.get("items") or (j.get("result") or {}).get("items") or []
            for it in items:
                offer = str(it.get("offer_id") or offers.get(it.get("id")) or "")
                skus = {str(s) for s in (it.get("sku"), it.get("fbo_sku"), it.get("fbs_sku")) if s}
                for src in it.get("sources") or []:
                    if src.get("sku"):
                        skus.add(str(src["sku"]))
                info[offer] = {"name": it.get("name") or "", "skus": sorted(skus)}
                for s in skus:
                    sku_to_offer[s] = offer

        costs: dict[str, float] = {}
        cursor = ""
        for _ in range(200):
            j = await self.post(PATH_PRICES, {"filter": {"visibility": "ALL"}, "cursor": cursor, "limit": 1000})
            items = j.get("items") or (j.get("result") or {}).get("items") or []
            for it in items:
                price = it.get("price") or {}
                try:
                    v = float(price.get("net_price") or 0)
                except (TypeError, ValueError):
                    v = 0
                if v > 0 and it.get("offer_id"):
                    costs[str(it["offer_id"])] = v
            cursor = j.get("cursor") or ""
            if not items or not cursor:
                break

        return {"names": {k: v["name"] for k, v in info.items()}, "sku_to_offer": sku_to_offer, "costs": costs}

    # ---------- finance ----------
    async def accrual_types(self) -> dict[int, str]:
        j = await self.post(PATH_ACCRUAL_TYPES, {})
        return {int(t.get("id") or 0): (t.get("name") or t.get("description") or "") for t in j.get("accrual_types") or []}

    async def accruals_day(self, day: dt.date) -> list[dict]:
        out, last_id = [], ""
        for _ in range(500):
            j = await self.post(PATH_ACCRUALS_DAY, {"date": day.isoformat(), "last_id": last_id})
            out.extend(j.get("accruals") or [])
            last_id = j.get("last_id") or ""
            if not last_id or not j.get("accruals"):
                break
        return out

    async def accruals(self, date_from: dt.date, date_to: dt.date, parallel: int = 4) -> list[dict]:
        """All accruals between the dates (the method takes one day per request)."""
        days = [date_from + dt.timedelta(days=i) for i in range((date_to - date_from).days + 1)]
        sem = asyncio.Semaphore(parallel)

        async def one(d):
            async with sem:
                return await self.accruals_day(d)

        out = []
        for chunk in await asyncio.gather(*(one(d) for d in days)):
            out.extend(chunk)
        return out


# ---------- conversion to page "lines" ----------
def _num(m) -> float:
    """Money object {"amount": "-12.5", "currency": "RUB"} or a plain number -> float."""
    if isinstance(m, dict):
        m = m.get("amount")
    try:
        return float(m or 0)
    except (TypeError, ValueError):
        return 0.0


def type_bucket(name: str) -> str:
    """Accrual type name (Russian names from /v1/finance/accrual/types, or old English codes) -> page bucket."""
    n = (name or "").lower()
    if "эквайринг" in n or "acquiring" in n:
        return "acq"
    if ("обратн" in n or "возврат" in n or "невыкуп" in n or "отмен" in n or "return" in n or "notdeliv" in n) and "реклам" not in n:
        return "rlog"
    if any(k in n for k in ("реклам", "продвиж", "маркетинг", "трафарет", "за клик", "за заказ", "premium", "costperclick", "promotion")):
        return "ads"
    if any(k in n for k in ("штраф", "утилизац", "жалоб", "хранени", "размещени", "fine", "penalty", "storage", "disposal")):
        return "fine"
    if "вознагражд" in n or "комисси" in n:
        return "fee"
    if any(k in n for k in ("логистик", "доставк", "магистрал", "миля", "курьер", "logistic", "directflow", "delivto")):
        return "log"
    return "svc"


def posting_key(s: str) -> str:
    p = str(s or "").split("-")
    return "-".join(p[:2]) if len(p) >= 3 else str(s or "")


def to_lines(accruals: list[dict], types: dict[int, str], sku_to_offer: dict[str, str], names: dict[str, str]) -> list[dict]:
    """Convert /v1/finance/accrual/by-day records to the page "lines". Every record's total_amount is kept exactly:
    whatever is not itemised goes to a remainder line, so the payout always matches Ozon."""
    lines = []

    def art_of(sku) -> str:
        sku = str(sku or "")
        return sku_to_offer.get(sku) or (f"SKU {sku}" if sku and sku != "0" else "")

    for a in accruals:
        cat = a.get("accrued_category") or "UNSPECIFIED"
        unit = str(a.get("unit_number") or "")
        key = posting_key(unit) or f"acc-{a.get('accrual_id')}"
        date = str(a.get("date") or "")[:10]
        total = _num(a.get("total_amount"))
        parts = []  # (article, bucket, amount, revenue, qty, type name)

        def add(art, bucket, amount, revenue=False, qty=0, tname=""):
            if abs(amount) >= 0.005:
                parts.append((art, bucket, amount, revenue, qty, tname))

        def fee(art, f):
            tname = types.get(int(f.get("type_id") or 0), "") or f"Начисление {f.get('type_id')}"
            add(art, type_bucket(tname), _num(f.get("accrued")), tname=tname)

        if cat == "POSTING":
            for pr in (a.get("posting") or {}).get("products") or []:
                art = art_of(pr.get("sku"))
                c = pr.get("commission") or {}
                sale = _num(c.get("sale_amount"))
                unit_price = abs(_num(c.get("seller_price")))
                qty = max(1, round(abs(sale) / unit_price)) if sale and unit_price else (1 if sale else 0)
                if sale > 0:
                    add(art, "rz", sale, True, qty, "Выручка")
                elif sale < 0:
                    add(art, "rzNeg", sale, tname="Возврат выручки")
                comm = abs(_num(c.get("commission")))
                if comm:  # charged on a sale, refunded on a return
                    add(art, "fee", -comm if sale >= 0 else comm, tname="Вознаграждение Ozon")
                d = pr.get("delivery") or {}
                svcs = d.get("services") or []
                if any(_num(x.get("accrued")) for x in svcs):
                    for x in svcs:
                        fee(art, x)
                else:
                    add(art, "log", _num(d.get("total_accrued")), tname="Логистика")
        elif cat == "ITEM":
            for it in (a.get("item_fees") or {}).get("fees") or []:
                art = art_of(it.get("sku"))
                for f in it.get("fees") or []:
                    fee(art, f)
        elif cat == "NON_ITEM":
            f = a.get("non_item_fee") or {}
            if f:
                fee("", f)
        elif cat == "CONTAINER_FEES":
            for f in (a.get("container_fees") or {}).get("fees") or []:
                fee("", f)

        rest = round(total - sum(p[2] for p in parts), 2)
        if abs(rest) >= 0.01:  # not itemised by Ozon (or itemised differently): keep totals exact
            arts = [p[0] for p in parts if p[0]] or [""]
            first = arts[0]
            tname = "Прочие начисления"
            if cat in ("NON_ITEM", "CONTAINER_FEES") and parts:
                tname = parts[0][5]
            add(first, type_bucket(tname) if tname != "Прочие начисления" else ("fee" if cat == "POSTING" else "svc"), rest, tname=tname)

        group = {"POSTING": "Отправление", "ITEM": "Услуги по товару", "NON_ITEM": "Услуги продавцу", "CONTAINER_FEES": "Контейнер"}.get(cat, "Начисление")
        for art, bucket, amount, revenue, qty, tname in parts:
            lines.append({
                "id": unit or key, "posting": key, "date": date,
                "group": group, "type": tname, "bucket": bucket, "revenue": revenue,
                "article": art, "name": names.get(art, ""),
                "qty": qty if revenue else 0, "amount": round(amount, 2),
            })
    return lines
