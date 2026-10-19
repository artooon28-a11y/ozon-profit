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

PATH_TRANSACTIONS = "/v3/finance/transaction/list"
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
    async def operations(self, date_from: dt.date, date_to: dt.date) -> list[dict]:
        """All finance operations, split into calendar months (Ozon allows up to one month per request)."""
        out = []
        start = date_from
        while start <= date_to:
            month_end = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
            end = min(month_end, date_to)
            page = 1
            while True:
                body = {
                    "filter": {
                        "date": {"from": f"{start.isoformat()}T00:00:00.000Z", "to": f"{end.isoformat()}T23:59:59.999Z"},
                        "operation_type": [], "posting_number": "", "transaction_type": "all",
                    },
                    "page": page, "page_size": 1000,
                }
                res = (await self.post(PATH_TRANSACTIONS, body)).get("result") or {}
                out.extend(res.get("operations") or [])
                if page >= int(res.get("page_count") or 0):
                    break
                page += 1
            start = end + dt.timedelta(days=1)
        return out


# ---------- conversion to page "lines" ----------
def _service_bucket(name: str) -> str:
    n = (name or "").lower()
    if "acquiring" in n:
        return "acq"
    if "return" in n or "notdeliv" in n:
        return "rlog"
    if "logistic" in n or "directflow" in n:
        return "log"
    return "svc"


def _rest_bucket(op_type: str) -> str:
    t = (op_type or "").lower()
    if "costperclick" in t or "promotion" in t or "advert" in t or "premium" in t:
        return "ads"
    if "fine" in t or "penalty" in t or "storage" in t or "disposal" in t or "utilization" in t:
        return "fine"
    return "svc"


def posting_key(s: str) -> str:
    p = str(s or "").split("-")
    return "-".join(p[:2]) if len(p) >= 3 else str(s or "")


def to_lines(operations: list[dict], sku_to_offer: dict[str, str], names: dict[str, str]) -> list[dict]:
    lines = []
    for op in operations:
        amount = float(op.get("amount") or 0)
        posting_number = (op.get("posting") or {}).get("posting_number") or ""
        date = str(op.get("operation_date") or "")[:10]
        op_type = op.get("operation_type") or ""
        op_name = op.get("operation_type_name") or op_type
        items = op.get("items") or []

        # which products the operation belongs to (several units of one product are counted)
        arts = Counter()
        for it in items:
            sku = str(it.get("sku") or "")
            arts[sku_to_offer.get(sku) or (f"SKU {sku}" if sku else "-")] += 1
        if not arts:
            arts["-"] = 1
        total_units = sum(arts.values())

        parts = []  # (bucket, amount, revenue_flag)
        afs = float(op.get("accruals_for_sale") or 0)
        if afs:
            parts.append(("rz" if afs > 0 else "rzNeg", afs, afs > 0))
        commission = float(op.get("sale_commission") or 0)
        if commission:
            parts.append(("fee", commission, False))
        for f, b in (("delivery_charge", "log"), ("return_delivery_charge", "rlog")):
            v = float(op.get(f) or 0)
            if v:
                parts.append((b, v, False))
        for s in op.get("services") or []:
            v = float(s.get("price") or 0)
            if v:
                parts.append((_service_bucket(s.get("name")), v, False))
        rest = round(amount - sum(p[1] for p in parts), 2)
        if abs(rest) >= 0.01:  # anything Ozon did not itemise, so totals always match the payout
            parts.append((_rest_bucket(op_type), rest, False))

        key = posting_key(posting_number) or f"op-{op.get('operation_id')}"
        for art, units in arts.items():
            share = units / total_units
            for bucket, value, revenue in parts:
                lines.append({
                    "id": posting_number or key, "posting": key, "date": date,
                    "group": op_name, "type": op_type, "bucket": bucket, "revenue": revenue,
                    "article": art if art != "-" else "", "name": names.get(art, ""),
                    "qty": units if revenue else 0, "amount": round(value * share, 2),
                })
    return lines
