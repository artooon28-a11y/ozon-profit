"""Прибыль на Ozon — small server: the page, plus read-only Ozon Seller API connections.

- GET  /                 the page (file mode: everything is calculated in the browser)
- GET  /d/{token}        the same page in API mode for one connected seller
- POST /api/connect      check Client-Id + Api-Key with Ozon, store them encrypted, return a personal token
- GET  /api/data/{token} accruals + products for the last months (cached, refreshed on demand)
- DELETE /api/connect/{token}  forget the keys and the cached data
- POST /api/event        anonymous daily counter (page opened, report calculated, …): no IP, no cookies, no ids
- GET  /admin/stats?key= active users and daily counters for the owner (needs ADMIN_KEY)
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import time
from collections import defaultdict, deque
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ozon import OzonClient, OzonError, to_lines

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "app.db"
FERNET = Fernet(os.environ["FERNET_KEY"].encode())
TG_USERNAME = os.environ.get("TG_USERNAME", "").lstrip("@").strip()
MONTHS_BACK = int(os.environ.get("MONTHS_BACK", "3"))
CACHE_HOURS = float(os.environ.get("CACHE_HOURS", "6"))
ADMIN_KEY = os.environ.get("ADMIN_KEY", "").strip()
MIN_REFRESH_SECONDS = 300
INDEX = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ozon-profit")
logging.getLogger("httpx").setLevel(logging.WARNING)

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/static/assets", StaticFiles(directory=Path(__file__).parent / "static" / "assets"), name="assets")


# ---------- storage ----------
def db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA secure_delete = ON")  # deleted keys are overwritten in the file, not just unlinked
    con.execute("""CREATE TABLE IF NOT EXISTS connections (
        token_hash TEXT PRIMARY KEY, client_id_enc BLOB, api_key_enc BLOB,
        created_at REAL, fetched_at REAL, cache_enc BLOB)""")
    cols = {r[1] for r in con.execute("PRAGMA table_info(connections)")}
    if "consent_version" not in cols:
        con.execute("ALTER TABLE connections ADD COLUMN consent_version TEXT")
        con.execute("ALTER TABLE connections ADD COLUMN consent_at REAL")
    if "last_seen_at" not in cols:
        con.execute("ALTER TABLE connections ADD COLUMN last_seen_at REAL")
        con.execute("ALTER TABLE connections ADD COLUMN source TEXT")
    con.execute("""CREATE TABLE IF NOT EXISTS daily_events (
        day TEXT, event TEXT, source TEXT, n INTEGER, PRIMARY KEY (day, event, source))""")
    return con


# ---------- anonymous counters ----------
EVENTS = ["visit", "file_report", "file_costs", "connect_start"]
_SRC = re.compile(r"[a-z0-9_.-]{1,32}")


def clean_source(s: str | None) -> str:
    """utm_source / ref from the landing URL, kept only if it looks like a short tag."""
    s = (s or "").strip().lower()
    return s if _SRC.fullmatch(s) else ""


def count(event: str, source: str = ""):
    with db() as con:
        con.execute("INSERT INTO daily_events (day, event, source, n) VALUES (?,?,?,1)"
                    " ON CONFLICT(day, event, source) DO UPDATE SET n = n + 1",
                    (dt.date.today().isoformat(), event, source))


def h(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def enc(s: str) -> bytes:
    return FERNET.encrypt(s.encode())


def dec(b: bytes) -> str:
    return FERNET.decrypt(b).decode()


def load(token: str):
    with db() as con:
        row = con.execute("SELECT client_id_enc, api_key_enc, fetched_at, cache_enc FROM connections WHERE token_hash=?", (h(token),)).fetchone()
    if not row:
        raise HTTPException(404, "Подключение не найдено. Возможно, его отключили — подключите кабинет заново.")
    try:
        return dec(row[0]), dec(row[1]), row[2], (json.loads(dec(row[3])) if row[3] else None)
    except InvalidToken:
        raise HTTPException(500, "Не удалось расшифровать данные подключения. Подключите кабинет заново.")


# ---------- simple per-IP limit for /api/connect ----------
_hits: dict[str, deque] = defaultdict(deque)


def limit(ip: str, n: int = 10, per: int = 3600):
    q = _hits[ip]
    now = time.time()
    while q and now - q[0] > per:
        q.popleft()
    if len(q) >= n:
        raise HTTPException(429, "Слишком много попыток подключения. Попробуйте через час.")
    q.append(now)


_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


# ---------- page ----------
def page() -> HTMLResponse:
    html = INDEX.replace("@@TG_USERNAME@@", TG_USERNAME)
    return HTMLResponse(html, headers={
        "Cache-Control": "no-store",
        "Referrer-Policy": "no-referrer",  # the /d/ link is personal: never leak it in Referer
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
    })


@app.get("/", response_class=HTMLResponse)
def index():
    return page()


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse(Path(__file__).parent / "static" / "assets" / "favicon.ico", media_type="image/x-icon",
                        headers={"Cache-Control": "public, max-age=604800"})


# ---------- legal pages ----------
LEGAL_DIR = Path(__file__).parent / "legal"
LEGAL = {"privacy": "Политика обработки персональных данных", "consent": "Согласие на обработку персональных данных", "terms": "Пользовательское соглашение"}


def legal_page(name: str) -> HTMLResponse:
    html = (LEGAL_DIR / "_layout.html").read_text(encoding="utf-8")
    html = html.replace("@@TITLE@@", LEGAL[name]).replace("@@BODY@@", (LEGAL_DIR / f"{name}.html").read_text(encoding="utf-8"))
    return HTMLResponse(html, headers={"Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff"})


@app.get("/privacy", response_class=HTMLResponse)
def privacy():
    return legal_page("privacy")


@app.get("/consent", response_class=HTMLResponse)
def consent():
    return legal_page("consent")


@app.get("/terms", response_class=HTMLResponse)
def terms():
    return legal_page("terms")


@app.get("/d/{token}", response_class=HTMLResponse)
def dashboard(token: str):
    return page()


# ---------- API ----------
CONSENT_VERSION = "1.2 от 21.10.2026"


class ConnectIn(BaseModel):
    client_id: str = Field(min_length=1, max_length=40)
    api_key: str = Field(min_length=10, max_length=200)
    consent: bool = False
    source: str | None = Field(default=None, max_length=64)


@app.post("/api/connect")
async def connect(body: ConnectIn, request: Request):
    limit(request.client.host if request.client else "?")
    if not body.consent:
        raise HTTPException(400, "Чтобы подключить кабинет, отметьте согласие на обработку персональных данных.")
    if not body.client_id.strip().isdigit():
        raise HTTPException(400, "Client-Id состоит только из цифр. Скопируйте его из раздела с API-ключами в кабинете.")
    client = OzonClient(body.client_id, body.api_key)
    try:
        await client.check()
    except OzonError as e:
        raise HTTPException(400, str(e))
    finally:
        await client.close()
    token = secrets.token_urlsafe(24)
    with db() as con:
        now = time.time()
        con.execute("INSERT INTO connections (token_hash, client_id_enc, api_key_enc, created_at, fetched_at, cache_enc, consent_version, consent_at, last_seen_at, source)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (h(token), enc(body.client_id.strip()), enc(body.api_key.strip()), now, None, None, CONSENT_VERSION, now, now, clean_source(body.source)))
    return {"token": token}


async def sync(token: str, client_id: str, api_key: str) -> dict:
    today = dt.date.today()
    first = today.replace(day=1)
    for _ in range(MONTHS_BACK):
        first = (first - dt.timedelta(days=1)).replace(day=1)
    client = OzonClient(client_id, api_key)
    try:
        prods = await client.products()
        types = await client.accrual_types()
        accs = await client.accruals(first, today)
    finally:
        await client.close()
    data = {
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "period": [first.isoformat(), today.isoformat()],
        "lines": to_lines(accs, types, prods["sku_to_offer"], prods["names"]),
        "costs": prods["costs"], "names": prods["names"],
    }
    with db() as con:
        con.execute("UPDATE connections SET fetched_at=?, cache_enc=? WHERE token_hash=?",
                    (time.time(), enc(json.dumps(data, ensure_ascii=False)), h(token)))
    return data


@app.get("/api/data/{token}")
async def data(token: str, refresh: int = 0):
    async with _locks[h(token)]:
        client_id, api_key, fetched_at, cache = load(token)
        with db() as con:  # "active" = opened the dashboard recently, see /admin/stats
            con.execute("UPDATE connections SET last_seen_at=? WHERE token_hash=?", (time.time(), h(token)))
        age = time.time() - (fetched_at or 0)
        stale = cache is None or age > CACHE_HOURS * 3600
        if cache is not None and (not refresh or age < MIN_REFRESH_SECONDS) and not stale:
            return JSONResponse(cache, headers={"Cache-Control": "no-store"})
        try:
            fresh = await sync(token, client_id, api_key)
        except OzonError as e:
            log.warning("sync failed: %s", e)
            if cache is not None:  # keep showing the last data, say why it is old
                return JSONResponse(dict(cache, warning=str(e)), headers={"Cache-Control": "no-store"})
            raise HTTPException(502, str(e))
        except Exception as e:  # unexpected answer from Ozon: log it and say what broke instead of a bare 500
            log.exception("sync crashed")
            msg = f"Не удалось обработать ответ Ozon ({type(e).__name__}: {str(e)[:200]}). Перешлите этот текст разработчику."
            if cache is not None:
                return JSONResponse(dict(cache, warning=msg), headers={"Cache-Control": "no-store"})
            raise HTTPException(502, msg)
        return JSONResponse(fresh, headers={"Cache-Control": "no-store"})


@app.delete("/api/connect/{token}")
def disconnect(token: str):
    with db() as con:
        con.execute("DELETE FROM connections WHERE token_hash=?", (h(token),))
    return {"ok": True}


class EventIn(BaseModel):
    event: str = Field(max_length=32)
    source: str | None = Field(default=None, max_length=64)


@app.post("/api/event", status_code=204)
def event(body: EventIn):
    if body.event in EVENTS:
        count(body.event, clean_source(body.source))
    return Response(status_code=204)


# ---------- owner stats ----------
def _table(head: list[str], rows: list) -> str:
    th = "".join(f"<th>{x}</th>" for x in head)
    tr = "".join("<tr>" + "".join(f"<td>{x}</td>" for x in r) + "</tr>" for r in rows) or f"<tr><td colspan={len(head)}>пока пусто</td></tr>"
    return f"<table><tr>{th}</tr>{tr}</table>"


@app.get("/admin/stats", response_class=HTMLResponse)
def stats(key: str = ""):
    if not ADMIN_KEY or not secrets.compare_digest(key.encode(), ADMIN_KEY.encode()):
        raise HTTPException(404, "Not Found")
    now = time.time()
    d7, d30 = now - 7 * 86400, now - 30 * 86400
    since = (dt.date.today() - dt.timedelta(days=29)).isoformat()
    with db() as con:
        total, active7, active30, new7, new30 = con.execute(
            "SELECT COUNT(*), SUM(last_seen_at>=?), SUM(last_seen_at>=?), SUM(created_at>=?), SUM(created_at>=?) FROM connections",
            (d7, d30, d7, d30)).fetchone()
        by_src = con.execute("SELECT COALESCE(NULLIF(source,''),'—'), COUNT(*), SUM(last_seen_at>=?) FROM connections"
                             " GROUP BY 1 ORDER BY 2 DESC", (d30,)).fetchall()
        days = con.execute("SELECT day, event, SUM(n) FROM daily_events WHERE day>=? GROUP BY day, event", (since,)).fetchall()
        ev_src = con.execute("SELECT COALESCE(NULLIF(source,''),'—'), event, SUM(n) FROM daily_events WHERE day>=?"
                             " GROUP BY 1, 2", (since,)).fetchall()
    grid: dict[str, dict] = defaultdict(dict)
    for day, ev, n in days:
        grid[day][ev] = n
    srcs: dict[str, dict] = defaultdict(dict)
    for src, ev, n in ev_src:
        srcs[src][ev] = n
    html = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Статистика</title>
<style>body{{font:15px/1.4 system-ui,sans-serif;margin:24px;color:#1b1b1b;background:#fff}}table{{border-collapse:collapse;margin:8px 0 24px}}
td,th{{border:1px solid #ddd;padding:4px 10px;text-align:right}}th{{background:#f4f4f4}}td:first-child,th:first-child{{text-align:left}}
.big{{font-size:32px;font-weight:600}}</style></head><body>
<h1>Статистика</h1>
<p>Активные пользователи: кабинет подключён по API и дашборд открывали за последние 30 дней</p>
<p class="big">{active30 or 0} <small>/ 1000</small></p>
{_table(["", "7 дней", "30 дней", "всего"], [["Активные подключения", active7 or 0, active30 or 0, total], ["Новые подключения", new7 or 0, new30 or 0, total]])}
<h2>Подключения по источнику</h2>
{_table(["Источник", "всего", "активны за 30 дней"], [[s, n, a or 0] for s, n, a in by_src])}
<h2>События за 30 дней по источнику</h2>
{_table(["Источник"] + EVENTS, [[s] + [srcs[s].get(c, 0) for c in EVENTS] for s in sorted(srcs)])}
<h2>События по дням</h2>
<p><small>visit — открыли главную, file_report — посчитали отчёт из файла, file_costs — загрузили себестоимость, connect_start — начали вводить Client-Id.</small></p>
{_table(["День"] + EVENTS, [[d] + [grid[d].get(c, 0) for c in EVENTS] for d in sorted(grid, reverse=True)])}
</body></html>"""
    return HTMLResponse(html, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Robots-Tag": "noindex"})


@app.get("/healthz")
def healthz():
    return {"ok": True}
