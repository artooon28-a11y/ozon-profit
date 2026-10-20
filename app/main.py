"""Прибыль на Ozon — small server: the page, plus read-only Ozon Seller API connections.

- GET  /                 the page (file mode: everything is calculated in the browser)
- GET  /d/{token}        the same page in API mode for one connected seller
- POST /api/connect      check Client-Id + Api-Key with Ozon, store them encrypted, return a personal token
- GET  /api/data/{token} accruals + products for the last months (cached, refreshed on demand)
- DELETE /api/connect/{token}  forget the keys and the cached data
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import logging
import os
import secrets
import sqlite3
import time
from collections import defaultdict, deque
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ozon import OzonClient, OzonError, to_lines

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "app.db"
FERNET = Fernet(os.environ["FERNET_KEY"].encode())
TG_USERNAME = os.environ.get("TG_USERNAME", "").lstrip("@").strip()
MONTHS_BACK = int(os.environ.get("MONTHS_BACK", "3"))
CACHE_HOURS = float(os.environ.get("CACHE_HOURS", "6"))
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
    return con


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
CONSENT_VERSION = "1.0 от 20.10.2026"


class ConnectIn(BaseModel):
    client_id: str = Field(min_length=1, max_length=40)
    api_key: str = Field(min_length=10, max_length=200)
    consent: bool = False


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
        con.execute("INSERT INTO connections (token_hash, client_id_enc, api_key_enc, created_at, fetched_at, cache_enc, consent_version, consent_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (h(token), enc(body.client_id.strip()), enc(body.api_key.strip()), now, None, None, CONSENT_VERSION, now))
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


@app.get("/healthz")
def healthz():
    return {"ok": True}
