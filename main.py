import base64
import json
import math
import time
import uuid
from collections import deque

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL = 49          # fixed catalog: ids 1..49
RATE_LIMIT = 16     # requests per client per window
WINDOW = 10.0       # seconds

app = FastAPI()

orders_by_key: dict[str, dict] = {}   # Idempotency-Key -> order
windows: dict[str, deque] = {}        # X-Client-Id -> request timestamps


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client = request.headers.get("x-client-id")
    if client and request.method != "OPTIONS":
        now = time.monotonic()
        q = windows.setdefault(client, deque())
        while q and now - q[0] >= WINDOW:
            q.popleft()
        if len(q) >= RATE_LIMIT:
            retry = max(1, math.ceil(WINDOW - (now - q[0])))
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit exceeded"},
                headers={"Retry-After": str(retry)},
            )
        q.append(now)
    return await call_next(request)


# Added after the rate limiter, so it is the outermost layer: preflights are answered
# before they can be counted, and 429 responses still carry CORS headers.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotent-Replayed"],
)


@app.post("/orders")
async def create_order(request: Request):
    key = request.headers.get("idempotency-key")
    if key and key in orders_by_key:
        return JSONResponse(
            status_code=200,
            content=orders_by_key[key],
            headers={"Idempotent-Replayed": "true"},
        )

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    order = {
        "id": f"ord_{uuid.uuid4().hex[:16]}",
        "status": "created",
        "created_at": time.time(),
        "payload": payload if isinstance(payload, dict) else {},
    }
    if key:
        orders_by_key[key] = order  # no await between check and store, so no race
    return JSONResponse(status_code=201, content=order)


def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"o": offset}).encode()).decode()


def decode_cursor(cursor: str) -> int:
    return int(json.loads(base64.urlsafe_b64decode(cursor.encode()))["o"])


@app.get("/orders")
async def list_orders(limit: int = 10, cursor: str | None = None):
    limit = max(1, min(limit, TOTAL))
    offset = 0
    if cursor:
        try:
            offset = decode_cursor(cursor)
            if not 0 <= offset <= TOTAL:
                raise ValueError
        except Exception:
            return JSONResponse(status_code=400, content={"detail": "invalid cursor"})

    ids = range(offset + 1, min(offset + limit, TOTAL) + 1)
    items = [{"id": i, "status": "fulfilled"} for i in ids]
    end = offset + len(items)
    return {"items": items, "next_cursor": encode_cursor(end) if end < TOTAL else None}
