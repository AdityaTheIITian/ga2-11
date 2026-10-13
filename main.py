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
    if key and key in
