"""FastAPI entrypoint: scan-time normalization plus subscription consumption."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Body, FastAPI, Request
from fastapi.responses import JSONResponse

from . import storage
from .errors import register_exception_handlers
from .schemas import ErrorResponse, NormalizeRequest, NormalizeResponse
from .subscriptions import (
    NormalizationRejected,
    router as subscriptions_router,
    run_normalization,
)

DESCRIPTION = """
Normalize carrier-exported local scan times to UTC under IANA timezone rules,
and let multiple warehouse-ingestion tasks consume the confirmed scan batches
durable via a SQLite-backed subscription API.

## `/normalize` — one-shot normalization

* Each record's local time yields **0, 1 or 2 legal UTC candidates**
  (DST fall-back duplicates a local time; spring-forward makes one nonexistent).
* Transfer links constrain the UTC duration between two records to a closed
  interval; **all links must hold simultaneously** and links may share records.
* The service computes, per record, **every candidate that appears in at least
  one globally feasible interpretation**:
  * exactly one → `determined` (确定), returned as `utc`;
  * several → `needs_review` (待核对), all candidates returned, none picked.
* If any record has no legal candidate, or the links are mutually inconsistent,
  the request fails with **HTTP 409** — no partial conversion is ever returned.

## `/subscriptions` — durable batch consumption

* `POST /subscriptions/deliveries` submits a delivery number plus the same
  normalization request. The **complete result is saved and assigned a
  monotonically increasing batch number only when every record is
  UTC-determined**. Same delivery id + same content returns the original
  batch; changed content is rejected (409).
* Each registered subscriber starts from the first batch and always claims the
  **earliest unconfirmed whole batch**, receiving a unique lease id with a
  **30-second lease**. Concurrent claims of a leased batch are rejected; once
  the lease lapses the batch can be re-claimed with a **new** lease id, and the
  old id can no longer advance progress.
* Confirming a valid lease advances exactly that subscriber's progress;
  re-confirming returns the original receipt. Different subscribers never block
  each other, and batches, progress, leases and receipts all persist in SQLite —
  no batch is skipped across concurrent calls, lost responses or restarts.
"""

DEMO_DST_FALLBACK = {
    "records": [
        {"id": "ARR_JFK", "local_time": "2024-11-03T01:30:00", "timezone": "America/New_York"},
        {"id": "DEP_JFK", "local_time": "2024-11-03T02:45:00", "timezone": "America/New_York"},
        {"id": "ARR_ORD", "local_time": "2024-11-03T04:10:00", "timezone": "America/Chicago"},
        {"id": "DEP_LAX", "local_time": "2024-11-03T03:20:00", "timezone": "America/Los_Angeles"},
        {"id": "XFR_ORD", "local_time": "2024-11-03T01:30:00", "timezone": "America/Chicago"},
    ],
    "links": [
        {"from_id": "ARR_JFK", "to_id": "DEP_JFK", "min_seconds": 3600, "max_seconds": 7200},
        {"from_id": "DEP_JFK", "to_id": "ARR_ORD", "min_seconds": 7200, "max_seconds": 10800},
        {"from_id": "ARR_ORD", "to_id": "DEP_LAX", "min_seconds": 1800, "max_seconds": 7200},
        {"from_id": "ARR_JFK", "to_id": "XFR_ORD", "min_seconds": 0, "max_seconds": 86400},
    ],
}

DEMO_IMAGINARY_TIME = {
    "records": [
        {"id": "GAP_1", "local_time": "2024-03-10T02:30:00", "timezone": "America/New_York"},
    ],
    "links": [],
}

DEMO_CONFLICTING_LINKS = {
    "records": [
        {"id": "A", "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
        {"id": "B", "local_time": "2024-06-01T12:00:00", "timezone": "UTC"},
    ],
    "links": [
        {"from_id": "A", "to_id": "B", "min_seconds": 0, "max_seconds": 3600},
    ],
}

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    # Ensure SQLite tables exist before serving (idempotent on every start).
    storage.init_db()
    yield


app = FastAPI(
    title="Scan Time Normalizer & Batch Subscription API",
    description=DESCRIPTION,
    version="2.0.0",
    lifespan=_lifespan,
)
app.include_router(subscriptions_router)
register_exception_handlers(app)


@app.exception_handler(NormalizationRejected)
async def _normalization_rejected_handler(_request: Request, exc: NormalizationRejected) -> JSONResponse:
    return JSONResponse(status_code=409, content=exc.body)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "service": "scan-time-normalizer",
        "docs": "/docs",
        "normalize": "POST /normalize",
        "submit_batch": "POST /subscriptions/deliveries",
        "register_subscriber": "POST /subscriptions/subscribers",
        "claim": "POST /subscriptions/claim",
        "confirm": "POST /subscriptions/confirm",
        "health": "GET /healthz",
    }


@app.get("/healthz", tags=["meta"])
def healthz() -> dict:
    return {"status": "up"}


@app.post(
    "/normalize",
    response_model=NormalizeResponse,
    responses={409: {"model": ErrorResponse, "description": "No legal candidate or no global solution"}},
    tags=["normalize"],
)
def normalize_endpoint(
    req: Annotated[
        NormalizeRequest,
        Body(
            openapi_examples={
                "dst_fallback_resolved": {
                    "summary": "DST fall-back: constraints disambiguate, one record stays 待核对",
                    "description": "2024-11-03 US fall-back. ARR_JFK 01:30 New York is ambiguous "
                                   "(05:30Z/06:30Z); the 1-2h transfer window to DEP_JFK forces "
                                   "06:30Z. XFR_ORD 01:30 Chicago keeps both candidates.",
                    "value": DEMO_DST_FALLBACK,
                },
                "imaginary_local_time": {
                    "summary": "Spring-forward gap: local time does not exist → 409",
                    "description": "2024-03-10 02:30 never happened in America/New_York, so the "
                                   "record has no legal UTC candidate.",
                    "value": DEMO_IMAGINARY_TIME,
                },
                "conflicting_links": {
                    "summary": "Mutually inconsistent links → 409 with irreducible conflict",
                    "description": "A→B is fixed at 7200s but the link allows at most 3600s.",
                    "value": DEMO_CONFLICTING_LINKS,
                },
            }
        ),
    ],
) -> NormalizeResponse:
    # The subscription gate (needs_review -> 409) only applies to batch
    # submission; /normalize keeps returning needs_review candidates as before.
    try:
        return run_normalization(req)
    except NormalizationRejected as exc:
        if exc.body.get("error") == "not_all_utc_determined":
            return NormalizeResponse(**exc.body["normalization"])
        raise
