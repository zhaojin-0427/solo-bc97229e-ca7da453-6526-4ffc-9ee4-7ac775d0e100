"""Subscription consumption API: idempotent delivery submission + lease-based claiming.

Flow
----
1. Producer ``POST /subscriptions/deliveries`` with a delivery id and the
   existing :class:`NormalizeRequest`. Normalization runs first; the complete
   result (and request) is persisted as one batch **only when every record is
   UTC-determined**. Batch numbers increase monotonically per commit.
2. A warehouse task registers as a subscriber, then ``POST /subscriptions/claim``
   to receive the earliest whole batch it has not confirmed, with a unique lease
   id and a 30-second lease. While the lease is live, further claims of that
   batch for the subscriber are rejected.
3. ``POST /subscriptions/confirm`` with a valid lease id advances the
   subscriber's progress exactly once. Expired/superseded lease ids are refused
   (410) and cannot move progress. Repeated confirmation returns the original
   receipt.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated

from fastapi import APIRouter, Body

from . import solver, storage
from .schemas import (
    ErrorResponse,
    InvalidRecordInfo,
    LinkInfo,
    NormalizeResponse,
    RecordOut,
    Summary,
)
from .subs_schemas import (
    ApiError,
    ClaimRequest,
    ClaimResponse,
    ConfirmRequest,
    DeliveryBatchResponse,
    DeliverySubmitRequest,
    ReceiptResponse,
    SubscriberRegisterRequest,
    SubscriberRegistered,
    SubscriberState,
)

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])

# Example payloads for the interactive docs (all records UTC-determined, so they
# pass the "only all-determined results are saved" gate).
DEMO_DELIVERY_1 = {
    "delivery_id": "AWB-DEMO-0001",
    "request": {
        "records": [
            {"id": "DEP_PVG", "local_time": "2024-11-03T09:00:00", "timezone": "Asia/Shanghai"},
            {"id": "ARR_ANC", "local_time": "2024-11-03T01:30:00", "timezone": "America/Anchorage"},
            {"id": "DEP_ANC", "local_time": "2024-11-03T03:10:00", "timezone": "America/Anchorage"},
        ],
        # Anchorage 01:30 is a DST fall-back duplicate (09:30Z/10:30Z). The 9-11h
        # flight window from Shanghai (01:00Z) and the 1-2h transfer window to
        # DEP_ANC (12:10Z) jointly force 10:30Z, so every record is determined.
        "links": [
            {"from_id": "DEP_PVG", "to_id": "ARR_ANC", "min_seconds": 32400, "max_seconds": 39600},
            {"from_id": "ARR_ANC", "to_id": "DEP_ANC", "min_seconds": 3600, "max_seconds": 7200},
        ],
    },
}

DEMO_DELIVERY_2 = {
    "delivery_id": "AWB-DEMO-0002",
    "request": {
        "records": [
            {"id": "DEP_FRA", "local_time": "2024-06-01T04:00:00", "timezone": "Europe/Berlin"},
            {"id": "ARR_JFK", "local_time": "2024-06-01T07:30:00", "timezone": "America/New_York"},
        ],
        # June, no DST transition: both fixed at 02:00Z / 11:30Z (9h30m flight).
        "links": [
            {"from_id": "DEP_FRA", "to_id": "ARR_JFK", "min_seconds": 32400, "max_seconds": 36000},
        ],
    },
}


def canonical_request_json(req_model) -> str:
    """Deterministic serialization of a normalize request (idempotency key input)."""
    return json.dumps(
        req_model.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def content_hash_of(canonical_json: str) -> str:
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def run_normalization(req_model) -> NormalizeResponse:
    """Run the solver and map its outcome to the existing response model.

    Raises FastAPI-style 409 responses (via the storage-independent
    :class:`NormalizationRejected`) for non-determined content.
    """
    outcome = solver.normalize(
        records=[solver.ScanInput(r.id, r.local_time, r.timezone) for r in req_model.records],
        links=[
            solver.LinkInput(l.from_id, l.to_id, l.min_seconds, l.max_seconds)
            for l in req_model.links
        ],
    )

    if isinstance(outcome, solver.NoLegalCandidate):
        body = ErrorResponse(
            error="no_legal_candidate",
            message="Local time has no legal UTC reading under its timezone rules "
                    "(it falls into a DST spring-forward gap). No batch was saved.",
            invalid_records=[
                InvalidRecordInfo(
                    id=r.id, local_time=r.local_time, timezone=r.timezone,
                    reason="local time does not exist in this timezone",
                )
                for r in outcome.records
            ],
        )
        raise NormalizationRejected(body.model_dump(mode="json"))

    if isinstance(outcome, solver.NoGlobalSolution):
        body = ErrorResponse(
            error="no_global_solution",
            message="Transfer links are mutually inconsistent. No batch was saved.",
            conflicting_links=[
                LinkInfo(
                    index=i,
                    from_id=req_model.links[i].from_id,
                    to_id=req_model.links[i].to_id,
                    min_seconds=req_model.links[i].min_seconds,
                    max_seconds=req_model.links[i].max_seconds,
                )
                for i in outcome.link_indices
            ],
            involved_records=outcome.record_ids,
        )
        raise NormalizationRejected(body.model_dump(mode="json"))

    by_id = {r.id: r for r in req_model.records}
    records_out = [
        RecordOut(
            id=res.id,
            timezone=by_id[res.id].timezone,
            local_time=by_id[res.id].local_time,
            status=res.status,
            utc=res.candidates[0] if res.status == "determined" else None,
            candidates=res.candidates,
        )
        for res in outcome.results
    ]
    response = NormalizeResponse(
        summary=Summary(
            total=len(records_out),
            determined=sum(1 for r in records_out if r.status == "determined"),
            needs_review=sum(1 for r in records_out if r.status == "needs_review"),
        ),
        records=records_out,
    )

    if any(r.status != "determined" for r in records_out):
        pending = [r.id for r in records_out if r.status == "needs_review"]
        raise NormalizationRejected(
            {
                "error": "not_all_utc_determined",
                "message": "At least one scan record remains needs_review (待核对): the "
                           "delivery is not fully UTC-determined, so the complete result is "
                           "not saved and no batch number is assigned.",
                "pending_records": pending,
                "normalization": response.model_dump(mode="json"),
            }
        )
    return response


class NormalizationRejected(Exception):
    """Carries a ready-to-send 409 body for undetermined normalization."""

    def __init__(self, body: dict) -> None:
        super().__init__(body.get("message", "normalization rejected"))
        self.body = body


# ---------------------------------------------------------------------------
# endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/deliveries",
    response_model=DeliveryBatchResponse,
    status_code=201,
    responses={
        409: {"model": ApiError, "description": "Content conflict, or normalization not fully UTC-determined"},
    },
    summary="Submit a delivery batch (idempotent on delivery_id + content)",
)
def submit_delivery_endpoint(
    payload: Annotated[
        DeliverySubmitRequest,
        Body(
            openapi_examples={
                "delivery_1_shanghai_anchorage": {
                    "summary": "Fully determined delivery AWB-DEMO-0001",
                    "value": DEMO_DELIVERY_1,
                },
                "delivery_2_frankfurt_newyork": {
                    "summary": "Fully determined delivery AWB-DEMO-0002",
                    "value": DEMO_DELIVERY_2,
                },
            }
        ),
    ],
) -> DeliveryBatchResponse:
    response = run_normalization(payload.request)  # raises 409 unless all UTC-determined
    canonical = canonical_request_json(payload.request)
    stored = storage.submit_delivery(
        delivery_id=payload.delivery_id,
        content_hash=content_hash_of(canonical),
        request_json=canonical,
        result_json=json.dumps(response.model_dump(mode="json"), sort_keys=True, ensure_ascii=False),
    )
    return DeliveryBatchResponse(**stored)


@router.post(
    "/subscribers",
    response_model=SubscriberRegistered,
    status_code=201,
    summary="Register a warehouse task as a subscriber",
)
def register_subscriber_endpoint(payload: SubscriberRegisterRequest) -> SubscriberRegistered:
    return SubscriberRegistered(**storage.register_subscriber(payload.subscriber_id))


@router.post(
    "/claim",
    response_model=ClaimResponse,
    responses={
        404: {"model": ApiError, "description": "Unknown subscriber or no unconfirmed batch"},
        409: {"model": ApiError, "description": "Batch already held by an active lease"},
    },
    summary="Claim the earliest unconfirmed whole batch (30s lease)",
)
def claim_endpoint(payload: ClaimRequest) -> ClaimResponse:
    return ClaimResponse(**storage.claim_batch(payload.subscriber_id))


@router.post(
    "/confirm",
    response_model=ReceiptResponse,
    responses={
        404: {"model": ApiError, "description": "Unknown lease id"},
        403: {"model": ApiError, "description": "Lease belongs to another subscriber"},
        410: {"model": ApiError, "description": "Lease expired/superseded; old id cannot advance progress"},
    },
    summary="Confirm a valid claim and advance this subscriber's progress",
)
def confirm_endpoint(payload: ConfirmRequest) -> ReceiptResponse:
    return ReceiptResponse(**storage.confirm_lease(payload.lease_id, payload.subscriber_id))


@router.get(
    "/subscribers/{subscriber_id}",
    response_model=SubscriberState,
    responses={404: {"model": ApiError}},
    summary="Inspect a subscriber's progress and live leases",
)
def subscriber_state_endpoint(subscriber_id: str) -> SubscriberState:
    return SubscriberState(**storage.get_subscriber_state(subscriber_id))
