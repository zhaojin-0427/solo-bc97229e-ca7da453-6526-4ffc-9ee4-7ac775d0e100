"""Request/response models for the subscription consumption API."""

from __future__ import annotations

from pydantic import BaseModel, Field

from .schemas import NormalizeRequest, NormalizeResponse


class DeliverySubmitRequest(BaseModel):
    """A producer submission: delivery id plus the existing normalize payload."""

    delivery_id: str = Field(
        min_length=1, max_length=128,
        examples=["AWB-77123456"],
        description="Stable delivery/waybill number used as the idempotency key.",
    )
    request: NormalizeRequest


class DeliveryBatchResponse(BaseModel):
    """The persisted batch: full normalization result plus its monotonic number."""

    delivery_id: str
    batch_seq: int = Field(description="Monotonic batch number, assigned in commit order (1, 2, 3 ...).")
    request: NormalizeRequest
    result: NormalizeResponse
    created_at: str
    replayed: bool = Field(description="True when an identical submission was already stored.")


class SubscriberRegisterRequest(BaseModel):
    subscriber_id: str | None = Field(
        default=None,
        min_length=1, max_length=128,
        description="Optional caller-chosen id; a generated one is returned when omitted.",
    )


class SubscriberRegistered(BaseModel):
    subscriber_id: str
    created_at: str
    replayed: bool


class ClaimRequest(BaseModel):
    subscriber_id: str = Field(min_length=1, max_length=128)


class ClaimResponse(BaseModel):
    lease_id: str = Field(description="Unique claim handle; required to confirm this batch.")
    subscriber_id: str
    batch_seq: int
    delivery_id: str
    request: NormalizeRequest
    result: NormalizeResponse
    leased_at: str
    expires_at: str = Field(description="UTC instant after which the lease may be re-claimed by anyone.")
    lease_seconds: int = Field(description="Lease duration in seconds (default 30).")


class ConfirmRequest(BaseModel):
    subscriber_id: str = Field(min_length=1, max_length=128)
    lease_id: str = Field(min_length=1, max_length=128)


class ReceiptResponse(BaseModel):
    receipt_id: str
    lease_id: str
    subscriber_id: str
    batch_seq: int
    delivery_id: str
    confirmed_at: str


class ActiveLeaseInfo(BaseModel):
    lease_id: str
    batch_seq: int
    leased_at: str
    expires_at: str


class SubscriberState(BaseModel):
    subscriber_id: str
    next_seq: int | None = Field(
        description="Earliest batch_seq not yet confirmed; NULL before the first claim."
    )
    confirmed_batches: int
    total_batches: int
    active_leases: list[ActiveLeaseInfo]


class ApiError(BaseModel):
    error: str
    message: str
    details: dict = Field(default_factory=dict)
