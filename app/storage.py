"""SQLite-backed persistence for delivery batches, subscribers, leases, receipts.

Concurrency model
-----------------
Every state-changing operation runs inside a single ``BEGIN IMMEDIATE``
transaction, so concurrent claims/confirms serialize at the database lock and
the "no skipped batch, no double lease" invariants hold even with parallel
workers. Lease expiry is computed against ``unixepoch()`` in SQL, so the logic
is identical before and after a process restart.

Durability
----------
WAL journaling plus ``synchronous=FULL``: a committed lease, progress move or
receipt survives process loss, and a response lost after commit can simply be
replayed by the client (all endpoints are idempotent). Expired/superseded
leases are kept as history rows so an old lease id is answered with 410 rather
than silently vanishing.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id      TEXT PRIMARY KEY,
    content_hash     TEXT NOT NULL,
    request_json     TEXT NOT NULL,
    result_json      TEXT NOT NULL,
    batch_seq        INTEGER NOT NULL UNIQUE,
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscribers (
    subscriber_id    TEXT PRIMARY KEY,
    created_at       TEXT NOT NULL,
    -- next batch_seq this subscriber must receive; NULL means nothing yet
    next_seq         INTEGER
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id         TEXT PRIMARY KEY,
    subscriber_id    TEXT NOT NULL,
    batch_seq        INTEGER NOT NULL,
    expires_at       INTEGER NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('active', 'confirmed', 'expired')),
    created_at       TEXT NOT NULL,
    confirmed_at     TEXT,
    FOREIGN KEY (subscriber_id) REFERENCES subscribers(subscriber_id),
    FOREIGN KEY (batch_seq) REFERENCES deliveries(batch_seq)
);

-- At most one live lease per (subscriber, batch); history rows are exempt.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_lease
    ON leases(subscriber_id, batch_seq) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS receipts (
    lease_id         TEXT PRIMARY KEY,
    subscriber_id    TEXT NOT NULL,
    batch_seq        INTEGER NOT NULL,
    delivery_id      TEXT NOT NULL,
    confirmed_at     TEXT NOT NULL,
    receipt_json     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_subscriber ON leases(subscriber_id, status);
CREATE INDEX IF NOT EXISTS idx_receipts_subscriber ON receipts(subscriber_id, batch_seq);
"""


def _connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def _tx() -> Iterator[sqlite3.Connection]:
    """Serialize writers: one BEGIN IMMEDIATE transaction per operation."""
    conn = _connect(settings.db_path())
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables on startup (idempotent)."""
    conn = _connect(settings.db_path())
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _epoch_to_iso(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# domain errors (mapped to HTTP responses in app/errors.py)
# ---------------------------------------------------------------------------


class StoreError(Exception):
    """Base class; carries an API error code and HTTP status."""

    code: str = "store_error"
    status: int = 409

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class DeliveryConflict(StoreError):
    code = "delivery_content_conflict"
    status = 409


class SubscriberNotFound(StoreError):
    code = "subscriber_not_found"
    status = 404


class BatchUnavailable(StoreError):
    code = "no_batch_available"
    status = 404


class LeaseConflict(StoreError):
    code = "lease_active"
    status = 409


class LeaseNotFound(StoreError):
    code = "lease_not_found"
    status = 404


class LeaseExpired(StoreError):
    code = "lease_expired"
    status = 410


class LeaseWrongSubscriber(StoreError):
    code = "lease_wrong_subscriber"
    status = 403


# ---------------------------------------------------------------------------
# delivery submission
# ---------------------------------------------------------------------------


def submit_delivery(
    delivery_id: str,
    content_hash: str,
    request_json: str,
    result_json: str,
) -> dict[str, Any]:
    """Insert a fully-UTC-determined delivery, or return the stored one.

    Same delivery id + same content -> original batch (idempotent).
    Same delivery id + different content -> 409, nothing is written.
    Batch sequence numbers are monotonically increasing, assigned in commit
    order from ``coalesce(max,0)+1``.
    """
    with _tx() as conn:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        if row is not None:
            if row["content_hash"] != content_hash:
                raise DeliveryConflict(
                    f"delivery_id {delivery_id!r} was already submitted with different "
                    "content; re-submission with changed content is rejected.",
                    details={"delivery_id": delivery_id, "stored_batch_seq": row["batch_seq"]},
                )
            out = _delivery_row_to_dict(row)
            out["replayed"] = True
            return out

        next_seq = conn.execute(
            "SELECT coalesce(max(batch_seq), 0) + 1 AS n FROM deliveries"
        ).fetchone()["n"]
        created = utcnow_iso()
        conn.execute(
            "INSERT INTO deliveries (delivery_id, content_hash, request_json, result_json, "
            "batch_seq, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (delivery_id, content_hash, request_json, result_json, next_seq, created),
        )
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        out = _delivery_row_to_dict(row)
        out["replayed"] = False
        return out


def get_delivery(delivery_id: str) -> dict[str, Any] | None:
    with _tx() as conn:
        row = conn.execute(
            "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        return _delivery_row_to_dict(row) if row else None


def _delivery_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "delivery_id": row["delivery_id"],
        "batch_seq": row["batch_seq"],
        "request": json.loads(row["request_json"]),
        "result": json.loads(row["result_json"], ),
        "created_at": row["created_at"],
    }


# ---------------------------------------------------------------------------
# subscribers
# ---------------------------------------------------------------------------


def register_subscriber(subscriber_id: str | None) -> dict[str, Any]:
    """Register a consumer; re-registering an existing id is idempotent."""
    with _tx() as conn:
        if subscriber_id is not None:
            row = conn.execute(
                "SELECT * FROM subscribers WHERE subscriber_id = ?", (subscriber_id,)
            ).fetchone()
            if row is not None:
                return {
                    "subscriber_id": row["subscriber_id"],
                    "created_at": row["created_at"],
                    "replayed": True,
                }
        sid = subscriber_id or _new_id("sub")
        created = utcnow_iso()
        conn.execute(
            "INSERT INTO subscribers (subscriber_id, created_at, next_seq) VALUES (?, ?, NULL)",
            (sid, created),
        )
        return {"subscriber_id": sid, "created_at": created, "replayed": False}


def get_subscriber_state(subscriber_id: str) -> dict[str, Any]:
    with _tx() as conn:
        sub = conn.execute(
            "SELECT * FROM subscribers WHERE subscriber_id = ?", (subscriber_id,)
        ).fetchone()
        if sub is None:
            raise SubscriberNotFound(f"unknown subscriber_id {subscriber_id!r}")
        active = conn.execute(
            "SELECT lease_id, batch_seq, expires_at, created_at FROM leases "
            "WHERE subscriber_id = ? AND status = 'active' AND expires_at > unixepoch()",
            (subscriber_id,),
        ).fetchall()
        confirmed_count = conn.execute(
            "SELECT count(*) AS n FROM leases WHERE subscriber_id = ? AND status = 'confirmed'",
            (subscriber_id,),
        ).fetchone()["n"]
        total_batches = conn.execute("SELECT count(*) AS n FROM deliveries").fetchone()["n"]
        return {
            "subscriber_id": subscriber_id,
            "next_seq": sub["next_seq"],
            "confirmed_batches": confirmed_count,
            "total_batches": total_batches,
            "active_leases": [
                {
                    "lease_id": r["lease_id"],
                    "batch_seq": r["batch_seq"],
                    "expires_at": _epoch_to_iso(r["expires_at"]),
                    "leased_at": r["created_at"],
                }
                for r in active
            ],
        }


# ---------------------------------------------------------------------------
# claiming
# ---------------------------------------------------------------------------


def claim_batch(subscriber_id: str) -> dict[str, Any]:
    """Hand the subscriber the earliest unconfirmed whole batch with a fresh lease.

    Rules (all enforced inside one IMMEDIATE transaction):
    * an unexpired lease on that batch blocks every other claim -> 409;
    * an expired lease is superseded: a new row + new lease id, the old id is
      marked ``expired`` and can never confirm;
    * no batch pending for this subscriber -> 404.
    """
    lease_ttl = settings.lease_seconds()
    with _tx() as conn:
        sub = conn.execute(
            "SELECT * FROM subscribers WHERE subscriber_id = ?", (subscriber_id,)
        ).fetchone()
        if sub is None:
            raise SubscriberNotFound(f"unknown subscriber_id {subscriber_id!r}")

        # Earliest batch this subscriber has not confirmed yet.
        target = conn.execute(
            "SELECT d.* FROM deliveries d WHERE d.batch_seq >= "
            "  coalesce((SELECT next_seq FROM subscribers WHERE subscriber_id = ?), 1) "
            "ORDER BY d.batch_seq LIMIT 1",
            (subscriber_id,),
        ).fetchone()
        if target is None:
            raise BatchUnavailable(
                "no unconfirmed batch available for this subscriber",
                details={"subscriber_id": subscriber_id},
            )
        seq = target["batch_seq"]
        now_epoch = conn.execute("SELECT unixepoch() AS t").fetchone()["t"]

        existing = conn.execute(
            "SELECT * FROM leases WHERE subscriber_id = ? AND batch_seq = ? "
            "AND status = 'active'",
            (subscriber_id, seq),
        ).fetchone()
        now_iso = utcnow_iso()
        if existing is not None and existing["expires_at"] > now_epoch:
            raise LeaseConflict(
                "an active lease already holds this batch for this subscriber; "
                "wait for expiry or confirm it",
                details={
                    "lease_id": existing["lease_id"],
                    "batch_seq": seq,
                    "expires_at": _epoch_to_iso(existing["expires_at"]),
                },
            )

        if existing is not None:
            # Timed-out active row: retire it, then issue a fresh lease id.
            conn.execute(
                "UPDATE leases SET status = 'expired' WHERE lease_id = ?",
                (existing["lease_id"],),
            )
        lease_id = _new_id("lease")
        conn.execute(
            "INSERT INTO leases (lease_id, subscriber_id, batch_seq, expires_at, "
            "status, created_at) VALUES (?, ?, ?, ?, 'active', ?)",
            (lease_id, subscriber_id, seq, now_epoch + lease_ttl, now_iso),
        )

        return {
            "lease_id": lease_id,
            "subscriber_id": subscriber_id,
            "batch_seq": seq,
            "delivery_id": target["delivery_id"],
            "request": json.loads(target["request_json"]),
            "result": json.loads(target["result_json"]),
            "leased_at": now_iso,
            "expires_at": _epoch_to_iso(now_epoch + lease_ttl),
            "lease_seconds": lease_ttl,
        }


# ---------------------------------------------------------------------------
# confirmation
# ---------------------------------------------------------------------------


def confirm_lease(lease_id: str, subscriber_id: str) -> dict[str, Any]:
    """Advance subscriber progress only for a currently valid lease id.

    * unknown id -> 404; id belonging to another subscriber -> 403;
    * superseded/timed-out id -> 410, progress is untouched;
    * replay of a successful confirm -> the original receipt, unchanged.
    Progress moves to ``batch_seq + 1`` in the same transaction that flips the
    lease, so a crash can neither skip a batch nor double-advance.
    """
    with _tx() as conn:
        lease = conn.execute(
            "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if lease is None:
            receipt = conn.execute(
                "SELECT receipt_json FROM receipts WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if receipt is not None:  # defensive: receipts share lease ids
                return json.loads(receipt["receipt_json"])
            raise LeaseNotFound(f"unknown lease_id {lease_id!r}")

        if lease["subscriber_id"] != subscriber_id:
            raise LeaseWrongSubscriber(
                "lease belongs to another subscriber",
                details={"lease_id": lease_id, "owner": lease["subscriber_id"]},
            )

        if lease["status"] == "confirmed":
            receipt = conn.execute(
                "SELECT receipt_json FROM receipts WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            return json.loads(receipt["receipt_json"])

        if lease["status"] == "expired" or lease["expires_at"] <= conn.execute(
            "SELECT unixepoch() AS t"
        ).fetchone()["t"]:
            raise LeaseExpired(
                "lease has expired; re-claim the batch to obtain a new lease id; "
                "the old lease id can no longer advance progress",
                details={"lease_id": lease_id, "batch_seq": lease["batch_seq"]},
            )

        confirmed_at = utcnow_iso()
        conn.execute(
            "UPDATE leases SET status = 'confirmed', confirmed_at = ? WHERE lease_id = ?",
            (confirmed_at, lease_id),
        )
        conn.execute(
            "UPDATE subscribers SET next_seq = ? WHERE subscriber_id = ?",
            (lease["batch_seq"] + 1, subscriber_id),
        )
        delivery = conn.execute(
            "SELECT delivery_id FROM deliveries WHERE batch_seq = ?",
            (lease["batch_seq"],),
        ).fetchone()
        receipt = {
            "receipt_id": _new_id("rcpt"),
            "lease_id": lease_id,
            "subscriber_id": subscriber_id,
            "batch_seq": lease["batch_seq"],
            "delivery_id": delivery["delivery_id"],
            "confirmed_at": confirmed_at,
        }
        conn.execute(
            "INSERT INTO receipts (lease_id, subscriber_id, batch_seq, delivery_id, "
            "confirmed_at, receipt_json) VALUES (?, ?, ?, ?, ?, ?)",
            (
                lease_id,
                subscriber_id,
                lease["batch_seq"],
                delivery["delivery_id"],
                confirmed_at,
                json.dumps(receipt),
            ),
        )
        return receipt
