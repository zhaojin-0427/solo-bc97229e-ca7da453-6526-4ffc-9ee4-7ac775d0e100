"""Tests for the subscription consumption API.

Covers: idempotent submission and content conflicts, the all-UTC-determined
gate, earliest-batch leasing with 30s TTL, expiry re-claim with new lease ids,
old-id rejection, idempotent receipts, no-skip progress, independent
subscribers, SQLite durability across a simulated restart, and concurrency.
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app import settings, storage
from app.main import app
from app.subscriptions import DEMO_DELIVERY_1, DEMO_DELIVERY_2

client = TestClient(app)


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Fresh SQLite file + short lease per test, independent of real env vars."""
    db_file = tmp_path / "subs.db"
    monkeypatch.setattr(settings, "db_path", lambda: str(db_file))
    monkeypatch.setattr(settings, "lease_seconds", lambda: 1)  # 1s TTL for expiry tests
    storage.init_db()
    yield


def _determined_delivery(delivery_id: str, record_id: str = "R1") -> dict:
    return {
        "delivery_id": delivery_id,
        "request": {
            "records": [
                {"id": record_id, "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
            ],
            "links": [],
        },
    }


def _register() -> str:
    return client.post("/subscriptions/subscribers", json={}).json()["subscriber_id"]


def _submit(delivery_id: str, record_id: str = "R1"):
    return client.post(
        "/subscriptions/deliveries", json=_determined_delivery(delivery_id, record_id)
    )


# ---------------------------------------------------------------------------
# submission: numbering, idempotency, conflict, all-UTC gate
# ---------------------------------------------------------------------------


def test_batches_get_monotonic_sequence_numbers():
    r1 = _submit("AWB-1")
    r2 = _submit("AWB-2")
    r3 = _submit("AWB-3")
    assert (r1.status_code, r1.json()["batch_seq"]) == (201, 1)
    assert (r2.status_code, r2.json()["batch_seq"]) == (201, 2)
    assert (r3.status_code, r3.json()["batch_seq"]) == (201, 3)
    # the full normalization result is stored alongside the request
    assert r1.json()["result"]["records"][0]["utc"] == "2024-06-01T10:00:00Z"
    assert r1.json()["request"]["records"][0]["id"] == "R1"


def test_same_delivery_same_content_returns_original_batch():
    first = _submit("AWB-DUP")
    again = _submit("AWB-DUP")
    assert first.status_code == 201 and again.status_code == 201
    assert again.json()["replayed"] is True
    assert first.json()["replayed"] is False
    assert again.json()["batch_seq"] == first.json()["batch_seq"] == 1
    # no extra batch was allocated
    assert _submit("AWB-OTHER").json()["batch_seq"] == 2


def test_same_delivery_different_content_is_rejected_and_not_saved():
    _submit("AWB-X")
    changed = _determined_delivery("AWB-X")
    changed["request"]["records"][0]["local_time"] = "2024-06-01T11:00:00"
    resp = client.post("/subscriptions/deliveries", json=changed)
    assert resp.status_code == 409
    assert resp.json()["error"] == "delivery_content_conflict"
    assert resp.json()["details"]["stored_batch_seq"] == 1

    # original content still replays the original batch untouched
    replay = _submit("AWB-X")
    assert replay.json()["replayed"] is True
    assert replay.json()["result"]["records"][0]["utc"] == "2024-06-01T10:00:00Z"


def test_needs_review_delivery_is_not_persisted_and_gets_no_sequence():
    payload = {"delivery_id": "AWB-REVIEW", "request": {
        "records": [
            {"id": "X", "local_time": "2024-11-03T01:30:00", "timezone": "America/New_York"},
        ],
        "links": [],
    }}
    resp = client.post("/subscriptions/deliveries", json=payload)
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "not_all_utc_determined"
    assert body["pending_records"] == ["X"]

    # a later valid delivery still gets sequence 1 — nothing was saved
    assert _submit("AWB-OK").json()["batch_seq"] == 1


def test_no_legal_candidate_delivery_rejected():
    payload = {"delivery_id": "AWB-GAP", "request": {
        "records": [
            {"id": "GAP", "local_time": "2024-03-10T02:30:00", "timezone": "America/New_York"},
        ],
        "links": [],
    }}
    resp = client.post("/subscriptions/deliveries", json=payload)
    assert resp.status_code == 409
    assert resp.json()["error"] == "no_legal_candidate"


def test_demo_payloads_are_fully_determined():
    for demo in (DEMO_DELIVERY_1, DEMO_DELIVERY_2):
        resp = client.post("/subscriptions/deliveries", json=demo)
        assert resp.status_code == 201, resp.text
        assert resp.json()["result"]["summary"]["needs_review"] == 0


# ---------------------------------------------------------------------------
# claiming: earliest batch, lease exclusivity, errors
# ---------------------------------------------------------------------------


def test_claim_starts_from_first_batch_and_walks_in_order():
    _submit("AWB-1")
    _submit("AWB-2")
    sid = _register()

    c1 = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert c1.status_code == 200
    assert c1.json()["batch_seq"] == 1
    assert c1.json()["lease_seconds"] == 1  # test fixture TTL
    assert c1.json()["lease_id"].startswith("lease_")

    client.post("/subscriptions/confirm",
                json={"subscriber_id": sid, "lease_id": c1.json()["lease_id"]})
    c2 = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert c2.json()["batch_seq"] == 2


def test_claim_with_no_batches_returns_404():
    sid = _register()
    resp = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert resp.status_code == 404
    assert resp.json()["error"] == "no_batch_available"


def test_unknown_subscriber_claim_returns_404():
    _submit("AWB-1")
    resp = client.post("/subscriptions/claim", json={"subscriber_id": "sub_ghost"})
    assert resp.status_code == 404
    assert resp.json()["error"] == "subscriber_not_found"


def test_active_lease_blocks_second_claim():
    _submit("AWB-1")
    sid = _register()
    first = client.post("/subscriptions/claim", json={"subscriber_id": sid}).json()
    blocked = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert blocked.status_code == 409
    assert blocked.json()["error"] == "lease_active"
    assert blocked.json()["details"]["lease_id"] == first["lease_id"]


def test_expired_lease_allows_reclaim_with_new_id_and_old_id_cannot_confirm():
    _submit("AWB-1")
    sid = _register()
    old = client.post("/subscriptions/claim", json={"subscriber_id": sid}).json()

    # wait out the 1s test lease
    _wait_for_expiry(old["expires_at"])

    reclaimed = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert reclaimed.status_code == 200
    new_lease = reclaimed.json()
    assert new_lease["lease_id"] != old["lease_id"]
    assert new_lease["batch_seq"] == 1

    # the old, superseded lease id can never advance progress
    stale = client.post(
        "/subscriptions/confirm",
        json={"subscriber_id": sid, "lease_id": old["lease_id"]},
    )
    assert stale.status_code == 410
    assert stale.json()["error"] == "lease_expired"

    state = client.get(f"/subscriptions/subscribers/{sid}").json()
    assert state["next_seq"] is None  # nothing confirmed yet — no skip

    # the fresh id confirms and advances exactly one step
    ok = client.post(
        "/subscriptions/confirm",
        json={"subscriber_id": sid, "lease_id": new_lease["lease_id"]},
    )
    assert ok.status_code == 200
    assert ok.json()["batch_seq"] == 1
    assert client.get(f"/subscriptions/subscribers/{sid}").json()["next_seq"] == 2


def test_confirmation_requires_a_currently_valid_lease():
    _submit("AWB-1")
    sid = _register()
    lease = client.post("/subscriptions/claim", json={"subscriber_id": sid}).json()

    # unknown lease id
    r = client.post("/subscriptions/confirm",
                    json={"subscriber_id": sid, "lease_id": "lease_unknown"})
    assert r.status_code == 404 and r.json()["error"] == "lease_not_found"

    # another subscriber cannot confirm someone else's lease
    other = _register()
    r = client.post("/subscriptions/confirm",
                    json={"subscriber_id": other, "lease_id": lease["lease_id"]})
    assert r.status_code == 403 and r.json()["error"] == "lease_wrong_subscriber"

    # owner succeeds
    r = client.post("/subscriptions/confirm",
                    json={"subscriber_id": sid, "lease_id": lease["lease_id"]})
    assert r.status_code == 200


def test_repeated_confirmation_returns_original_receipt():
    _submit("AWB-1")
    sid = _register()
    lease = client.post("/subscriptions/claim", json={"subscriber_id": sid}).json()
    first = client.post(
        "/subscriptions/confirm",
        json={"subscriber_id": sid, "lease_id": lease["lease_id"]},
    ).json()
    second = client.post(
        "/subscriptions/confirm",
        json={"subscriber_id": sid, "lease_id": lease["lease_id"]},
    ).json()
    assert second == first  # same receipt_id, lease_id, confirmed_at


def test_no_batch_is_skipped_after_confirmation():
    for i in range(1, 4):
        _submit(f"AWB-{i}")
    sid = _register()
    for expected_seq in (1, 2, 3):
        lease = client.post("/subscriptions/claim", json={"subscriber_id": sid}).json()
        assert lease["batch_seq"] == expected_seq
        client.post("/subscriptions/confirm",
                    json={"subscriber_id": sid, "lease_id": lease["lease_id"]})
    # everything confirmed -> nothing left
    done = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert done.status_code == 404
    state = client.get(f"/subscriptions/subscribers/{sid}").json()
    assert state["confirmed_batches"] == 3
    assert state["next_seq"] == 4


def test_subscribers_are_independent_and_do_not_block_each_other():
    _submit("AWB-1")
    _submit("AWB-2")
    a, b = _register(), _register()

    lease_a = client.post("/subscriptions/claim", json={"subscriber_id": a}).json()
    # B holds the same batch concurrently with its own lease — no blocking
    lease_b = client.post("/subscriptions/claim", json={"subscriber_id": b})
    assert lease_b.status_code == 200
    assert lease_b.json()["batch_seq"] == 1
    assert lease_b.json()["lease_id"] != lease_a["lease_id"]

    # A progresses; B's position is unaffected
    client.post("/subscriptions/confirm",
                json={"subscriber_id": a, "lease_id": lease_a["lease_id"]})
    assert client.post(
        "/subscriptions/claim", json={"subscriber_id": a}
    ).json()["batch_seq"] == 2
    state_b = client.get(f"/subscriptions/subscribers/{b}").json()
    assert state_b["next_seq"] is None
    assert len(state_b["active_leases"]) == 1


def test_subscriber_registration_is_idempotent():
    r1 = client.post("/subscriptions/subscribers", json={"subscriber_id": "wh-task-7"})
    r2 = client.post("/subscriptions/subscribers", json={"subscriber_id": "wh-task-7"})
    assert r1.json()["replayed"] is False
    assert r2.json()["replayed"] is True
    assert r1.json()["subscriber_id"] == r2.json()["subscriber_id"] == "wh-task-7"


# ---------------------------------------------------------------------------
# durability across a simulated restart + concurrency
# ---------------------------------------------------------------------------


def test_state_survives_restart(tmp_path, monkeypatch):
    db = str(tmp_path / "restart.db")
    monkeypatch.setattr(settings, "db_path", lambda: db)
    monkeypatch.setattr(settings, "lease_seconds", lambda: 30)
    storage.init_db()

    _submit("AWB-PERSIST-1")
    _submit("AWB-PERSIST-2")
    sid = client.post("/subscriptions/subscribers",
                      json={"subscriber_id": "persistent-task"}).json()["subscriber_id"]
    lease = client.post("/subscriptions/claim", json={"subscriber_id": sid}).json()
    client.post("/subscriptions/confirm",
                json={"subscriber_id": sid, "lease_id": lease["lease_id"]})

    # simulate restart: throw away every connection and reinitialize on the file
    storage.init_db()

    state = client.get(f"/subscriptions/subscribers/{sid}").json()
    assert state["next_seq"] == 2
    assert state["confirmed_batches"] == 1
    nxt = client.post("/subscriptions/claim", json={"subscriber_id": sid})
    assert nxt.json()["batch_seq"] == 2
    assert nxt.json()["delivery_id"] == "AWB-PERSIST-2"
    # replay of the pre-restart confirmation still returns its receipt
    replay = client.post(
        "/subscriptions/confirm",
        json={"subscriber_id": sid, "lease_id": lease["lease_id"]},
    )
    assert replay.status_code == 200
    assert replay.json()["batch_seq"] == 1


def test_concurrent_claims_never_hand_out_two_active_leases():
    _submit("AWB-CONC")
    sid = _register()
    results: list = []

    def claim(_i):
        r = client.post("/subscriptions/claim", json={"subscriber_id": sid})
        results.append((r.status_code, r.json()))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(claim, range(8)))

    ok = [r for status, r in results if status == 200]
    blocked = [status for status, _ in results if status == 409]
    assert len(ok) == 1
    assert len(blocked) == 7
    assert len({r["lease_id"] for r in ok}) == 1


def test_concurrent_confirmations_advance_progress_once():
    for i in range(1, 4):
        _submit(f"AWB-CC-{i}")
    sid = _register()

    def cycle(_i):
        r = client.post("/subscriptions/claim", json={"subscriber_id": sid})
        if r.status_code != 200:
            return None
        return client.post(
            "/subscriptions/confirm",
            json={"subscriber_id": sid, "lease_id": r.json()["lease_id"]},
        ).status_code

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(cycle, range(6)))

    state = client.get(f"/subscriptions/subscribers/{sid}").json()
    # exactly three distinct batches confirmed, next_seq moved exactly to 4
    assert state["confirmed_batches"] == 3
    assert state["next_seq"] == 4


def test_concurrent_submissions_get_distinct_sequence_numbers():
    def submit(i):
        return _submit(f"AWB-PARA-{i}", record_id=f"R{i}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(submit, range(10)))

    seqs = sorted(r.json()["batch_seq"] for r in responses)
    assert seqs == list(range(1, 11))
    assert len(set(seqs)) == 10


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _wait_for_expiry(expires_at_iso: str) -> None:
    import time
    from datetime import datetime, timezone

    deadline = datetime.fromisoformat(expires_at_iso.replace("Z", "+00:00"))
    while datetime.now(timezone.utc) <= deadline:
        time.sleep(0.02)
    time.sleep(0.05)
