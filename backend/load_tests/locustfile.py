"""Flash-sale traffic spike against the purchase endpoint.

Usage (with the API running and an event seeded):

    EVENT_ID=<uuid> STRATEGY=optimistic locust -f load_tests/locustfile.py \
        --headless -u 1000 -r 200 -t 60s -H http://localhost:8000

EVENT_ID  the event to buy tickets for (required)
STRATEGY  optimistic | redis_lock | queue (default: optimistic)

Every simulated buyer hammers the purchase endpoint with a fresh idempotency
key per attempt (worst case: nobody retries, everyone is a new buyer), with
an occasional event-page view mixed in. A sold-out 409 is a *correct* answer
under load and counts as success; only 5xx/timeouts count as failures.
"""

import os
import uuid

from locust import HttpUser, constant, task

EVENT_ID = os.environ.get("EVENT_ID", "")
STRATEGY = os.environ.get("STRATEGY", "optimistic")


class FlashSaleBuyer(HttpUser):
    wait_time = constant(0)  # a spike, not a browse: no think time

    def on_start(self) -> None:
        if not EVENT_ID:
            raise RuntimeError("Set EVENT_ID to the event under test")
        self.user_id = str(uuid.uuid4())

    @task(10)
    def purchase(self) -> None:
        with self.client.post(
            f"/events/{EVENT_ID}/purchase",
            json={
                "user_id": self.user_id,
                "idempotency_key": f"load-{uuid.uuid4()}",
                "strategy": STRATEGY,
            },
            name="/events/[id]/purchase",
            catch_response=True,
        ) as resp:
            # 201 bought, 202 queued, 409 sold out - all correct behavior.
            if resp.status_code in (201, 202, 409):
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:120]}")

    @task(1)
    def view_event(self) -> None:
        self.client.get(f"/events/{EVENT_ID}", name="/events/[id]")
