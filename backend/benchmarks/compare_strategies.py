"""Benchmark all three concurrency strategies under identical load.

Fires BUYERS concurrent purchase attempts at a fresh TICKETS-ticket event per
strategy (in-process, so we measure the strategies themselves, not HTTP
overhead), then writes a comparison table (CSV + Markdown) and charts (PNG)
into data/results/.

Usage (from backend/, with postgres + redis up):

    python benchmarks/compare_strategies.py
"""

import csv
import statistics
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import delete, func, select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.redis_client import get_redis
from app.db.session import SessionLocal
from app.models import Event, Reservation, Ticket
from app.strategies import optimistic_locking, queue_waiting_room, redis_distributed_lock

TICKETS = 100
BUYERS = 500
RESULTS_DIR = Path(__file__).resolve().parents[2] / "data" / "results"

# Correctness benchmark: with 100 winners serializing on one lock, the last
# winner legitimately waits longer than the production 2s acquire timeout.
# The cost shows up honestly in the latency distribution instead.
redis_distributed_lock.LOCK_ACQUIRE_TIMEOUT_SECONDS = 30.0

# ── palette (validated light-mode values from the dataviz reference palette) ──
SURFACE = "#fcfcfb"
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, BASELINE = "#e1e0d9", "#c3c2b7"
ORDINAL = ["#86b6ef", "#2a78d6", "#104281"]  # p50 → p95 → p99 (light→dark)
BLUE = "#2a78d6"


@dataclass
class BenchResult:
    name: str
    latencies_ms: list[float]  # what the buyer waited for a definitive answer
    successes: int
    sold_out: int
    errors: int
    wall_seconds: float
    note: str = ""
    extra: dict = field(default_factory=dict)

    def pct(self, q: float) -> float:
        return float(np.percentile(self.latencies_ms, q))

    @property
    def throughput(self) -> float:
        return len(self.latencies_ms) / self.wall_seconds


def make_event() -> uuid.UUID:
    with SessionLocal() as db:
        event = Event(
            name=f"bench-{uuid.uuid4()}",
            total_tickets=TICKETS,
            tickets_remaining=TICKETS,
            sale_start_time=datetime.now(timezone.utc),
        )
        db.add(event)
        db.flush()
        db.add_all(Ticket(event_id=event.id) for _ in range(TICKETS))
        db.commit()
        return event.id


def destroy_event(event_id: uuid.UUID) -> None:
    with SessionLocal() as db:
        ticket_ids = db.scalars(select(Ticket.id).where(Ticket.event_id == event_id)).all()
        db.execute(delete(Reservation).where(Reservation.ticket_id.in_(ticket_ids)))
        db.execute(delete(Ticket).where(Ticket.event_id == event_id))
        db.execute(delete(Event).where(Event.id == event_id))
        db.commit()
    r = get_redis()
    keys = r.keys(f"event:{event_id}:*") + r.keys("purchase:result:*")
    if keys:
        r.delete(*keys)


def verify_exactly_sold_out(event_id: uuid.UUID) -> int:
    """Recount from Postgres - the benchmark asserts correctness too."""
    with SessionLocal() as db:
        remaining = db.scalar(select(Event.tickets_remaining).where(Event.id == event_id))
        reservations = db.scalar(
            select(func.count())
            .select_from(Reservation)
            .join(Ticket, Reservation.ticket_id == Ticket.id)
            .where(Ticket.event_id == event_id)
        )
    assert remaining == 0, f"tickets_remaining={remaining}, expected 0"
    assert reservations == TICKETS, f"{reservations} reservations, expected {TICKETS}"
    return reservations


def bench_direct(name: str, attempt) -> BenchResult:
    event_id = make_event()
    latencies = [0.0] * BUYERS
    statuses: list[str] = [""] * BUYERS

    def buy(i: int) -> None:
        t0 = time.perf_counter()
        result = attempt(event_id, uuid.uuid4(), f"bench-{uuid.uuid4()}")
        latencies[i] = (time.perf_counter() - t0) * 1000
        statuses[i] = result.status.value

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=BUYERS) as pool:
        list(pool.map(buy, range(BUYERS)))
    wall = time.perf_counter() - t0

    reservations = verify_exactly_sold_out(event_id)
    result = BenchResult(
        name=name,
        latencies_ms=latencies,
        successes=statuses.count("success"),
        sold_out=statuses.count("sold_out"),
        errors=BUYERS - statuses.count("success") - statuses.count("sold_out"),
        wall_seconds=wall,
        extra={"reservations_in_db": reservations},
    )
    destroy_event(event_id)
    return result


def bench_queue() -> tuple[BenchResult, BenchResult]:
    """Queue semantics: buyers get an instant 'queued' ack; the worker resolves
    them at its own pace. Measure both the ack and the time-to-resolution."""
    event_id = make_event()
    keys = [f"bench-q-{uuid.uuid4()}" for _ in range(BUYERS)]
    ack_ms = [0.0] * BUYERS
    enqueued_at = [0.0] * BUYERS
    served_at: dict[str, float] = {}
    enqueue_done = threading.Event()

    def worker() -> None:  # drains concurrently, like the lifespan worker
        while True:
            key = queue_waiting_room.process_one(event_id)
            if key is not None:
                served_at[key] = time.perf_counter()
            elif enqueue_done.is_set() and queue_waiting_room.queue_length(event_id) == 0:
                return
            else:
                time.sleep(0.001)

    def join(i: int) -> None:
        t0 = time.perf_counter()
        enqueued_at[i] = t0
        queue_waiting_room.attempt_purchase(event_id, uuid.uuid4(), keys[i])
        ack_ms[i] = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    worker_thread = threading.Thread(target=worker)
    worker_thread.start()
    with ThreadPoolExecutor(max_workers=BUYERS) as pool:
        list(pool.map(join, range(BUYERS)))
    enqueue_wall = time.perf_counter() - t0  # all acks are delivered by now
    enqueue_done.set()
    worker_thread.join()
    wall = time.perf_counter() - t0

    outcomes = [queue_waiting_room.fetch_result(k) for k in keys]
    successes = sum(1 for o in outcomes if o and o.succeeded)
    resolution_ms = [(served_at[keys[i]] - enqueued_at[i]) * 1000 for i in range(BUYERS)]
    reservations = verify_exactly_sold_out(event_id)

    ack = BenchResult(
        name="queue (ack)",
        latencies_ms=ack_ms,
        successes=successes,
        sold_out=BUYERS - successes,
        errors=sum(1 for o in outcomes if o is None),
        wall_seconds=enqueue_wall,
        note="instant 'you are in line' response",
        extra={"reservations_in_db": reservations},
    )
    resolved = BenchResult(
        name="queue (resolved)",
        latencies_ms=resolution_ms,
        successes=successes,
        sold_out=BUYERS - successes,
        errors=0,
        wall_seconds=wall,
        note="enqueue -> final answer via worker",
        extra={"reservations_in_db": reservations},
    )
    destroy_event(event_id)
    return ack, resolved


# ── output: table ────────────────────────────────────────────────────────────

COLUMNS = ["strategy", "p50_ms", "p95_ms", "p99_ms", "success", "sold_out",
           "errors", "throughput_rps", "wall_s", "reservations_in_db"]


def row(r: BenchResult) -> list:
    return [r.name, round(r.pct(50), 1), round(r.pct(95), 1), round(r.pct(99), 1),
            r.successes, r.sold_out, r.errors, round(r.throughput, 1),
            round(r.wall_seconds, 2), r.extra.get("reservations_in_db", "")]


def write_table(results: list[BenchResult]) -> None:
    with open(RESULTS_DIR / "benchmark_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        writer.writerows(row(r) for r in results)

    lines = [
        f"| {' | '.join(COLUMNS)} |",
        f"|{'---|' * len(COLUMNS)}",
        *(f"| {' | '.join(str(v) for v in row(r))} |" for r in results),
    ]
    (RESULTS_DIR / "benchmark_results.md").write_text("\n".join(lines) + "\n")


# ── output: charts ───────────────────────────────────────────────────────────

def _style(ax) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(BASELINE)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _label_bars(ax, bars, fmt) -> None:
    for bar in bars:
        ax.annotate(
            fmt(bar.get_height()),
            (bar.get_x() + bar.get_width() / 2, bar.get_height()),
            ha="center", va="bottom", fontsize=8, color=INK_2,
            xytext=(0, 2), textcoords="offset points",
        )


def _fmt_ms(v: float) -> str:
    return f"{v:,.0f}" if v >= 10 else f"{v:.1f}"


def chart_latency(results: list[BenchResult], path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)

    x = np.arange(len(results))
    width = 0.24
    for offset, (q, color) in enumerate(zip((50, 95, 99), ORDINAL)):
        bars = ax.bar(
            x + (offset - 1) * width,
            [r.pct(q) for r in results],
            width - 0.02,  # 2px-ish gap between adjacent bars
            color=color, label=f"p{q}", zorder=3,
        )
        _label_bars(ax, bars, _fmt_ms)

    ax.set_xticks(x, [r.name for r in results], color=INK)
    ax.set_ylabel("latency (ms)", color=INK_2, fontsize=9)
    ax.set_title(title, color=INK, fontsize=11, loc="left", pad=12)
    ax.legend(frameon=False, labelcolor=INK_2, fontsize=9)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def chart_throughput(results: list[BenchResult], path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.4, 3.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)

    x = np.arange(len(results))
    bars = ax.bar(x, [r.throughput for r in results], 0.5, color=BLUE, zorder=3)
    _label_bars(ax, bars, lambda v: f"{v:,.0f}")

    ax.set_xticks(x, [r.name for r in results], color=INK)
    ax.set_ylabel("definitive answers / second", color=INK_2, fontsize=9)
    ax.set_title(
        f"Throughput - {BUYERS} buyers vs {TICKETS} tickets",
        color=INK, fontsize=11, loc="left", pad=12,
    )
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def chart_correctness(results: list[BenchResult], path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.4, 3.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    _style(ax)

    x = np.arange(len(results))
    bars = ax.bar(x, [r.successes for r in results], 0.5, color=BLUE, zorder=3)
    _label_bars(ax, bars, lambda v: f"{v:,.0f}")
    ax.axhline(TICKETS, color=INK_2, linewidth=1, linestyle=(0, (4, 3)), zorder=4)
    ax.annotate(f"target = {TICKETS} (exactly)", (len(results) - 0.5, TICKETS),
                ha="right", va="bottom", fontsize=8, color=INK_2, xytext=(0, 3),
                textcoords="offset points")

    ax.set_xticks(x, [r.name for r in results], color=INK)
    ax.set_ylim(0, TICKETS * 1.25)
    ax.set_ylabel("successful reservations", color=INK_2, fontsize=9)
    ax.set_title("Correctness - no overselling, no lost tickets",
                 color=INK, fontsize=11, loc="left", pad=12)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"{BUYERS} buyers vs {TICKETS} tickets, per strategy\n")

    results: list[BenchResult] = []
    for name, fn in (
        ("optimistic", optimistic_locking.attempt_purchase),
        ("redis_lock", redis_distributed_lock.attempt_purchase),
    ):
        print(f"  running {name} ...", flush=True)
        results.append(bench_direct(name, fn))
    print("  running queue ...", flush=True)
    ack, resolved = bench_queue()
    results += [ack, resolved]

    write_table(results)
    header = f"{'strategy':<18}{'p50':>8}{'p95':>8}{'p99':>9}{'ok':>5}{'rps':>8}"
    print("\n" + header)
    for r in results:
        print(f"{r.name:<18}{r.pct(50):>7.1f} {r.pct(95):>7.1f} {r.pct(99):>8.1f} "
              f"{r.successes:>4} {r.throughput:>7.1f}")

    answered = results[:3]  # what a buyer waits for a definitive/ack response
    chart_latency(answered, RESULTS_DIR / "latency_percentiles.png",
                  f"Response latency - {BUYERS} concurrent buyers, {TICKETS} tickets")
    chart_latency([resolved], RESULTS_DIR / "queue_resolution.png",
                  "Queue: enqueue -> final answer (worker-paced)")
    chart_throughput(answered, RESULTS_DIR / "throughput.png")
    chart_correctness([results[0], results[1], resolved], RESULTS_DIR / "correctness.png")
    print(f"\nwrote table + charts to {RESULTS_DIR}")


if __name__ == "__main__":
    main()
