import { useCallback, useEffect, useRef, useState } from "react";
import { getEvent, purchase, queueSocketUrl, STRATEGIES } from "./api/client";
import type { EventOut, PurchaseOut, QueueMessage, Strategy } from "./api/types";
import { FaqWidget } from "./FaqWidget";
import "./App.css";

function getOrCreateUserId(): string {
  const existing = localStorage.getItem("cs-user-id");
  if (existing !== null) return existing;
  const id = crypto.randomUUID();
  localStorage.setItem("cs-user-id", id);
  return id;
}

type BuyState =
  | { phase: "idle" }
  | { phase: "requesting" }
  | { phase: "queued"; position: number }
  | { phase: "done"; result: PurchaseOut };

const STRATEGY_LABELS: Record<Strategy, string> = {
  optimistic: "Instant checkout",
  redis_lock: "Locked checkout",
  queue: "Waiting room",
};

function formatEventDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatEventTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

export default function App() {
  const eventId = new URLSearchParams(window.location.search).get("event") ?? "";
  const [event, setEvent] = useState<EventOut | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [strategy, setStrategy] = useState<Strategy>("optimistic");
  const [buy, setBuy] = useState<BuyState>({ phase: "idle" });
  // The key survives retry clicks: resending it can never buy two tickets.
  const idempotencyKey = useRef<string | null>(null);
  const socket = useRef<WebSocket | null>(null);

  const refreshEvent = useCallback(async () => {
    if (!eventId) return;
    try {
      setEvent(await getEvent(eventId));
      setLoadError(null);
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : "Failed to load event.");
    }
  }, [eventId]);

  useEffect(() => {
    void refreshEvent();
    return () => socket.current?.close();
  }, [refreshEvent]);

  const watchQueue = (key: string, initialPosition: number) => {
    setBuy({ phase: "queued", position: initialPosition });
    const ws = new WebSocket(queueSocketUrl(eventId, key));
    socket.current = ws;
    ws.onmessage = (msg: MessageEvent<string>) => {
      const data = JSON.parse(msg.data) as QueueMessage;
      if (data.type === "position") {
        setBuy({ phase: "queued", position: data.position });
      } else {
        setBuy({
          phase: "done",
          result: {
            status: data.status,
            reservation_id: data.reservation_id || null,
            ticket_id: data.ticket_id || null,
            queue_position: null,
            message: data.message,
          },
        });
        idempotencyKey.current = null;
        ws.close();
        void refreshEvent();
      }
    };
  };

  const buyTicket = async () => {
    if (buy.phase === "requesting" || buy.phase === "queued") return;
    idempotencyKey.current ??= crypto.randomUUID();
    const key = idempotencyKey.current;
    setBuy({ phase: "requesting" });
    try {
      const result = await purchase(eventId, {
        user_id: getOrCreateUserId(),
        idempotency_key: key,
        strategy,
      });
      if (result.status === "queued") {
        watchQueue(key, result.queue_position ?? 0);
      } else {
        setBuy({ phase: "done", result });
        if (result.status !== "retry_exhausted") idempotencyKey.current = null;
        void refreshEvent();
      }
    } catch (err) {
      setBuy({
        phase: "done",
        result: {
          status: "retry_exhausted",
          reservation_id: null,
          ticket_id: null,
          queue_position: null,
          message: err instanceof Error ? err.message : "Request failed.",
        },
      });
    }
  };

  if (!eventId) {
    return (
      <main className="landing">
        <h1>ConcurrenSeat</h1>
        <p>
          Open this page with an event id, e.g. <code>?event=&lt;uuid&gt;</code>{" "}
          (seed one with <code>python -m app.db.seed</code>).
        </p>
      </main>
    );
  }

  const busy = buy.phase === "requesting" || buy.phase === "queued";
  const saleOpen =
    event !== null && new Date(event.sale_start_time).getTime() <= Date.now();
  const soldOut = event !== null && event.tickets_remaining === 0;
  const remainingPct =
    event !== null && event.total_tickets > 0
      ? (event.tickets_remaining / event.total_tickets) * 100
      : 0;
  const barClass =
    remainingPct <= 10 ? "bar-fill critical" : remainingPct <= 35 ? "bar-fill low" : "bar-fill";

  return (
    <>
      <header className="nav">
        <div className="nav-inner">
          <a className="brand" href="/">
            <span className="brand-mark" aria-hidden="true">
              ⧉
            </span>
            ConcurrenSeat
          </a>
          <nav className="nav-links">
            <a href="#faq">Help</a>
            <a
              href="https://github.com/harshues04/ConcurrenSeat"
              target="_blank"
              rel="noreferrer"
            >
              GitHub
            </a>
          </nav>
        </div>
      </header>

      {loadError !== null && (
        <main className="landing">
          <p className="error">{loadError}</p>
        </main>
      )}

      {event !== null && (
        <>
          <section className="hero">
            <div className="hero-inner">
              <p className="hero-kicker">Concert · Live event</p>
              <h1>{event.name}</h1>
              <p className="hero-meta">
                {formatEventDate(event.sale_start_time)} ·{" "}
                {formatEventTime(event.sale_start_time)} · Main Arena
              </p>
              {soldOut ? (
                <span className="pill soldout">Sold out</span>
              ) : saleOpen ? (
                <span className="pill onsale">On sale now</span>
              ) : (
                <span className="pill upcoming">
                  Sale starts {formatEventDate(event.sale_start_time)}
                </span>
              )}
            </div>
          </section>

          <main className="content">
            <div className="grid">
              <div>
                <section className="panel">
                  <h2>Event details</h2>
                  <dl>
                    <div className="info-row">
                      <dt>Date</dt>
                      <dd>
                        {formatEventDate(event.sale_start_time)},{" "}
                        {formatEventTime(event.sale_start_time)}
                      </dd>
                    </div>
                    <div className="info-row">
                      <dt>Venue</dt>
                      <dd>Main Arena (demo)</dd>
                    </div>
                    <div className="info-row">
                      <dt>Capacity</dt>
                      <dd>{event.total_tickets} tickets</dd>
                    </div>
                  </dl>
                </section>

                <section className="panel">
                  <h2>About this event</h2>
                  <p>
                    This is the demo storefront for ConcurrenSeat, a flash-sale
                    backend that sells a fixed pool of tickets to a burst of
                    simultaneous buyers without ever overselling.
                  </p>
                  <p>
                    The checkout-mode picker on the ticket card switches between
                    three real concurrency strategies — optimistic locking, a
                    Redis distributed lock, and a FIFO waiting room with live
                    queue positions over WebSocket. Same guarantee either way:
                    one ticket per buyer, exactly as many as exist.
                  </p>
                </section>

                <FaqWidget eventId={eventId} />
              </div>

              <aside className="ticket-card">
                <div className="tier">
                  <div>
                    <div className="tier-name">General Admission</div>
                    <div className="tier-sub">Standing · e-ticket</div>
                  </div>
                  <div className="tier-price">
                    $79.00
                    <span>demo — no charge</span>
                  </div>
                </div>

                <div className="availability">
                  <div className="availability-label">
                    <span>Availability</span>
                    <span>
                      <strong>{event.tickets_remaining}</strong> of{" "}
                      {event.total_tickets} left
                    </span>
                  </div>
                  <div className="bar">
                    <div className={barClass} style={{ width: `${remainingPct}%` }} />
                  </div>
                </div>

                <div className="mode-row">
                  <label htmlFor="strategy">Checkout mode</label>
                  <select
                    id="strategy"
                    value={strategy}
                    onChange={(e) => setStrategy(e.target.value as Strategy)}
                    disabled={busy}
                  >
                    {STRATEGIES.map((s) => (
                      <option key={s} value={s}>
                        {STRATEGY_LABELS[s]}
                      </option>
                    ))}
                  </select>
                </div>

                <button className="cta" onClick={() => void buyTicket()} disabled={busy}>
                  {buy.phase === "requesting" ? "Processing…" : "Get Tickets"}
                </button>
                <p className="fine-print">
                  Limit 1 per order · duplicate clicks are safe (idempotent)
                </p>

                {buy.phase === "queued" && (
                  <div className="state-panel waiting" role="status">
                    <div className="spinner" aria-hidden="true" />
                    <h3>You're in the waiting room</h3>
                    <p className="queue-position">#{buy.position}</p>
                    <p>
                      Hold tight — your place in line is saved and this updates
                      live. Don't refresh.
                    </p>
                  </div>
                )}

                {buy.phase === "done" && buy.result.status === "success" && (
                  <div className="state-panel confirm" role="status">
                    <h3>✓ You're going!</h3>
                    <p>
                      Ticket reserved. Order ref:{" "}
                      <span className="order-ref">
                        {buy.result.reservation_id ?? "—"}
                      </span>
                    </p>
                  </div>
                )}

                {buy.phase === "done" && buy.result.status === "sold_out" && (
                  <div className="state-panel notice" role="status">
                    <h3>Sold out</h3>
                    <p>All tickets have been claimed for this event.</p>
                  </div>
                )}

                {buy.phase === "done" && buy.result.status === "retry_exhausted" && (
                  <div className="state-panel trouble" role="alert">
                    <h3>High demand right now</h3>
                    <p>
                      {buy.result.message ||
                        "We couldn't complete your order — please try again."}{" "}
                      Your click is idempotent, so retrying is safe.
                    </p>
                  </div>
                )}
              </aside>
            </div>
          </main>

          <footer className="footer">
            ConcurrenSeat — a concurrency-control demo project. Not a real box
            office; no payments are taken.
          </footer>
        </>
      )}
    </>
  );
}
