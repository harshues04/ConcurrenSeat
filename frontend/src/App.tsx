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
      <main className="page">
        <h1>ConcurrenSeat</h1>
        <p>
          Open this page with an event id, e.g. <code>?event=&lt;uuid&gt;</code>{" "}
          (seed one with <code>python -m app.db.seed</code>).
        </p>
      </main>
    );
  }

  return (
    <main className="page">
      <h1>ConcurrenSeat</h1>
      {loadError !== null && <p className="error">{loadError}</p>}
      {event !== null && (
        <>
          <section className="card">
            <h2>{event.name}</h2>
            <p className="big-count">
              {event.tickets_remaining}
              <span> of {event.total_tickets} tickets left</span>
            </p>
            <p>Sale starts: {new Date(event.sale_start_time).toLocaleString()}</p>

            <div className="buy-row">
              <label>
                Strategy{" "}
                <select
                  value={strategy}
                  onChange={(e) => setStrategy(e.target.value as Strategy)}
                  disabled={buy.phase === "requesting" || buy.phase === "queued"}
                >
                  {STRATEGIES.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </label>
              <button
                onClick={() => void buyTicket()}
                disabled={buy.phase === "requesting" || buy.phase === "queued"}
              >
                {buy.phase === "requesting" ? "Buying…" : "Buy ticket"}
              </button>
            </div>

            {buy.phase === "queued" && (
              <p className="queue-status">
                You are <strong>number {buy.position}</strong> in line — this
                updates live, no refresh needed.
              </p>
            )}
            {buy.phase === "done" && (
              <p className={buy.result.status === "success" ? "success" : "error"}>
                {buy.result.status === "success"
                  ? `Ticket reserved! Reservation ${buy.result.reservation_id ?? ""}`
                  : buy.result.message || buy.result.status}
              </p>
            )}
          </section>
          <FaqWidget eventId={eventId} />
        </>
      )}
    </main>
  );
}
