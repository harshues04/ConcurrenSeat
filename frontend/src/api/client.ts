import type { AskOut, EventOut, PurchaseIn, PurchaseOut, Strategy } from "./types";

const API_URL: string = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

async function parseOrThrow<T>(resp: Response, okStatuses: number[]): Promise<T> {
  const body: unknown = await resp.json();
  if (!okStatuses.includes(resp.status)) {
    const detail =
      typeof body === "object" && body !== null && "detail" in body
        ? String((body as { detail: unknown }).detail)
        : resp.statusText;
    throw new Error(detail);
  }
  return body as T;
}

export async function getEvent(eventId: string): Promise<EventOut> {
  return parseOrThrow<EventOut>(await fetch(`${API_URL}/events/${eventId}`), [200]);
}

export async function purchase(
  eventId: string,
  body: PurchaseIn,
): Promise<PurchaseOut> {
  const resp = await fetch(`${API_URL}/events/${eventId}/purchase`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  // 409 (sold out) and 503 (retry exhausted) still carry a PurchaseOut body.
  return parseOrThrow<PurchaseOut>(resp, [201, 202, 409, 503]);
}

export async function ask(eventId: string, question: string): Promise<AskOut> {
  const resp = await fetch(`${API_URL}/events/${eventId}/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  });
  return parseOrThrow<AskOut>(resp, [200]);
}

export function queueSocketUrl(eventId: string, idempotencyKey: string): string {
  const ws = API_URL.replace(/^http/, "ws");
  return `${ws}/ws/queue/${eventId}?idempotency_key=${encodeURIComponent(idempotencyKey)}`;
}

export const STRATEGIES: readonly Strategy[] = ["optimistic", "redis_lock", "queue"];
