// Shared shapes for the ConcurrenSeat API and WebSocket. Keep in sync with
// backend/app/schemas.py and backend/app/routers/ws.py.

export type Strategy = "optimistic" | "redis_lock" | "queue";

export type PurchaseStatus = "success" | "sold_out" | "retry_exhausted" | "queued";

export interface EventOut {
  id: string;
  name: string;
  total_tickets: number;
  tickets_remaining: number;
  sale_start_time: string; // ISO 8601
}

export interface PurchaseIn {
  user_id: string;
  idempotency_key: string;
  strategy: Strategy | null;
}

export interface PurchaseOut {
  status: PurchaseStatus;
  reservation_id: string | null;
  ticket_id: string | null;
  queue_position: number | null;
  message: string;
}

export interface AskOut {
  answer: string;
}

export interface ApiError {
  detail: string;
}

// Server -> client messages on /ws/queue/{event_id}
export interface PositionMessage {
  type: "position";
  position: number;
}

export interface ResolvedMessage {
  type: "resolved";
  status: PurchaseStatus;
  reservation_id: string;
  ticket_id: string;
  message: string;
}

export type QueueMessage = PositionMessage | ResolvedMessage;
