import { useState } from "react";
import { ask } from "./api/client";

export function FaqWidget({ eventId }: { eventId: string }) {
  const [question, setQuestion] = useState("");
  const [answer, setAnswer] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!question.trim() || busy) return;
    setBusy(true);
    setError(null);
    setAnswer(null);
    try {
      const result = await ask(eventId, question.trim());
      setAnswer(result.answer);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Something went wrong.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="card">
      <h2>Questions?</h2>
      <form onSubmit={submit} className="faq-form">
        <input
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="e.g. How does the waiting room work?"
          maxLength={500}
          aria-label="Ask a question about this sale"
        />
        <button type="submit" disabled={busy || !question.trim()}>
          {busy ? "Thinking…" : "Ask"}
        </button>
      </form>
      {answer !== null && <p className="faq-answer">{answer}</p>}
      {error !== null && <p className="error">{error}</p>}
    </section>
  );
}
