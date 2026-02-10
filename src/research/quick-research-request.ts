export type QuickResearchRequest =
  | {
      kind: "theme";
      minutes: number;
      theme: string;
    }
  | {
      kind: "company";
      minutes: number;
      ticker: string;
      question: string;
    };

const normalizeSpaces = (value: string): string => value.replaceAll(/\s+/g, " ").trim();

const normalizeTicker = (value: string): string =>
  value
    .trim()
    .toUpperCase()
    .replaceAll(/[^A-Z0-9.]/g, "")
    .slice(0, 10);

export function parseQuickResearchRequest(raw: string): QuickResearchRequest | null {
  const text = normalizeSpaces(raw);
  if (!text) return null;
  const lowered = text.toLowerCase();

  // Natural language trigger: require all three so we don't accidentally hijack normal chat.
  if (!/\b(research|reserach|reasearch)\b/.test(lowered)) return null;
  if (!/\bpdf\b/.test(lowered)) return null;
  if (!/\b(\d{1,3})\s*(min|mins|minutes)\b/.test(lowered)) return null;

  const minutesMatch = lowered.match(/\b(\d{1,3})\s*(min|mins|minutes)\b/);
  const minutesRaw = minutesMatch?.[1] ?? "";
  const minutes = Math.max(1, Math.min(600, Number.parseInt(minutesRaw, 10) || 5));

  // Try to extract a subject after "on <subject>".
  const onMatch = text.match(/\bon\s+(.+?)(?:\s+and\s+send\b|[.?!]|$)/i);
  let subject = normalizeSpaces(onMatch?.[1] ?? "");
  if (!subject) {
    // Fallback: strip common prefix patterns.
    subject = normalizeSpaces(
      text
        .replaceAll(/give me a\b/i, "")
        .replaceAll(/\b(?:\d{1,3})\s*(?:min|mins|minutes)\b/i, "")
        .replaceAll(/\bresearch\b/i, "")
        .replaceAll(/\brun\b/i, "")
        .replaceAll(/\bsend\b.*\bpdf\b.*$/i, "")
        .replaceAll(/\bpdf\b/i, ""),
    );
  }
  if (!subject) {
    return null;
  }

  // Heuristic: if subject is a single ticker-ish token, treat as company.
  const tickerCandidate = normalizeTicker(subject.replaceAll(/^\$/g, ""));
  const isLikelyTicker =
    /^[A-Z][A-Z0-9.]{0,7}$/.test(tickerCandidate) && subject.split(" ").length === 1;
  if (isLikelyTicker) {
    return {
      kind: "company",
      minutes,
      ticker: tickerCandidate,
      question: `Write an institutional memo on ${tickerCandidate}: base/bull/bear, falsifiers, KPIs, valuation scenarios, catalysts, and an evidence table.`,
    };
  }

  return { kind: "theme", minutes, theme: subject };
}
