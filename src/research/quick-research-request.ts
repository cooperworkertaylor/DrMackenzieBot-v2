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

  // Natural language trigger: require minutes + pdf + an "action-ish" keyword so we don't hijack normal chat.
  const actionOk =
    /\b(research|reserach|reasearch|snapshot|memo|report|write[- ]?up|update|deep\s*dive|run)\b/.test(
      lowered,
    ) || /\bt\+\s*\d+\b/.test(lowered);
  if (!/\bpdf\b/.test(lowered)) return null;
  const minuteRe = /\b(\d{1,3})\s*(?:[-\u2010-\u2015\u2212]\s*)?(min|mins|minute|minutes)\b/;
  if (!minuteRe.test(lowered)) return null;
  if (!actionOk) return null;

  const minutesMatch = lowered.match(minuteRe);
  const minutesRaw = minutesMatch?.[1] ?? "";
  const minutes = Math.max(1, Math.min(600, Number.parseInt(minutesRaw, 10) || 5));

  const cutAtInstruction = (value: string): string =>
    value
      .replace(/\b(?:and\s+)?(?:send|post|deliver|attach|email|dm)\b[\s\S]*$/i, "")
      .replace(/\b(?:here|now|please)\b[\s\S]*$/i, "")
      .trim();

  // Try to extract a subject after a preposition first.
  const prepMatch = text.match(/\b(on|about|of|for)\s+(.+?)(?:\s+(?:and|then)\b|[.?!]|$)/i);
  let subject = normalizeSpaces(cutAtInstruction(prepMatch?.[2] ?? ""));
  if (!subject) {
    // Fallback: try to grab the text after the minutes phrase up to delivery instructions.
    const idx = lowered.search(minuteRe);
    const after = idx >= 0 ? text.slice(idx).replace(minuteRe, "") : text;
    subject = normalizeSpaces(
      cutAtInstruction(
        after
          .replace(/\b(?:a|an|the)\b/gi, "")
          .replaceAll(
            /\b(?:research|reserach|reasearch|snapshot|memo|report|write[- ]?up|update|deep\s*dive|run)\b/gi,
            "",
          )
          .replaceAll(/\bpdf\b/gi, ""),
      ),
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
