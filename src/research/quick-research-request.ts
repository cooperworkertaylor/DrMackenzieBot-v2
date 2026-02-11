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

const stripTransportPrefix = (value: string): string => {
  // Some inbound channels prepend metadata like:
  //   "[Telegram Name id:1234567890 2026-02-10 20:46 EST] optical networking"
  // Keep this sanitizer conservative: only strip bracketed prefixes that look like transport headers.
  let out = value.trim();
  for (let i = 0; i < 3; i += 1) {
    const m = out.match(/^\[([^\]]{1,220})\]\s*(.+)$/s);
    if (!m) break;
    const header = (m[1] ?? "").toLowerCase();
    if (
      header.includes("telegram") ||
      header.includes("whatsapp") ||
      header.includes("signal") ||
      header.includes("discord") ||
      header.includes("slack") ||
      header.includes(" id:") ||
      header.includes(" est") ||
      header.includes(" et") ||
      header.match(/\b\d{4}-\d{2}-\d{2}\b/) ||
      header.match(/\b\d{2}:\d{2}\b/)
    ) {
      out = (m[2] ?? "").trim();
      continue;
    }
    break;
  }
  return out;
};

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

  const actionOk =
    /\b(research|reserach|reasearch|snapshot|memo|report|write[- ]?up|update|deep\s*dive|run)\b/.test(
      lowered,
    );
  const mentionsPdf = /\bpdf\b/.test(lowered);
  const minuteRe = /\b(\d{1,3})\s*(?:[-\u2010-\u2015\u2212]\s*)?(min|mins|minute|minutes)\b/;
  const tPlusRe = /\bt\+\s*(\d{1,3})\b/i;

  let minutes: number | null = null;
  const minuteMatch = lowered.match(minuteRe);
  if (minuteMatch?.[1]) {
    minutes = Math.max(1, Math.min(600, Number.parseInt(minuteMatch[1], 10) || 5));
  } else {
    const tPlus = lowered.match(tPlusRe);
    if (tPlus?.[1]) {
      minutes = Math.max(1, Math.min(600, Number.parseInt(tPlus[1], 10) || 5));
    } else {
      // Very common shorthand: "<topic> 5" or "<topic> 30" (minutes).
      const tail = text.match(/\b(\d{1,3})\b\s*[:.\u2010-\u2015\u2212]*\s*$/);
      const tailN = tail?.[1] ? Number.parseInt(tail[1], 10) : NaN;
      if (Number.isFinite(tailN) && tailN >= 1 && tailN <= 600) {
        minutes = Math.max(1, Math.min(600, tailN));
      }
    }
  }

  if (minutes == null) return null;

  // Trigger guard: require either an action keyword OR explicit PDF mention OR a T+ timebox.
  // Also allow shorthand "<topic> <minutes>" if the topic is non-trivial.
  const hasTPlus = tPlusRe.test(lowered);
  const shorthandCandidate = /\b(\d{1,3})\b\s*[:.\u2010-\u2015\u2212]*\s*$/.test(text);
  if (!actionOk && !mentionsPdf && !hasTPlus && !shorthandCandidate) {
    return null;
  }

  const cutAtInstruction = (value: string): string =>
    value
      .replace(/\b(?:and\s+)?(?:send|post|deliver|attach|email|dm)\b[\s\S]*$/i, "")
      .replace(/\b(?:here|now|please)\b[\s\S]*$/i, "")
      .trim();

  // Try to extract a subject after a preposition first.
  const prepMatch = text.match(/\b(on|about|of|for)\s+(.+?)(?:\s+(?:and|then)\b|[.?!]|$)/i);
  let subject = normalizeSpaces(cutAtInstruction(prepMatch?.[2] ?? ""));
  if (!subject) {
    // Fallback: strip the timebox and common verbs, keeping the remaining topic.
    const withoutTime = minuteMatch?.[0]
      ? text.replace(minuteRe, "")
      : text.replace(tPlusRe, "").replace(/\b(\d{1,3})\b\s*[:.\u2010-\u2015\u2212]*\s*$/g, "");
    subject = normalizeSpaces(
      cutAtInstruction(
        withoutTime
          .replace(/\b(?:a|an|the)\b/gi, "")
          .replaceAll(/\b(?:fresh|new|quick|brief|single)\b/gi, "")
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

  subject = normalizeSpaces(stripTransportPrefix(subject));
  if (!subject) return null;

  // Shorthand safety: if we triggered only on a trailing number, require a non-trivial topic.
  if (!actionOk && !mentionsPdf && !hasTPlus) {
    const tokens = subject.split(" ").filter(Boolean);
    if (tokens.length < 2 && subject.length < 10) return null;
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
