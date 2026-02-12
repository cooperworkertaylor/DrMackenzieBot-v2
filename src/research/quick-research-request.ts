export type QuickResearchRequest =
  | {
      kind: "theme";
      minutes: number;
      theme: string;
      tickers?: string[];
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

const isLikelyTickerToken = (value: string): boolean =>
  /^[A-Z][A-Z0-9.]{0,7}$/.test(normalizeTicker(value.replace(/^\$/, "")));

const extractTickersFromList = (value: string): string[] => {
  const tokens = value.match(/\$?[A-Za-z][A-Za-z0-9.]{0,15}/g) ?? [];
  const out: string[] = [];
  for (const token of tokens) {
    const t = normalizeTicker(token.replace(/^\$/g, ""));
    if (!t) continue;
    out.push(t);
  }
  return Array.from(new Set(out)).slice(0, 80);
};

const stripTickersSpec = (value: string): string =>
  value
    // Remove parenthetical forms: "theme (tickers: A, B)"
    .replace(/\(\s*(?:tickers|universe)\s*:\s*[^)]*\)\s*/gi, " ")
    // Remove inline tickers/universe spec from theme name: "theme tickers: A, B, C"
    .replace(/\b(?:tickers|universe)\s*:\s*[\s\S]+$/gi, "")
    .replaceAll(/\s+/g, " ")
    .trim();

const stripTransportPrefix = (value: string): string => {
  // Some inbound channels prepend metadata like:
  //   "[Telegram Name id:1234567890 2026-02-10 20:46 EST] optical networking"
  // Keep this sanitizer conservative: only strip bracketed prefixes that look like transport headers.
  let out = value.replace(/^\uFEFF/, "").trim();
  for (let i = 0; i < 3; i += 1) {
    const m = out.match(/^\s*\[([^\]]{1,800})\]\s*(.+)$/s);
    if (!m) break;
    const header = (m[1] ?? "").toLowerCase();
    if (
      header.includes("telegram") ||
      header.includes("whatsapp") ||
      header.includes("signal") ||
      header.includes("discord") ||
      header.includes("slack") ||
      header.includes(" id:") ||
      header.match(/\+\s*\d{1,3}\s*m\b/) ||
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

const parseMinutes = (text: string): number | null => {
  const lowered = text.toLowerCase();
  const minuteRe = /\b(\d{1,3})\s*(?:[-\u2010-\u2015\u2212]\s*)?(min|mins|minute|minutes)\b/;
  const tPlusRe = /\bt\+\s*(\d{1,3})\b/i;

  const minuteMatch = lowered.match(minuteRe);
  if (minuteMatch?.[1]) {
    return Math.max(1, Math.min(600, Number.parseInt(minuteMatch[1], 10) || 5));
  }
  const tPlus = lowered.match(tPlusRe);
  if (tPlus?.[1]) {
    return Math.max(1, Math.min(600, Number.parseInt(tPlus[1], 10) || 5));
  }
  const tail = text.match(/\b(\d{1,3})\b\s*[:.\u2010-\u2015\u2212]*\s*$/);
  const tailN = tail?.[1] ? Number.parseInt(tail[1], 10) : NaN;
  if (Number.isFinite(tailN) && tailN >= 1 && tailN <= 600) {
    return Math.max(1, Math.min(600, tailN));
  }
  return null;
};

const stripTimeboxText = (text: string): string =>
  normalizeSpaces(
    text
      .replace(/\b\d{1,3}\s*(?:[-\u2010-\u2015\u2212]\s*)?(?:min|mins|minute|minutes)\b/gi, " ")
      .replace(/\bt\+\s*\d{1,3}\b/gi, " ")
      .replace(/\b\d{1,3}\b\s*[:.\u2010-\u2015\u2212]*\s*$/g, " "),
  );

const parseSlashQuickResearch = (raw: string): QuickResearchRequest | null => {
  const m = raw.match(/^\/([a-z0-9_-]+)\b\s*([\s\S]*)$/i);
  if (!m) return null;
  const command = (m[1] ?? "").toLowerCase();
  const isFastCommand =
    command === "research_fast" || command === "research-fast" || command === "researchfast";
  const isDeepCommand =
    command === "research_deep" || command === "research-deep" || command === "researchdeep";
  const isResearchCommand = command === "research";
  if (!isFastCommand && !isDeepCommand && !isResearchCommand) {
    return null;
  }

  const bodyRaw = normalizeSpaces(stripTransportPrefix(m[2] ?? ""));
  if (!bodyRaw) return null;

  const defaultMinutes = isDeepCommand ? 120 : 30;
  const minutes = parseMinutes(bodyRaw) ?? defaultMinutes;
  const body = stripTimeboxText(bodyRaw);
  const tickersMatch = body.match(/\b(?:tickers|universe)\s*:\s*([\s\S]+)$/i);
  const tickers = tickersMatch?.[1] ? extractTickersFromList(tickersMatch[1]) : [];
  const subject = stripTickersSpec(body);
  if (!subject) return null;

  const subjectTokens = subject.split(/\s+/).filter(Boolean);
  const firstToken = subjectTokens[0] ?? "";
  const ticker = normalizeTicker(firstToken.replace(/^\$/, ""));
  const companyIntentHint =
    /\b(investment|opportunity|long[- ]?term|thesis|valuation|moat|catalyst|bull|bear|falsifier)\b/i.test(
      subject,
    );
  const treatAsCompany =
    isLikelyTickerToken(firstToken) &&
    (subjectTokens.length === 1 || (companyIntentHint && subjectTokens.length <= 16));

  if (treatAsCompany) {
    return {
      kind: "company",
      minutes,
      ticker,
      question: `Write an institutional memo on ${ticker}: base/bull/bear, falsifiers, KPIs, valuation scenarios, catalysts, and an evidence table.`,
    };
  }

  return { kind: "theme", minutes, theme: subject, ...(tickers.length ? { tickers } : {}) };
};

export function parseQuickResearchRequest(raw: string): QuickResearchRequest | null {
  const text = normalizeSpaces(raw);
  if (!text) return null;

  const slashParsed = parseSlashQuickResearch(text);
  if (slashParsed) return slashParsed;

  const lowered = text.toLowerCase();

  const tickersMatch = text.match(/\b(?:tickers|universe)\s*:\s*([\s\S]+)$/i);
  const tickers = tickersMatch?.[1] ? extractTickersFromList(tickersMatch[1]) : [];

  const actionOk =
    /\b(research|reserach|reasearch|snapshot|memo|report|write[- ]?up|update|deep\s*dive|run)\b/.test(
      lowered,
    );
  const mentionsPdf = /\bpdf\b/.test(lowered);
  const minuteRe = /\b(\d{1,3})\s*(?:[-\u2010-\u2015\u2212]\s*)?(min|mins|minute|minutes)\b/;
  const tPlusRe = /\bt\+\s*(\d{1,3})\b/i;
  const minuteMatch = lowered.match(minuteRe);

  const minutes = parseMinutes(text);

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
  subject = stripTickersSpec(subject);
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

  return { kind: "theme", minutes, theme: subject, ...(tickers.length ? { tickers } : {}) };
}
