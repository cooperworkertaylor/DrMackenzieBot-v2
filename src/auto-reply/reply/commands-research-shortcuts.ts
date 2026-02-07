import type { CommandHandler } from "./commands-types.js";
import { logVerbose } from "../../globals.js";

const parseArgs = (normalized: string, command: string): string[] =>
  normalized
    .replace(new RegExp(`^\\/${command}\\b`, "i"), "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

const parseArgsWithTail = (
  normalized: string,
  command: string,
): { first: string; rest?: string } => {
  const body = normalized.replace(new RegExp(`^\\/${command}\\b`, "i"), "").trim();
  if (!body) return { first: "" };
  const [first, ...rest] = body.split(/\s+/);
  return {
    first: first ?? "",
    rest: rest.length ? rest.join(" ") : undefined,
  };
};

const parseCompanyMemo = (normalized: string): { ticker: string; hypothesis: string } => {
  const parsed = parseArgsWithTail(normalized, "icmemo");
  const ticker = (parsed.first || "AAPL").toUpperCase();
  const hypothesis =
    parsed.rest?.trim() || "What is the 12-18 month variant thesis and what would falsify it?";
  return { ticker, hypothesis };
};

const parseThemeMemo = (normalized: string): { theme: string; hypothesis: string } => {
  const parsed = parseArgsWithTail(normalized, "ictheme");
  const theme = parsed.first || "ai-infrastructure";
  const hypothesis =
    parsed.rest?.trim() ||
    "What is the core 12-18 month theme thesis, dominant drivers, and disconfirmers?";
  return { theme, hypothesis };
};

const parseSectorMemo = (normalized: string): { sector: string; hypothesis: string } => {
  const parsed = parseArgsWithTail(normalized, "icsector");
  const sector = parsed.first || "Technology";
  const hypothesis =
    parsed.rest?.trim() ||
    "What is the benchmark-relative sector thesis over 12-18 months and what risks could break it?";
  return { sector, hypothesis };
};

const parseExhibitRequest = (normalized: string): { ticker: string; focus: string } => {
  const parsed = parseArgsWithTail(normalized, "icexhibit");
  const ticker = (parsed.first || "AAPL").toUpperCase();
  const focus = parsed.rest?.trim() || "revenue, operating income, margins, EPS, FCF, debt, cash";
  return { ticker, focus };
};

export const handleResearchShortcutCommands: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  const normalized = params.command.commandBodyNormalized.trim();
  const lower = normalized.toLowerCase();
  const supported =
    lower === "/icmemo" ||
    lower.startsWith("/icmemo ") ||
    lower === "/ictheme" ||
    lower.startsWith("/ictheme ") ||
    lower === "/icsector" ||
    lower.startsWith("/icsector ") ||
    lower === "/icexhibit" ||
    lower.startsWith("/icexhibit ");
  if (!supported) return null;
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring institutional shortcut command from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }

  if (lower === "/icmemo" || lower.startsWith("/icmemo ")) {
    const { ticker, hypothesis } = parseCompanyMemo(normalized);
    return {
      shouldContinue: false,
      reply: {
        text: [
          "Institutional Company Memo Shortcut",
          "",
          `Use this request (ready to send):`,
          `Run institutional research for TICKER=${ticker}.`,
          `Question: "${hypothesis}"`,
          "Build SEC/XBRL time-series exhibits first (revenue, operating income, margin, EPS, FCF, debt/cash if available), then produce a citation-enforced memo.",
          "Include: variant view vs consensus, benchmark-relative context, base/bull/bear scenarios, disconfirming evidence, contradiction checks, and explicit falsification triggers.",
          "Quality rules: fail-closed institutional gate at min score 0.82; if it fails, return no recommendation and list all failed checks + required fixes.",
          "Output format: 1) Thesis, 2) Exhibits summary, 3) Evidence with citations, 4) Scenarios, 5) Risks/disconfirmers, 6) Decision + sizing/risk controls, 7) Quality gate report.",
          "",
          `Tip: /icmemo ${ticker} <your hypothesis>`,
        ].join("\n"),
      },
    };
  }

  if (lower === "/ictheme" || lower.startsWith("/ictheme ")) {
    const { theme, hypothesis } = parseThemeMemo(normalized);
    return {
      shouldContinue: false,
      reply: {
        text: [
          "Institutional Theme Memo Shortcut",
          "",
          `Use this request (ready to send):`,
          `Run institutional THEME research for "${theme}".`,
          `Question: "${hypothesis}"`,
          "Include top constituents, benchmark-relative attribution, style + macro factor decomposition, rolling windows, catalyst calendar, and disconfirmers.",
          "Quality rules: fail-closed institutional gate at min score 0.82; if it fails, return no recommendation and list all failed checks + required fixes.",
          "Output format: 1) Theme thesis, 2) Benchmark-relative context, 3) Factor/macro attribution, 4) Catalysts & risks, 5) Actionable positioning, 6) Quality gate report.",
          "",
          `Tip: /ictheme ${theme} <your hypothesis>`,
        ].join("\n"),
      },
    };
  }

  if (lower === "/icsector" || lower.startsWith("/icsector ")) {
    const { sector, hypothesis } = parseSectorMemo(normalized);
    return {
      shouldContinue: false,
      reply: {
        text: [
          "Institutional Sector Memo Shortcut",
          "",
          `Use this request (ready to send):`,
          `Run institutional SECTOR research for "${sector}".`,
          `Question: "${hypothesis}"`,
          "Include cross-sectional breadth/dispersion, leaders/laggards, benchmark-relative context, style + macro attribution, rolling windows, and catalyst crowding.",
          "Quality rules: fail-closed institutional gate at min score 0.82; if it fails, return no recommendation and list all failed checks + required fixes.",
          "Output format: 1) Sector thesis, 2) Evidence + exhibits, 3) Attribution drivers, 4) Risk/disconfirmers, 5) Positioning framework, 6) Quality gate report.",
          "",
          `Tip: /icsector ${sector} <your hypothesis>`,
        ].join("\n"),
      },
    };
  }

  const { ticker, focus } = parseExhibitRequest(normalized);
  return {
    shouldContinue: false,
    reply: {
      text: [
        "SEC/XBRL Exhibits Shortcut",
        "",
        `Use this request (ready to send):`,
        `Build SEC/XBRL exhibits for ${ticker}.`,
        `Required exhibit focus: ${focus}.`,
        "Return point-in-time series with clear labels, source provenance, and trend interpretation for decision use.",
        "Then summarize what confirms vs disconfirms the thesis in 8 concise bullets with citations.",
        "",
        `Tip: /icexhibit ${ticker} <focus areas>`,
      ].join("\n"),
    },
  };
};

export const __testOnly = {
  parseArgs,
  parseArgsWithTail,
};
