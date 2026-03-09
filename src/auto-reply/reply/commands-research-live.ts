import type { CommandHandler } from "./commands-types.js";
import { listProfilesForProvider, loadAuthProfileStore } from "../../agents/auth-profiles.js";
import { logVerbose } from "../../globals.js";
import { explainResearchClaim } from "../../research/claim-provenance.js";
import { resolveResearchDbPath } from "../../research/db.js";
import {
  compareExternalResearchPeers,
  detectExternalResearchSourceConflicts,
} from "../../research/external-research-advanced.js";
import { buildPersonalizedResearchSnapshot } from "../../research/external-research-personalization.js";
import {
  buildExternalResearchStructuredReport,
  getLatestExternalResearchStructuredReport,
  storeExternalResearchStructuredReport,
  type StoredExternalResearchReport,
} from "../../research/external-research-report.js";
import {
  buildExternalResearchThesisFromReport,
  getLatestExternalResearchThesis,
  storeExternalResearchThesis,
  type ExternalResearchThesis,
} from "../../research/external-research-thesis.js";
import {
  buildQuickResearchRecentJobsReply,
  buildQuickResearchStatusReply,
  listQuickResearchJobsForRoute,
  retryQuickResearchJob,
  type QuickResearchJobRoute,
} from "../../research/quickrun/quick-research-jobs.js";
import {
  buildResearchExecutionProfile,
  clearStoredResearchExecutionProfile,
  getStoredResearchExecutionProfile,
  listResearchExecutionProfilePresets,
  resolveActiveResearchDbPath as resolveConfiguredResearchDbPath,
  resolveResearchExecutionProfile,
  setStoredResearchExecutionProfile,
} from "../../research/research-model-profile.js";

const parseArgs = (normalized: string, command: string): string[] =>
  normalized
    .replace(new RegExp(`^\\/${command}\\b`, "i"), "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

const formatPct = (value: number): string => `${(value * 100).toFixed(0)}%`;

const formatSourceDate = (value?: string): string => {
  if (!value?.trim()) return "undated";
  const ts = Date.parse(value);
  if (!Number.isFinite(ts)) return "undated";
  return new Date(ts).toISOString().slice(0, 10);
};

const truncateLine = (value: string, max = 220): string => {
  const trimmed = value.replace(/\s+/g, " ").trim();
  if (trimmed.length <= max) return trimmed;
  return `${trimmed.slice(0, max - 1).trimEnd()}…`;
};

const uniqueLines = (values: Array<string | undefined>, limit: number): string[] => {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const normalized = truncateLine(value ?? "");
    if (!normalized) continue;
    const key = normalized.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
    if (out.length >= limit) break;
  }
  return out;
};

const resolveTickerArg = (
  normalized: string,
  command: string,
): { ticker?: string; usage: string } => {
  const args = parseArgs(normalized, command);
  const ticker = args[0]?.trim().toUpperCase();
  return {
    ticker,
    usage: `Usage: /${command} <ticker>`,
  };
};

const resolveTickerTopicArgs = (
  normalized: string,
  command: string,
): { ticker?: string; topic?: string; usage: string } => {
  const args = parseArgs(normalized, command);
  const ticker = args[0]?.trim().toUpperCase();
  const topic = args.slice(1).join(" ").trim();
  return {
    ticker,
    topic,
    usage: `Usage: /${command} <ticker> <topic>`,
  };
};

const resolvePairArgs = (
  normalized: string,
  command: string,
): { left?: string; right?: string; usage: string } => {
  const args = parseArgs(normalized, command).map((value) => value.trim().toUpperCase());
  return {
    left: args[0],
    right: args[1],
    usage: `Usage: /${command} <left_ticker> <right_ticker>`,
  };
};

const resolveCurrentQuickResearchRoute = (params: {
  ctx: { OriginatingTo?: unknown; To?: unknown; AccountId?: unknown; MessageThreadId?: unknown };
  sessionKey?: string;
}): QuickResearchJobRoute | null => {
  const to = String(params.ctx.OriginatingTo ?? params.ctx.To ?? "").trim();
  if (!to) return null;
  return {
    channel: "telegram",
    to,
    accountId:
      params.ctx.AccountId != null && String(params.ctx.AccountId).trim()
        ? String(params.ctx.AccountId)
        : undefined,
    threadId:
      params.ctx.MessageThreadId != null && String(params.ctx.MessageThreadId).trim()
        ? String(params.ctx.MessageThreadId)
        : undefined,
    sessionKey: params.sessionKey?.trim() || undefined,
  };
};

const resolveActiveResearchDbPath = (): string =>
  resolveResearchDbPath(resolveActiveResearchDbPathFromEnv());

const resolveActiveResearchDbPathFromEnv = (): string =>
  resolveConfiguredResearchDbPath(
    process.env.RESEARCH_DB_PATH?.trim() ||
      process.env.OPENCLAW_RESEARCH_DB_PATH?.trim() ||
      undefined,
  );

const parseResearchProfileArgs = (
  normalized: string,
): { action: "status" | "list" | "set" | "reset"; target?: string; profileId?: string } | null => {
  const args = normalized
    .replace(/^\/rprofile\b/i, "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (!args.length) return { action: "status" };
  const action = args[0]?.trim().toLowerCase();
  if (action === "status" || action === "list" || action === "reset") {
    return { action };
  }
  if (action === "set") {
    return {
      action: "set",
      target: args[1]?.trim(),
      profileId: args[2]?.trim(),
    };
  }
  return {
    action: "set",
    target: args[0]?.trim(),
    profileId: args[1]?.trim(),
  };
};

const buildResearchProfileStatusReply = (): string => {
  const dbPath = resolveActiveResearchDbPathFromEnv();
  const active = resolveResearchExecutionProfile({ dbPath });
  const stored = getStoredResearchExecutionProfile({ dbPath });
  const presets = listResearchExecutionProfilePresets();
  const lines = [
    "Research model profile",
    `- Active: ${active.key} -> ${active.modelRef}${active.profileId ? ` (${active.profileId})` : ""}`,
    `- Source: ${active.source}`,
  ];
  if (stored) {
    lines.push(`- Stored override: ${stored.key}`);
  } else {
    lines.push("- Stored override: none");
  }
  lines.push("Presets");
  for (const preset of presets) {
    lines.push(
      `- ${preset.key}: ${preset.modelRef}${preset.profileId ? ` (${preset.profileId})` : ""}`,
    );
  }
  lines.push(
    "Usage: /rprofile list | /rprofile set <preset|provider/model> [profileId] | /rprofile reset",
  );
  return lines.join("\n");
};

const buildResearchProfileListReply = (): string => {
  const store = loadAuthProfileStore();
  const openrouterProfiles = listProfilesForProvider(store, "openrouter");
  const lines = ["Research model profiles"];
  for (const preset of listResearchExecutionProfilePresets()) {
    lines.push(
      `- ${preset.key}: ${preset.modelRef}${preset.profileId ? ` (${preset.profileId})` : ""} — ${preset.description}`,
    );
  }
  lines.push("- custom: /rprofile set openrouter/auto openrouter:default");
  if (openrouterProfiles.length) {
    lines.push(`- OpenRouter auth profiles: ${openrouterProfiles.join(", ")}`);
  } else {
    lines.push("- OpenRouter auth profiles: none detected");
  }
  return lines.join("\n");
};

const resolveReportAndThesis = (
  ticker: string,
): {
  dbPath: string;
  report: StoredExternalResearchReport;
  thesis: ExternalResearchThesis;
} => {
  const dbPath = resolveActiveResearchDbPath();
  let report = getLatestExternalResearchStructuredReport({ ticker, dbPath });
  if (!report) {
    const built = buildExternalResearchStructuredReport({ ticker, dbPath });
    report = storeExternalResearchStructuredReport({ report: built, dbPath });
  }
  let thesis = getLatestExternalResearchThesis({ ticker, dbPath });
  if (!thesis) {
    thesis = storeExternalResearchThesis({
      thesis: buildExternalResearchThesisFromReport({
        report,
        reportId: report.id,
      }),
      dbPath,
    });
  }
  return { dbPath, report, thesis };
};

const buildThesisReply = (ticker: string): string => {
  const { report, thesis } = resolveReportAndThesis(ticker);
  const lines: string[] = [];
  lines.push(`${ticker} thesis`);
  lines.push(`- Stance: ${thesis.stance} | confidence=${formatPct(report.confidence)}`);
  lines.push(`- Summary: ${truncateLine(thesis.summary, 260)}`);
  lines.push("Bull case");
  for (const line of uniqueLines(thesis.bullCase, 3)) lines.push(`- ${line}`);
  if (!thesis.bullCase.length) lines.push("- No bullish points extracted yet.");
  lines.push("Bear case");
  for (const line of uniqueLines(thesis.bearCase, 3)) lines.push(`- ${line}`);
  if (!thesis.bearCase.length) lines.push("- No bear-case points extracted yet.");
  if (thesis.openQuestions.length) {
    lines.push("Open questions");
    for (const line of uniqueLines(thesis.openQuestions, 2)) lines.push(`- ${line}`);
  }
  lines.push(`Use: /changed ${ticker} | /sources ${ticker} | /risks ${ticker}`);
  return lines.join("\n");
};

const buildRiskReply = (ticker: string): string => {
  const { dbPath, report, thesis } = resolveReportAndThesis(ticker);
  const conflicts = detectExternalResearchSourceConflicts({
    ticker,
    dbPath,
    lookbackDays: Math.max(45, report.lookbackDays),
    maxConflicts: 3,
  });
  const topConflict = conflicts.conflicts[0];
  const lines: string[] = [];
  lines.push(`${ticker} risk monitor`);
  lines.push(`- Stance: ${thesis.stance} | confidence=${formatPct(report.confidence)}`);
  lines.push("Top risks");
  for (const line of uniqueLines([...thesis.bearCase, ...report.bearCase], 4))
    lines.push(`- ${line}`);
  if (!thesis.bearCase.length && !report.bearCase.length)
    lines.push("- No explicit risks extracted yet.");
  if (thesis.openQuestions.length) {
    lines.push("Open diligence");
    for (const line of uniqueLines(thesis.openQuestions, 3)) lines.push(`- ${line}`);
  }
  if (topConflict) {
    lines.push("Top source conflict");
    lines.push(`- ${topConflict.topic}: ${truncateLine(topConflict.summary, 220)}`);
  }
  return lines.join("\n");
};

const buildSourcesReply = (ticker: string): string => {
  const { report } = resolveReportAndThesis(ticker);
  const lines: string[] = [];
  lines.push(`${ticker} sources`);
  lines.push(
    `- Coverage: ${report.evidenceCoverage.sourceCount} sources / ${report.evidenceCoverage.providerCount} providers / fresh=${formatPct(report.evidenceCoverage.freshSourceRatio)}`,
  );
  lines.push(
    `- Confidence: ${formatPct(report.confidence)} | ${truncateLine(report.confidenceRationale, 180)}`,
  );
  for (const source of report.sources.slice(0, 5)) {
    lines.push(
      `- ${formatSourceDate(source.publishedAt ?? source.receivedAt)} | ${source.provider} | tier ${source.trustTier} | ${truncateLine(source.title, 110)}`,
    );
  }
  if (!report.sources.length) lines.push("- No recent sources found.");
  return lines.join("\n");
};

const buildChangedReply = (ticker: string): string => {
  const { report } = resolveReportAndThesis(ticker);
  const lines: string[] = [];
  lines.push(`${ticker} what changed`);
  lines.push(`- Confidence: ${formatPct(report.confidence)}`);
  for (const line of uniqueLines(report.whatChanged, 5)) lines.push(`- ${line}`);
  if (report.diffFromPrevious) {
    lines.push("Delta vs prior run");
    lines.push(
      `- Confidence delta: ${(report.diffFromPrevious.confidenceDelta * 100).toFixed(0)} pts`,
    );
    for (const line of uniqueLines(report.diffFromPrevious.newBullCase, 2))
      lines.push(`- New support: ${line}`);
    for (const line of uniqueLines(report.diffFromPrevious.newBearCase, 2))
      lines.push(`- New risk: ${line}`);
    for (const line of uniqueLines(report.diffFromPrevious.resolvedUnknowns, 2))
      lines.push(`- Resolved: ${line}`);
  }
  if (report.nextActions.length) {
    lines.push("Next actions");
    for (const line of uniqueLines(report.nextActions, 3)) lines.push(`- ${line}`);
  }
  return lines.join("\n");
};

const buildSnapshotReply = (ticker: string): string => {
  const { report, thesis } = resolveReportAndThesis(ticker);
  const snapshot = buildPersonalizedResearchSnapshot({
    ticker,
    dbPath: resolveActiveResearchDbPath(),
  });
  const lines: string[] = [];
  lines.push(`${ticker} snapshot`);
  lines.push(`- Stance: ${thesis.stance} | confidence=${formatPct(report.confidence)}`);
  lines.push(`- Summary: ${truncateLine(snapshot.summary, 260)}`);
  lines.push("Next actions");
  for (const line of uniqueLines(snapshot.nextActions, 4)) lines.push(`- ${line}`);
  if (snapshot.notebookEntries.length) {
    lines.push(`- Latest notebook: ${truncateLine(snapshot.notebookEntries[0]!.title, 90)}`);
  }
  return lines.join("\n");
};

const buildCompareReply = (leftTicker: string, rightTicker: string): string => {
  const dbPath = resolveActiveResearchDbPath();
  resolveReportAndThesis(leftTicker);
  resolveReportAndThesis(rightTicker);
  const comparison = compareExternalResearchPeers({
    leftTicker,
    rightTicker,
    dbPath,
  });
  const lines: string[] = [];
  lines.push(`${leftTicker} vs ${rightTicker}`);
  lines.push(`- Summary: ${truncateLine(comparison.summary, 220)}`);
  lines.push(`- Evidence edge: ${truncateLine(comparison.evidenceEdge, 180)}`);
  lines.push(`- Risk edge: ${truncateLine(comparison.riskEdge, 180)}`);
  for (const line of uniqueLines(comparison.notableDeltas, 4)) lines.push(`- ${line}`);
  return lines.join("\n");
};

const buildWhyReply = (ticker: string, topic: string): string => {
  const report = explainResearchClaim({
    ticker,
    topic,
    dbPath: resolveActiveResearchDbPath(),
    maxClaims: 3,
    maxEvidencePerClaim: 3,
  });
  const lines: string[] = [];
  lines.push(`${ticker} why: ${topic}`);
  if (!report.matches.length) {
    lines.push("- No matching stored claims found.");
    lines.push(`Try: /sources ${ticker} | /snapshot ${ticker}`);
    return lines.join("\n");
  }
  for (const match of report.matches) {
    lines.push(
      `- Claim: ${truncateLine(match.claimText, 220)} | type=${match.claimType} | confidence=${formatPct(match.confidence)}`,
    );
    if (match.validFrom) {
      lines.push(`  - As of: ${match.validFrom}`);
    }
    if (match.matchedTerms.length) {
      lines.push(`  - Match: ${match.matchedTerms.join(", ")}`);
    }
    lines.push("  - Evidence:");
    for (const evidence of match.evidence) {
      lines.push(
        `    - ${evidence.publishedAt ?? "undated"} | ${evidence.provider} | tier ${evidence.trustTier} | ${truncateLine(evidence.title, 110)}`,
      );
    }
  }
  lines.push(`Use: /sources ${ticker} | /changed ${ticker} | /thesis ${ticker}`);
  return lines.join("\n");
};

export const handleLiveResearchCommands: CommandHandler = async (params, allowTextCommands) => {
  if (!allowTextCommands) return null;
  const normalized = params.command.commandBodyNormalized.trim();
  const lower = normalized.toLowerCase();
  const supported =
    lower === "/thesis" ||
    lower.startsWith("/thesis ") ||
    lower === "/risks" ||
    lower.startsWith("/risks ") ||
    lower === "/sources" ||
    lower.startsWith("/sources ") ||
    lower === "/changed" ||
    lower.startsWith("/changed ") ||
    lower === "/snapshot" ||
    lower.startsWith("/snapshot ") ||
    lower === "/why" ||
    lower.startsWith("/why ") ||
    lower === "/compare" ||
    lower.startsWith("/compare ") ||
    lower === "/qstatus" ||
    lower.startsWith("/qstatus ") ||
    lower === "/qlast" ||
    lower.startsWith("/qlast ") ||
    lower === "/qretry" ||
    lower.startsWith("/qretry ") ||
    lower === "/rprofile" ||
    lower.startsWith("/rprofile ");
  if (!supported) return null;
  if (!params.command.isAuthorizedSender) {
    logVerbose(
      `Ignoring research command from unauthorized sender: ${params.command.senderId || "<unknown>"}`,
    );
    return { shouldContinue: false };
  }

  try {
    if (lower === "/rprofile" || lower.startsWith("/rprofile ")) {
      const parsed = parseResearchProfileArgs(normalized);
      if (!parsed) {
        return {
          shouldContinue: false,
          reply: {
            text: "Usage: /rprofile list | /rprofile set <preset|provider/model> [profileId] | /rprofile reset",
          },
        };
      }
      const dbPath = resolveActiveResearchDbPathFromEnv();
      if (parsed.action === "status") {
        return { shouldContinue: false, reply: { text: buildResearchProfileStatusReply() } };
      }
      if (parsed.action === "list") {
        return { shouldContinue: false, reply: { text: buildResearchProfileListReply() } };
      }
      if (parsed.action === "reset") {
        clearStoredResearchExecutionProfile({ dbPath });
        const profile = resolveResearchExecutionProfile({ dbPath });
        return {
          shouldContinue: false,
          reply: {
            text: `Research profile reset.\n- Active: ${profile.key} -> ${profile.modelRef}`,
          },
        };
      }
      if (!parsed.target) {
        return {
          shouldContinue: false,
          reply: {
            text: "Usage: /rprofile set <preset|provider/model> [profileId]",
          },
        };
      }
      const profile = buildResearchExecutionProfile({
        rawSelection: parsed.target,
        profileId: parsed.profileId,
      });
      const stored = setStoredResearchExecutionProfile({ profile, dbPath });
      return {
        shouldContinue: false,
        reply: {
          text: `Research profile updated.\n- Active: ${stored.key} -> ${stored.modelRef}${stored.profileId ? ` (${stored.profileId})` : ""}`,
        },
      };
    }

    if (lower === "/compare" || lower.startsWith("/compare ")) {
      const { left, right, usage } = resolvePairArgs(normalized, "compare");
      if (!left || !right) {
        return { shouldContinue: false, reply: { text: usage, isError: true } };
      }
      return {
        shouldContinue: false,
        reply: { text: buildCompareReply(left, right) },
      };
    }

    if (lower === "/why" || lower.startsWith("/why ")) {
      const { ticker, topic, usage } = resolveTickerTopicArgs(normalized, "why");
      if (!ticker || !topic) {
        return { shouldContinue: false, reply: { text: usage, isError: true } };
      }
      return {
        shouldContinue: false,
        reply: { text: buildWhyReply(ticker, topic) },
      };
    }

    if (lower === "/qstatus" || lower.startsWith("/qstatus ")) {
      const route = resolveCurrentQuickResearchRoute({
        ctx: params.ctx,
        sessionKey: params.sessionKey,
      });
      if (!route) {
        return {
          shouldContinue: false,
          reply: { text: "No quick research route found.", isError: true },
        };
      }
      const reply = buildQuickResearchStatusReply({
        route,
        dbPath: resolveActiveResearchDbPath(),
      });
      return {
        shouldContinue: false,
        reply: { text: reply ?? "No quick research jobs found for this chat.", isError: !reply },
      };
    }

    if (lower === "/qlast" || lower.startsWith("/qlast ")) {
      const route = resolveCurrentQuickResearchRoute({
        ctx: params.ctx,
        sessionKey: params.sessionKey,
      });
      if (!route) {
        return {
          shouldContinue: false,
          reply: { text: "No quick research route found.", isError: true },
        };
      }
      const reply = buildQuickResearchRecentJobsReply({
        route,
        dbPath: resolveActiveResearchDbPath(),
        limit: 3,
      });
      return {
        shouldContinue: false,
        reply: {
          text: reply ?? "No recent quick research jobs found for this chat.",
          isError: !reply,
        },
      };
    }

    if (lower === "/qretry" || lower.startsWith("/qretry ")) {
      const route = resolveCurrentQuickResearchRoute({
        ctx: params.ctx,
        sessionKey: params.sessionKey,
      });
      if (!route) {
        return {
          shouldContinue: false,
          reply: { text: "No quick research route found.", isError: true },
        };
      }
      const args = parseArgs(normalized, "qretry");
      const dbPath = resolveActiveResearchDbPath();
      const targetId =
        args[0]?.trim() ||
        listQuickResearchJobsForRoute({ route, dbPath, limit: 10 }).find(
          (job) => job.status === "failed",
        )?.id;
      if (!targetId) {
        return {
          shouldContinue: false,
          reply: { text: "No failed quick research job found to retry.", isError: true },
        };
      }
      const retried = retryQuickResearchJob({ id: targetId, dbPath });
      if (!retried || retried.status !== "queued") {
        return {
          shouldContinue: false,
          reply: { text: `Retry failed for job ${targetId}.`, isError: true },
        };
      }
      return {
        shouldContinue: false,
        reply: { text: `Requeued quick research job ${targetId}. Use /qstatus for live progress.` },
      };
    }

    const commandName = lower.replace(/^\/([a-z]+).*$/i, "$1");
    const { ticker, usage } = resolveTickerArg(normalized, commandName);
    if (!ticker) {
      return { shouldContinue: false, reply: { text: usage, isError: true } };
    }

    const text =
      commandName === "thesis"
        ? buildThesisReply(ticker)
        : commandName === "risks"
          ? buildRiskReply(ticker)
          : commandName === "sources"
            ? buildSourcesReply(ticker)
            : commandName === "changed"
              ? buildChangedReply(ticker)
              : buildSnapshotReply(ticker);

    return { shouldContinue: false, reply: { text } };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      shouldContinue: false,
      reply: {
        text: `Research command failed: ${message}`,
        isError: true,
      },
    };
  }
};

export const __testOnly = {
  parseArgs,
};
