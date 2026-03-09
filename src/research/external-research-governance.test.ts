import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import {
  approveResearchApprovalRequest,
  createResearchApprovalRequest,
  resolveGovernedSecret,
} from "./external-research-governance.js";
import { syncNewsletterSources } from "./external-research.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-governance-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research governance", () => {
  it("creates approval requests for authenticated newsletter sync before cookie access", async () => {
    const dbPath = testDbPath("approval");
    const sourceUrl = "https://example.substack.com/archive";
    const result = await syncNewsletterSources({
      dbPath,
      requestedBy: "telegram:123",
      sources: [{ provider: "substack", url: sourceUrl }],
      env: {
        OPENCLAW_RESEARCH_SUBSTACK_COOKIE: "op://OpenClaw/openclaw-prod/OPENCLAW_RESEARCH_SUBSTACK_COOKIE",
      } as NodeJS.ProcessEnv,
    });

    expect(result.failures).toBe(1);
    expect(result.docs[0]?.reason).toContain("approval required: research-approval:");

    const db = openResearchDb(dbPath);
    const request = db.prepare(
      `SELECT workflow, capability_key, status, requested_by FROM research_approval_requests ORDER BY id DESC LIMIT 1`,
    ).get() as {
      workflow: string;
      capability_key: string;
      status: string;
      requested_by: string;
    };
    expect(request.workflow).toBe("newsletter_sync");
    expect(request.capability_key).toBe("newsletter_cookie_access");
    expect(request.status).toBe("pending");
    expect(request.requested_by).toBe("telegram:123");
  });

  it("resolves 1Password-backed secrets after approval and records tool runs", async () => {
    const dbPath = testDbPath("approved");
    const sourceUrl = "https://example.substack.com/archive";
    const articleUrl = "https://example.substack.com/p/ai-market-structure";
    const archiveHtml = `<html><body><a href="${articleUrl}">AI market structure</a></body></html>`;
    const articleHtml = `<html><head><title>AI Market Structure</title><meta property="article:published_time" content="2026-03-01T10:00:00Z" /></head><body><article><h1>AI Market Structure</h1><p>${"AI infrastructure spending is shifting from pilots to production budgets. ".repeat(12)}</p></article></body></html>`;

    const pending = createResearchApprovalRequest({
      workflow: "newsletter_sync",
      capability: "newsletter_cookie_access",
      subject: `substack:${sourceUrl}`,
      requestedBy: "telegram:123",
      dbPath,
    });
    approveResearchApprovalRequest({
      approvalRef: `research-approval:${pending.id}`,
      decision: "allow-once",
      resolvedBy: "telegram:123",
      dbPath,
    });

    const fetchFn: typeof fetch = (async (input) => {
      const raw = typeof input === "string" ? input : input.url;
      if (raw === sourceUrl) {
        return new Response(archiveHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw === articleUrl) {
        return new Response(articleHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      return new Response("not found", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await syncNewsletterSources({
      dbPath,
      approvalRef: `research-approval:${pending.id}`,
      requestedBy: "telegram:123",
      sources: [{ provider: "substack", url: sourceUrl }],
      env: {
        OPENCLAW_RESEARCH_SUBSTACK_COOKIE: "op://OpenClaw/openclaw-prod/OPENCLAW_RESEARCH_SUBSTACK_COOKIE",
      } as NodeJS.ProcessEnv,
      secretResolver: (ref) =>
        ref === "op://OpenClaw/openclaw-prod/OPENCLAW_RESEARCH_SUBSTACK_COOKIE"
          ? "substack_session=abc123"
          : "",
      fetchFn,
      maxDocs: 2,
      maxLinksPerSource: 2,
      useSitemaps: false,
    });

    expect(result.ingested).toBe(1);
    const db = openResearchDb(dbPath);
    const toolRuns = db.prepare(
      `SELECT COUNT(*) AS count FROM research_tool_runs WHERE workflow='newsletter_sync'`,
    ).get() as { count: number };
    expect(toolRuns.count).toBeGreaterThanOrEqual(2);
  });

  it("resolves inline secrets without 1Password when explicitly allowed", () => {
    const dbPath = testDbPath("inline");
    const resolved = resolveGovernedSecret({
      refOrValue: "cookie=value",
      workflow: "newsletter_sync",
      capability: "newsletter_cookie_access",
      subject: "substack:https://example.com/archive",
      dbPath,
    });
    expect(resolved).toBe("cookie=value");
  });
});
