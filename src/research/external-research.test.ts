import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { openResearchDb } from "./db.js";
import {
  buildResearchCandidateFromGmailHook,
  computeWeeklyNewsletterDigest,
  ingestExternalResearchDocument,
  parseNewsletterSourceSpecs,
  parseGmailHookMessage,
  resolveNewsletterSourcesFromEnv,
  renderWeeklyNewsletterDigestMarkdown,
  syncNewsletterSources,
} from "./external-research.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-research-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research ingestion", () => {
  it("parses gmail hook messages", () => {
    const parsed = parseGmailHookMessage(
      [
        "New email from Cooper Taylor <cooptaylor1@gmail.com>",
        "Subject: RESEARCH NU Variant Notes",
        "Key variant points and valuation disconfirmers.",
        "https://example.com/nu-variant",
      ].join("\n"),
    );
    expect(parsed).not.toBeNull();
    expect(parsed?.senderEmail).toBe("cooptaylor1@gmail.com");
    expect(parsed?.subject).toBe("RESEARCH NU Variant Notes");
    expect(parsed?.url).toBe("https://example.com/nu-variant");
  });

  it("builds candidates for research-subject and newsletter emails", () => {
    const researchCandidate = buildResearchCandidateFromGmailHook({
      sessionKey: "hook:gmail:msg-123",
      message: [
        "New email from Cooper Taylor <cooptaylor1@gmail.com>",
        "Subject: RESEARCH NVDA supply chain channel checks",
        "Findings from checks and notes.",
      ].join("\n"),
      allowedSenders: ["cooptaylor1@gmail.com"],
    });
    expect(researchCandidate?.sourceType).toBe("email_research");
    expect(researchCandidate?.ticker).toBe("NVDA");
    expect(researchCandidate?.externalId).toBe("msg-123");

    const newsletterCandidate = buildResearchCandidateFromGmailHook({
      sessionKey: "hook:gmail:msg-456",
      message: [
        "New email from updates@substack.com",
        "Subject: AI infra pricing power update",
        "A long-form market structure post.",
      ].join("\n"),
      allowedSenders: ["cooptaylor1@gmail.com"],
    });
    expect(newsletterCandidate?.sourceType).toBe("newsletter");
    expect(newsletterCandidate?.provider).toBe("substack");
  });

  it("ingests external docs and generates weekly digest", () => {
    const dbPath = testDbPath("digest");

    const ingestedA = ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "substack",
      sender: "author@substack.com",
      title: "AI compute capex and margin setup",
      subject: "AI compute capex and margin setup",
      content:
        "Capex is accelerating, valuation multiples are stretching, and pricing power depends on supply discipline.",
      url: "https://example.com/substack/ai-capex",
      tags: ["newsletter", "ai"],
    });
    expect(ingestedA.chunks).toBeGreaterThan(0);

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "stratechery",
      sender: "updates@stratechery.com",
      title: "Competition and market share dynamics",
      subject: "Competition and market share dynamics",
      content:
        "Market share concentration is tightening while valuation embeds consensus margin expansion assumptions.",
      url: "https://example.com/stratechery/market-share",
      tags: ["newsletter", "competition"],
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA valuation checks",
      subject: "RESEARCH NVDA valuation checks",
      content: "Model notes and downside scenarios.",
      ticker: "NVDA",
      tags: ["research-email"],
    });

    const db = openResearchDb(dbPath);
    const docs = db.prepare(`SELECT COUNT(*) AS count FROM external_documents`).get() as {
      count: number;
    };
    expect(docs.count).toBe(3);

    const digest = computeWeeklyNewsletterDigest({ dbPath, lookbackDays: 7, limit: 20 });
    expect(digest.totalDocs).toBe(2);
    expect(digest.providers.length).toBeGreaterThan(0);
    const markdown = renderWeeklyNewsletterDigestMarkdown(digest);
    expect(markdown).toContain("Weekly Research Newsletter Digest");
    expect(markdown).toContain("Read In Full");
  });

  it("parses newsletter source specs and env defaults", () => {
    const specs = parseNewsletterSourceSpecs(
      [
        "substack|https://example.substack.com/archive|NVDA",
        "stratechery|https://stratechery.com/archive",
        "# comment",
      ].join("\n"),
    );
    expect(specs).toHaveLength(2);
    expect(specs[0]?.provider).toBe("substack");
    expect(specs[0]?.ticker).toBe("NVDA");

    const envSpecs = resolveNewsletterSourcesFromEnv({
      OPENCLAW_RESEARCH_SUBSTACK_ARCHIVES: "https://x.substack.com/archive",
      OPENCLAW_RESEARCH_STRATECHERY_ARCHIVES: "https://stratechery.com/archive",
      OPENCLAW_RESEARCH_DIFF_ARCHIVES: "https://www.thediff.co/archive",
      OPENCLAW_RESEARCH_NEWSLETTER_SOURCES: "substack|https://x.substack.com/archive",
    });
    expect(envSpecs.length).toBeGreaterThanOrEqual(3);
  });

  it("syncs newsletter sources via authenticated crawl and ingests articles", async () => {
    const dbPath = testDbPath("sync");
    const sourceUrl = "https://substack.com/home";
    const articleA = "https://example.substack.com/p/ai-market-structure";
    const articleB = "https://example.substack.com/p/capex-cycle";
    const archiveHtml = `
      <html><body>
        <a href="https://example.substack.com/p/ai-market-structure">AI market structure</a>
        <a href="https://example.substack.com/p/capex-cycle">Capex cycle</a>
      </body></html>
    `;
    const longTextA =
      "AI infrastructure spending is shifting from experimental pilots to core production budgets. " +
      "Operating leverage is emerging in networking and optical components while training cost curves flatten. ".repeat(
        8,
      );
    const longTextB =
      "Capex guidance now signals a multi-year digestion cycle, but supply-chain bottlenecks keep gross margin dispersion wide. " +
      "Investors should separate cyclicality from structural adoption and track revision breadth across vendors. ".repeat(
        8,
      );
    const articleHtmlA = `<html><head><title>AI Market Structure</title><meta property="article:published_time" content="2026-02-01T10:00:00Z" /></head><body><article><h1>AI Market Structure</h1><p>${longTextA}</p></article></body></html>`;
    const articleHtmlB = `<html><head><title>Capex Cycle</title></head><body><article><h1>Capex Cycle</h1><p>${longTextB}</p></article></body></html>`;

    const fetchFn: typeof fetch = vi.fn(async (input) => {
      const raw = typeof input === "string" ? input : input.url;
      if (raw === sourceUrl || raw === `${sourceUrl}/`) {
        return new Response(archiveHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw === articleA) {
        return new Response(articleHtmlA, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw === articleB) {
        return new Response(articleHtmlB, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      return new Response("not found", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await syncNewsletterSources({
      dbPath,
      sources: [{ provider: "substack", url: sourceUrl }],
      maxLinksPerSource: 3,
      maxDocs: 4,
      fetchFn,
    });

    expect(result.ingested).toBe(2);
    expect(result.failures).toBe(0);
    const digest = computeWeeklyNewsletterDigest({
      dbPath,
      lookbackDays: 14,
      providers: ["substack"],
      limit: 20,
    });
    expect(digest.totalDocs).toBe(2);
    expect(digest.providers[0]?.provider).toBe("substack");
  });

  it("skips login and static pages from sync candidates", async () => {
    const dbPath = testDbPath("skip-static");
    const sourceUrl = "https://stratechery.com/";
    const article =
      "https://stratechery.com/2026/an-interview-with-benedict-evans-about-ai-and-software/";
    const archiveHtml = `
      <html><body>
        <a href="/about/">About</a>
        <a href="https://stratechery.passport.online/member/login?mode=password">Login</a>
        <a href="/2026/an-interview-with-benedict-evans-about-ai-and-software/">Interview</a>
      </body></html>
    `;
    const longText =
      "Interview discussion on AI distribution, economics, and competition dynamics with detailed investor implications. ".repeat(
        20,
      );
    const articleHtml = `<html><head><title>Interview</title></head><body><article><h1>Interview</h1><p>${longText}</p></article></body></html>`;
    const fetchFn: typeof fetch = vi.fn(async (input) => {
      const raw = typeof input === "string" ? input : input.url;
      if (raw === sourceUrl || raw === `${sourceUrl}/`) {
        return new Response(archiveHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw === article) {
        return new Response(articleHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw.includes("passport.online/member/login")) {
        return new Response("<html><title>Login</title></html>", {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      return new Response("not found", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await syncNewsletterSources({
      dbPath,
      sources: [{ provider: "stratechery", url: sourceUrl }],
      maxLinksPerSource: 10,
      maxDocs: 10,
      fetchFn,
    });

    expect(result.ingested).toBe(1);
    expect(result.docs.some((doc) => doc.url.includes("/about/"))).toBe(false);
    expect(result.docs.some((doc) => doc.url.includes("passport.online/member/login"))).toBe(false);
  });
});
