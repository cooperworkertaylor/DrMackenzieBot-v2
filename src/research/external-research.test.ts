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
import { safeCanonicalizeUrl } from "./ingestion-utils.js";

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

    const semianalysisCandidate = buildResearchCandidateFromGmailHook({
      sessionKey: "hook:gmail:msg-789",
      message: [
        "New email from digest@semianalysis.com",
        "Subject: AI capex stack update",
        "Latest piece from SemiAnalysis with demand and supply-side checks.",
      ].join("\n"),
      allowedSenders: ["cooptaylor1@gmail.com"],
    });
    expect(semianalysisCandidate?.sourceType).toBe("newsletter");
    expect(semianalysisCandidate?.provider).toBe("semianalysis");
  });

  it("force-ingests configured senders even without RESEARCH prefix", () => {
    const noForce = buildResearchCandidateFromGmailHook({
      sessionKey: "hook:gmail:msg-900",
      message: [
        "New email from CT <ct@salemcounsel.com>",
        "Subject: optical networking follow-up",
        "My latest notes and quick checks.",
      ].join("\n"),
      allowedSenders: ["cooptaylor1@gmail.com"],
    });
    expect(noForce).toBeNull();

    const forced = buildResearchCandidateFromGmailHook({
      sessionKey: "hook:gmail:msg-901",
      message: [
        "New email from CT <ct@salemcounsel.com>",
        "Subject: optical networking follow-up",
        "My latest notes and quick checks.",
      ].join("\n"),
      allowedSenders: ["cooptaylor1@gmail.com"],
      forceSenders: ["ct@salemcounsel.com"],
    });
    expect(forced).not.toBeNull();
    expect(forced?.sourceType).toBe("email_research");
    expect(forced?.provider).toBe("other");
    expect(forced?.externalId).toBe("msg-901");
    expect(forced?.tags).toContain("sender-force-ingest");
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

  it("canonicalizes URLs, registers sources, and dedupes newsletter documents", () => {
    const dbPath = testDbPath("canonical-dedupe");

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "substack",
      sender: "author@substack.com",
      title: "AI infra pricing power",
      subject: "AI infra pricing power",
      content: "Pricing discipline and supply constraints matter for margins.",
      url: "https://www.example.com/p/ai-infra?utm_source=test&a=2&b=1",
      tags: ["newsletter", "ai"],
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "substack",
      sender: "author@substack.com",
      title: "AI infra pricing power",
      subject: "AI infra pricing power",
      content: "Pricing discipline and supply constraints matter for margins.",
      url: "https://example.com/p/ai-infra?b=1&a=2&utm_source=other",
      tags: ["newsletter", "ai", "duplicate"],
    });

    const db = openResearchDb(dbPath);
    const docs = db.prepare(
      `SELECT count(*) AS count, canonical_url, trust_tier, source_key, materiality_score
       FROM external_documents`,
    ).get() as {
      count: number;
      canonical_url: string;
      trust_tier: number;
      source_key: string;
      materiality_score: number;
    };
    expect(docs.count).toBe(1);
    expect(docs.canonical_url).toBe(safeCanonicalizeUrl("https://example.com/p/ai-infra?b=1&a=2"));
    expect(docs.trust_tier).toBeGreaterThan(0);
    expect(docs.source_key).toContain("newsletter:substack");
    expect(docs.materiality_score).toBeGreaterThan(0);

    const sources = db.prepare(`SELECT count(*) AS count FROM research_sources`).get() as {
      count: number;
    };
    expect(sources.count).toBe(1);
  });

  it("parses newsletter source specs and env defaults", () => {
    const specs = parseNewsletterSourceSpecs(
      [
        "substack|https://example.substack.com/archive|NVDA",
        "stratechery|https://stratechery.com/archive",
        "semianalysis|https://www.semianalysis.com",
        "# comment",
      ].join("\n"),
    );
    expect(specs).toHaveLength(3);
    expect(specs[0]?.provider).toBe("substack");
    expect(specs[0]?.ticker).toBe("NVDA");
    expect(specs[2]?.provider).toBe("semianalysis");

    const envSpecs = resolveNewsletterSourcesFromEnv({
      OPENCLAW_RESEARCH_SUBSTACK_ARCHIVES: "https://x.substack.com/archive",
      OPENCLAW_RESEARCH_STRATECHERY_ARCHIVES: "https://stratechery.com/archive",
      OPENCLAW_RESEARCH_DIFF_ARCHIVES: "https://www.thediff.co/archive",
      OPENCLAW_RESEARCH_SEMIANALYSIS_ARCHIVES: "https://www.semianalysis.com",
      OPENCLAW_RESEARCH_NEWSLETTER_SOURCES: "substack|https://x.substack.com/archive",
    });
    expect(envSpecs.length).toBeGreaterThanOrEqual(4);
    expect(envSpecs.some((spec) => spec.provider === "semianalysis")).toBe(true);
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

  it("expands candidates from sitemap and applies since-date filter", async () => {
    const dbPath = testDbPath("sitemap-since");
    const sourceUrl = "https://www.semianalysis.com";
    const oldArticle = "https://www.semianalysis.com/p/old-cycle-note";
    const newArticle = "https://www.semianalysis.com/p/new-cycle-note";
    const archiveHtml = `<html><body><h1>SemiAnalysis</h1></body></html>`;
    const robotsTxt = `User-agent: *\nSitemap: https://www.semianalysis.com/sitemap.xml\n`;
    const sitemapXml = `
      <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url>
          <loc>${oldArticle}</loc>
          <lastmod>2021-01-01T00:00:00Z</lastmod>
        </url>
        <url>
          <loc>${newArticle}</loc>
          <lastmod>2025-10-01T00:00:00Z</lastmod>
        </url>
      </urlset>
    `;
    const longText =
      "Detailed supply chain checks, unit economics, and capex regime transition with implications for investor positioning. ".repeat(
        20,
      );
    const articleHtml = `<html><head><title>New Cycle Note</title><meta property="article:published_time" content="2025-10-02T00:00:00Z" /></head><body><article><h1>New Cycle Note</h1><p>${longText}</p></article></body></html>`;

    const fetchFn: typeof fetch = vi.fn(async (input) => {
      const raw = typeof input === "string" ? input : input.url;
      if (
        raw === sourceUrl ||
        raw === `${sourceUrl}/` ||
        raw === "https://www.semianalysis.com/" ||
        raw === "https://www.semianalysis.com"
      ) {
        return new Response(archiveHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw === "https://www.semianalysis.com/robots.txt") {
        return new Response(robotsTxt, {
          status: 200,
          headers: { "content-type": "text/plain; charset=utf-8" },
        });
      }
      if (raw === "https://www.semianalysis.com/sitemap.xml") {
        return new Response(sitemapXml, {
          status: 200,
          headers: { "content-type": "application/xml; charset=utf-8" },
        });
      }
      if (raw === newArticle) {
        return new Response(articleHtml, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (raw === oldArticle) {
        return new Response("<html><title>Old</title></html>", {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      return new Response("not found", { status: 404 });
    }) as unknown as typeof fetch;

    const result = await syncNewsletterSources({
      dbPath,
      sources: [{ provider: "semianalysis", url: sourceUrl }],
      sinceDate: "2023-01-01",
      useSitemaps: true,
      sitemapMaxUrls: 100,
      maxLinksPerSource: 1,
      maxDocs: 10,
      fetchFn,
    });

    expect(result.ingested).toBe(1);
    expect(result.attempted).toBe(1);
    expect(result.docs.some((doc) => doc.url === newArticle && doc.ingested)).toBe(true);
    expect(result.docs.some((doc) => doc.url === oldArticle)).toBe(false);
  });
});
