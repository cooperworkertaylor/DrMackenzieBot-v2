import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import {
  buildResearchCandidateFromGmailHook,
  computeWeeklyNewsletterDigest,
  ingestExternalResearchDocument,
  parseGmailHookMessage,
  renderWeeklyNewsletterDigestMarkdown,
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
});
