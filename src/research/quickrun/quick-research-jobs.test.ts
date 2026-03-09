import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { QuickrunJobStore } from "./job-store.js";
import {
  buildQuickResearchPdfFollowupReply,
  buildQuickResearchStatusReply,
  buildQuickResearchTelegramSummary,
} from "./quick-research-jobs.js";

const tempDirs: string[] = [];

const makeDbPath = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "quickrun-jobs-"));
  tempDirs.push(dir);
  return path.join(dir, "research.db");
};

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe("buildQuickResearchTelegramSummary", () => {
  it("extracts an investor digest from the v2 report shape", () => {
    const text = buildQuickResearchTelegramSummary({
      kind: "company",
      subject: "NVDA",
      jobId: "job-123",
      runId: "run-456",
      builtAtEt: "2026-03-09 14:40 ET",
      pdfBytes: 1024,
      sha256: "abc123",
      report: {
        sections: [
          {
            key: "executive_summary",
            blocks: [
              {
                tag: "INTERPRETATION",
                text: "Demand remains strong as enterprise AI budgets expand.",
              },
            ],
          },
          {
            key: "thesis",
            blocks: [
              {
                tag: "FACT",
                text: "Gross margin could hold near 72% if pricing stays disciplined.",
              },
            ],
          },
          {
            key: "risks_premortem",
            blocks: [
              { tag: "INTERPRETATION", text: "Custom silicon competition could pressure pricing." },
            ],
          },
        ],
        appendix: {
          whats_missing: ["Need one primary filing to validate the margin bridge."],
        },
      },
    });

    expect(text).toContain("Company memo ready: NVDA");
    expect(text).toContain("Summary");
    expect(text).toContain("Demand remains strong as enterprise AI budgets expand.");
    expect(text).toContain("Bull case");
    expect(text).toContain("Gross margin could hold near 72% if pricing stays disciplined.");
    expect(text).toContain("Bear case / change-mind triggers");
    expect(text).toContain("Custom silicon competition could pressure pricing.");
    expect(text).toContain("Next diligence");
    expect(text).toContain("Need one primary filing to validate the margin bridge.");
  });

  it("builds a deterministic status reply for the latest matching route", () => {
    const dbPath = makeDbPath();
    const store = QuickrunJobStore.open(dbPath);
    store.enqueue({
      id: "job-789",
      jobType: "quick_research_pdf_v2",
      runAfterMs: 0,
      payload: {
        jobId: "job-789",
        request: {
          kind: "company",
          ticker: "NVDA",
          minutes: 5,
        },
        createdAtMs: Date.UTC(2026, 2, 9, 19, 25),
        deliverAtMs: Date.UTC(2026, 2, 9, 19, 30),
        researchProfile: {
          key: "primary",
          label: "Primary",
          modelRef: "openai/gpt-5.4",
        },
        route: {
          channel: "telegram",
          to: "telegram:123",
          sessionKey: "agent:main:main",
        },
      },
    });
    store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: Date.UTC(2026, 2, 9, 19, 26),
    });
    store.setProgress({
      id: "job-789",
      workerId: "worker-a",
      note: "Draft passed quality gate. Rendering PDF.",
      nowMs: Date.UTC(2026, 2, 9, 19, 27),
    });

    const text = buildQuickResearchStatusReply({
      dbPath,
      route: {
        channel: "telegram",
        to: "telegram:123",
        sessionKey: "agent:main:main",
      },
      nowMs: Date.UTC(2026, 2, 9, 19, 27),
    });

    expect(text).toContain("Quick status:");
    expect(text).toContain("Job: company v2 NVDA (5 min)");
    expect(text).toContain("Status: running");
    expect(text).toContain("Model: openai/gpt-5.4");
    expect(text).toContain("Progress: Draft passed quality gate. Rendering PDF.");
    expect(text).toContain("job_id=job-789");
  });

  it("builds a deterministic PDF re-delivery reply for the latest completed route job", () => {
    const dbPath = makeDbPath();
    const store = QuickrunJobStore.open(dbPath);
    store.enqueue({
      id: "job-900",
      jobType: "quick_research_pdf_v2",
      runAfterMs: 0,
      payload: {
        jobId: "job-900",
        request: {
          kind: "company",
          ticker: "NVDA",
          minutes: 5,
        },
        createdAtMs: Date.UTC(2026, 2, 9, 19, 25),
        deliverAtMs: Date.UTC(2026, 2, 9, 19, 30),
        route: {
          channel: "telegram",
          to: "telegram:123",
          sessionKey: "agent:main:main",
        },
      },
    });
    store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: Date.UTC(2026, 2, 9, 19, 26),
    });
    store.setResult({
      id: "job-900",
      workerId: "worker-a",
      text: "Company memo ready: NVDA",
      mediaUrl: "/tmp/nvda.pdf",
      runId: "run-900",
      nowMs: Date.UTC(2026, 2, 9, 19, 27),
    });
    store.markCompleted({
      id: "job-900",
      workerId: "worker-a",
      nowMs: Date.UTC(2026, 2, 9, 19, 28),
    });

    const reply = buildQuickResearchPdfFollowupReply({
      dbPath,
      route: {
        channel: "telegram",
        to: "telegram:123",
        sessionKey: "agent:main:main",
      },
    });

    expect(reply?.text).toContain("Company memo ready: NVDA");
    expect(reply?.mediaUrl).toBe("/tmp/nvda.pdf");
  });

  it("recovers the latest PDF artifact from the quickrun manifest for older completed jobs", () => {
    const dbPath = makeDbPath();
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "quickrun-state-"));
    tempDirs.push(stateDir);
    const pdfPath = path.join(stateDir, "nvda-latest.pdf");
    fs.writeFileSync(pdfPath, "pdf");
    const manifestDir = path.join(stateDir, "research", "quickrun");
    fs.mkdirSync(manifestDir, { recursive: true });
    fs.writeFileSync(
      path.join(manifestDir, "quickrun_company_nvda.artifact.json"),
      JSON.stringify(
        {
          outPath: pdfPath,
          metrics: {
            run_id: "run-manifest",
          },
        },
        null,
        2,
      ),
    );

    const priorStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_STATE_DIR = stateDir;
    try {
      const store = QuickrunJobStore.open(dbPath);
      store.enqueue({
        id: "job-901",
        jobType: "quick_research_pdf_v2",
        runAfterMs: 0,
        payload: {
          jobId: "job-901",
          request: {
            kind: "company",
            ticker: "NVDA",
            minutes: 5,
          },
          createdAtMs: Date.UTC(2026, 2, 9, 19, 25),
          deliverAtMs: Date.UTC(2026, 2, 9, 19, 30),
          route: {
            channel: "telegram",
            to: "telegram:123",
            sessionKey: "agent:main:main",
          },
        },
      });
      store.claimNext({
        jobType: "quick_research_pdf_v2",
        workerId: "worker-a",
        nowMs: Date.UTC(2026, 2, 9, 19, 26),
      });
      store.markCompleted({
        id: "job-901",
        workerId: "worker-a",
        nowMs: Date.UTC(2026, 2, 9, 19, 28),
      });

      const reply = buildQuickResearchPdfFollowupReply({
        dbPath,
        route: {
          channel: "telegram",
          to: "telegram:123",
          sessionKey: "agent:main:main",
        },
      });

      expect(reply?.mediaUrl).toBe(pdfPath);
      expect(reply?.text).toContain("run_id=run-manifest");
    } finally {
      if (priorStateDir == null) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = priorStateDir;
      }
    }
  });
});
