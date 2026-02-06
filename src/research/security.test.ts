import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runResearchSecurityAudit } from "./security.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-security-audit-${name}-${Date.now()}-${Math.random()}.db`);

describe("research security audit", () => {
  const envSnapshot = {
    RESEARCH_DB_ALLOW_EXTENSIONS: process.env.RESEARCH_DB_ALLOW_EXTENSIONS,
    RESEARCH_DB_REQUIRE_ENCRYPTION: process.env.RESEARCH_DB_REQUIRE_ENCRYPTION,
    RESEARCH_DB_KEY: process.env.RESEARCH_DB_KEY,
    RESEARCH_PROVENANCE_SECRET: process.env.RESEARCH_PROVENANCE_SECRET,
  };

  afterEach(() => {
    process.env.RESEARCH_DB_ALLOW_EXTENSIONS = envSnapshot.RESEARCH_DB_ALLOW_EXTENSIONS;
    process.env.RESEARCH_DB_REQUIRE_ENCRYPTION = envSnapshot.RESEARCH_DB_REQUIRE_ENCRYPTION;
    process.env.RESEARCH_DB_KEY = envSnapshot.RESEARCH_DB_KEY;
    process.env.RESEARCH_PROVENANCE_SECRET = envSnapshot.RESEARCH_PROVENANCE_SECRET;
  });

  it("reports extension and provenance controls", () => {
    process.env.RESEARCH_DB_ALLOW_EXTENSIONS = "false";
    process.env.RESEARCH_PROVENANCE_SECRET = "test-secret";
    const report = runResearchSecurityAudit({
      dbPath: testDbPath("controls"),
    });
    expect(report.controls.find((control) => control.id === "db_extension_loading")?.status).toBe(
      "pass",
    );
    expect(report.controls.find((control) => control.id === "provenance_signing")?.status).toBe(
      "pass",
    );
  });
});
