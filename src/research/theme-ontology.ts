import { openResearchDb } from "./db.js";
import { appendProvenanceEvent } from "./provenance.js";

export type ThemeRuleSet = {
  includeKeywords: string[];
  excludeKeywords: string[];
  requiredSectors: string[];
  excludedSectors: string[];
  requiredIndustries: string[];
  excludedIndustries: string[];
  tickerAllowlist: string[];
  tickerBlocklist: string[];
  minMembershipScore: number;
};

export type ThemeDefinition = {
  id: number;
  themeKey: string;
  version: number;
  displayName: string;
  description: string;
  parentThemeKey: string;
  benchmark: string;
  rules: ThemeRuleSet;
  status: "active" | "inactive" | "draft";
  effectiveFrom: string;
  effectiveTo: string;
  createdAt: number;
  updatedAt: number;
};

export type ThemeConstituent = {
  id: number;
  themeKey: string;
  themeVersion: number;
  ticker: string;
  membershipScore: number;
  confidence: number;
  status: "active" | "candidate" | "excluded" | "inactive";
  rationale: string;
  source: string;
  validFrom: string;
  validTo: string;
  metadata: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type ThemeMembershipRefreshResult = {
  generatedAt: string;
  theme: ThemeDefinition;
  candidatesScored: number;
  activeCount: number;
  candidateCount: number;
  excludedCount: number;
  constituents: ThemeConstituent[];
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const normalizeThemeKey = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const normalizeList = (values: string[] | undefined, uppercase = false): string[] =>
  Array.from(
    new Set(
      (values ?? [])
        .map((value) => value.trim())
        .filter(Boolean)
        .map((value) => (uppercase ? value.toUpperCase() : value.toLowerCase())),
    ),
  );

const defaultRules = (): ThemeRuleSet => ({
  includeKeywords: [],
  excludeKeywords: [],
  requiredSectors: [],
  excludedSectors: [],
  requiredIndustries: [],
  excludedIndustries: [],
  tickerAllowlist: [],
  tickerBlocklist: [],
  minMembershipScore: 0.55,
});

const normalizeRules = (rules?: Partial<ThemeRuleSet>): ThemeRuleSet => {
  const defaults = defaultRules();
  return {
    includeKeywords: normalizeList(rules?.includeKeywords, false),
    excludeKeywords: normalizeList(rules?.excludeKeywords, false),
    requiredSectors: normalizeList(rules?.requiredSectors, false),
    excludedSectors: normalizeList(rules?.excludedSectors, false),
    requiredIndustries: normalizeList(rules?.requiredIndustries, false),
    excludedIndustries: normalizeList(rules?.excludedIndustries, false),
    tickerAllowlist: normalizeList(rules?.tickerAllowlist, true),
    tickerBlocklist: normalizeList(rules?.tickerBlocklist, true),
    minMembershipScore: clamp(
      typeof rules?.minMembershipScore === "number"
        ? rules.minMembershipScore
        : defaults.minMembershipScore,
      0.2,
      0.95,
    ),
  };
};

const parseRules = (raw: unknown): ThemeRuleSet => {
  if (typeof raw !== "string" || !raw.trim()) return defaultRules();
  try {
    const parsed = JSON.parse(raw) as Partial<ThemeRuleSet>;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return defaultRules();
    return normalizeRules(parsed);
  } catch {
    return defaultRules();
  }
};

const parseMetadata = (raw: unknown): Record<string, unknown> => {
  if (typeof raw !== "string" || !raw.trim()) return {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
};

const mapThemeDefinition = (row: {
  id: number;
  theme_key: string;
  version: number;
  display_name: string;
  description: string;
  parent_theme_key: string;
  benchmark: string;
  rules: string;
  status: string;
  effective_from: string;
  effective_to: string;
  created_at: number;
  updated_at: number;
}): ThemeDefinition => ({
  id: row.id,
  themeKey: row.theme_key,
  version: row.version,
  displayName: row.display_name,
  description: row.description,
  parentThemeKey: row.parent_theme_key,
  benchmark: row.benchmark,
  rules: parseRules(row.rules),
  status: (row.status as ThemeDefinition["status"]) ?? "active",
  effectiveFrom: row.effective_from,
  effectiveTo: row.effective_to,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const mapThemeConstituent = (row: {
  id: number;
  theme_key: string;
  theme_version: number;
  ticker: string;
  membership_score: number;
  confidence: number;
  status: string;
  rationale: string;
  source: string;
  valid_from: string;
  valid_to: string;
  metadata: string;
  created_at: number;
  updated_at: number;
}): ThemeConstituent => ({
  id: row.id,
  themeKey: row.theme_key,
  themeVersion: row.theme_version,
  ticker: row.ticker,
  membershipScore: row.membership_score,
  confidence: row.confidence,
  status: (row.status as ThemeConstituent["status"]) ?? "candidate",
  rationale: row.rationale,
  source: row.source,
  validFrom: row.valid_from,
  validTo: row.valid_to,
  metadata: parseMetadata(row.metadata),
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

export const listThemeDefinitions = (
  params: {
    theme?: string;
    includeInactive?: boolean;
    dbPath?: string;
  } = {},
): ThemeDefinition[] => {
  const db = openResearchDb(params.dbPath);
  const themeKey = params.theme ? normalizeThemeKey(params.theme) : "";
  const includeInactive = Boolean(params.includeInactive);
  const rows = db
    .prepare(
      `SELECT
         id, theme_key, version, display_name, description, parent_theme_key, benchmark,
         rules, status, effective_from, effective_to, created_at, updated_at
       FROM theme_taxonomy
       WHERE (? = '' OR theme_key = ?)
         AND (? = 1 OR status = 'active')
       ORDER BY theme_key ASC, version DESC`,
    )
    .all(themeKey, themeKey, includeInactive ? 1 : 0) as Array<{
    id: number;
    theme_key: string;
    version: number;
    display_name: string;
    description: string;
    parent_theme_key: string;
    benchmark: string;
    rules: string;
    status: string;
    effective_from: string;
    effective_to: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map(mapThemeDefinition);
};

const resolveThemeDefinition = (params: {
  theme: string;
  version?: number;
  includeInactive?: boolean;
  dbPath?: string;
}): ThemeDefinition | undefined => {
  const db = openResearchDb(params.dbPath);
  const themeKey = normalizeThemeKey(params.theme);
  if (!themeKey) return undefined;
  if (typeof params.version === "number" && Number.isFinite(params.version)) {
    const row = db
      .prepare(
        `SELECT
           id, theme_key, version, display_name, description, parent_theme_key, benchmark,
           rules, status, effective_from, effective_to, created_at, updated_at
         FROM theme_taxonomy
         WHERE theme_key = ? AND version = ?
         LIMIT 1`,
      )
      .get(themeKey, Math.round(params.version)) as
      | {
          id: number;
          theme_key: string;
          version: number;
          display_name: string;
          description: string;
          parent_theme_key: string;
          benchmark: string;
          rules: string;
          status: string;
          effective_from: string;
          effective_to: string;
          created_at: number;
          updated_at: number;
        }
      | undefined;
    return row ? mapThemeDefinition(row) : undefined;
  }
  const includeInactive = Boolean(params.includeInactive);
  const row = db
    .prepare(
      `SELECT
         id, theme_key, version, display_name, description, parent_theme_key, benchmark,
         rules, status, effective_from, effective_to, created_at, updated_at
       FROM theme_taxonomy
       WHERE theme_key = ?
         AND (? = 1 OR status = 'active')
       ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END, version DESC
       LIMIT 1`,
    )
    .get(themeKey, includeInactive ? 1 : 0) as
    | {
        id: number;
        theme_key: string;
        version: number;
        display_name: string;
        description: string;
        parent_theme_key: string;
        benchmark: string;
        rules: string;
        status: string;
        effective_from: string;
        effective_to: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  return row ? mapThemeDefinition(row) : undefined;
};

export const upsertThemeDefinition = (params: {
  theme: string;
  displayName?: string;
  description?: string;
  parentTheme?: string;
  benchmark?: string;
  rules?: Partial<ThemeRuleSet>;
  version?: number;
  activate?: boolean;
  status?: ThemeDefinition["status"];
  effectiveFrom?: string;
  effectiveTo?: string;
  dbPath?: string;
}): ThemeDefinition => {
  const themeKey = normalizeThemeKey(params.theme);
  if (!themeKey) throw new Error("theme is required");
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const rules = normalizeRules(params.rules);
  const existingLatest = db
    .prepare(
      `SELECT MAX(version) AS version
       FROM theme_taxonomy
       WHERE theme_key = ?`,
    )
    .get(themeKey) as { version?: number | null };
  const resolvedVersion =
    typeof params.version === "number" && Number.isFinite(params.version)
      ? Math.max(1, Math.round(params.version))
      : Math.max(1, (existingLatest.version ?? 0) + 1);
  const existing = resolveThemeDefinition({
    theme: themeKey,
    version: resolvedVersion,
    includeInactive: true,
    dbPath: params.dbPath,
  });
  const displayName = params.displayName?.trim() || params.theme.trim();
  const status = params.activate ? "active" : (params.status ?? existing?.status ?? "active");
  const description = params.description?.trim() ?? existing?.description ?? "";
  const parentThemeKey = params.parentTheme ? normalizeThemeKey(params.parentTheme) : "";
  const benchmark = params.benchmark?.trim() ?? existing?.benchmark ?? "";
  const effectiveFrom = params.effectiveFrom?.trim() ?? existing?.effectiveFrom ?? "";
  const effectiveTo = params.effectiveTo?.trim() ?? existing?.effectiveTo ?? "";

  if (existing) {
    db.prepare(
      `UPDATE theme_taxonomy
       SET display_name=?, description=?, parent_theme_key=?, benchmark=?, rules=?, status=?,
           effective_from=?, effective_to=?, updated_at=?
       WHERE theme_key=? AND version=?`,
    ).run(
      displayName,
      description,
      parentThemeKey,
      benchmark,
      JSON.stringify(rules),
      status,
      effectiveFrom,
      effectiveTo,
      now,
      themeKey,
      resolvedVersion,
    );
  } else {
    db.prepare(
      `INSERT INTO theme_taxonomy (
         theme_key, version, display_name, description, parent_theme_key, benchmark, rules,
         status, effective_from, effective_to, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      themeKey,
      resolvedVersion,
      displayName,
      description,
      parentThemeKey,
      benchmark,
      JSON.stringify(rules),
      status,
      effectiveFrom,
      effectiveTo,
      now,
      now,
    );
  }
  if (status === "active") {
    db.prepare(
      `UPDATE theme_taxonomy
       SET status='inactive', updated_at=?
       WHERE theme_key=? AND version<>? AND status<>'inactive'`,
    ).run(now, themeKey, resolvedVersion);
  }
  const definition = resolveThemeDefinition({
    theme: themeKey,
    version: resolvedVersion,
    includeInactive: true,
    dbPath: params.dbPath,
  });
  if (!definition) throw new Error("Failed to persist theme definition.");
  try {
    appendProvenanceEvent({
      eventType: "theme_taxonomy_upsert",
      entityType: "theme",
      entityId: `${themeKey}:v${resolvedVersion}`,
      payload: {
        theme: themeKey,
        version: resolvedVersion,
        status,
        benchmark,
      },
      metadata: {
        display_name: displayName,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Keep taxonomy update path resilient to provenance failures.
  }
  return definition;
};

const loadInstruments = (params: {
  tickers?: string[];
  dbPath?: string;
}): Array<{
  ticker: string;
  name: string;
  sector: string;
  industry: string;
}> => {
  const db = openResearchDb(params.dbPath);
  if (params.tickers?.length) {
    const tickers = Array.from(new Set(params.tickers.map(normalizeTicker).filter(Boolean)));
    if (!tickers.length) return [];
    const placeholders = tickers.map(() => "?").join(",");
    const rows = db
      .prepare(
        `SELECT
           UPPER(i.ticker) AS ticker,
           COALESCE(NULLIF(TRIM(i.name), ''), UPPER(i.ticker)) AS name,
           COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
           COALESCE(NULLIF(TRIM(i.industry), ''), 'Unknown') AS industry
         FROM instruments i
         WHERE UPPER(i.ticker) IN (${placeholders})
         ORDER BY i.ticker ASC`,
      )
      .all(...tickers) as Array<{
      ticker: string;
      name: string;
      sector: string;
      industry: string;
    }>;
    const found = new Set(rows.map((row) => normalizeTicker(row.ticker)));
    const fallback = tickers
      .filter((ticker) => !found.has(ticker))
      .map((ticker) => ({
        ticker,
        name: ticker,
        sector: "Unknown",
        industry: "Unknown",
      }));
    return [...rows, ...fallback];
  }
  const rows = db
    .prepare(
      `SELECT
         UPPER(i.ticker) AS ticker,
         COALESCE(NULLIF(TRIM(i.name), ''), UPPER(i.ticker)) AS name,
         COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
         COALESCE(NULLIF(TRIM(i.industry), ''), 'Unknown') AS industry
       FROM instruments i
       WHERE TRIM(COALESCE(i.ticker, '')) <> ''
       ORDER BY i.ticker ASC`,
    )
    .all() as Array<{
    ticker: string;
    name: string;
    sector: string;
    industry: string;
  }>;
  return rows;
};

const evaluateMembership = (params: {
  ticker: string;
  name: string;
  sector: string;
  industry: string;
  rules: ThemeRuleSet;
  minScore: number;
  themeKey: string;
}): {
  membershipScore: number;
  confidence: number;
  status: ThemeConstituent["status"];
  rationale: string;
  metadata: Record<string, unknown>;
} => {
  const ticker = normalizeTicker(params.ticker);
  const sectorNorm = params.sector.toLowerCase();
  const industryNorm = params.industry.toLowerCase();
  const blob = `${params.name} ${params.sector} ${params.industry}`.toLowerCase();
  const reasons: string[] = [];
  let score = 0.2;
  let includeHits = 0;
  let excludeHits = 0;

  if (params.rules.tickerBlocklist.includes(ticker)) {
    reasons.push("Ticker is explicitly blocked.");
    return {
      membershipScore: 0,
      confidence: 1,
      status: "excluded",
      rationale: reasons.join(" "),
      metadata: {
        include_hits: includeHits,
        exclude_hits: excludeHits,
      },
    };
  }
  if (params.rules.tickerAllowlist.includes(ticker)) {
    score += 0.45;
    reasons.push("Ticker allowlist match.");
  }
  if (blob.includes(params.themeKey)) {
    score += 0.08;
    reasons.push("Theme key appears in metadata text.");
  }

  if (params.rules.requiredSectors.length) {
    if (params.rules.requiredSectors.includes(sectorNorm)) {
      score += 0.22;
      reasons.push("Required sector match.");
    } else {
      score -= 0.26;
      reasons.push("Missing required sector.");
    }
  }
  if (params.rules.excludedSectors.includes(sectorNorm)) {
    score -= 0.32;
    excludeHits += 1;
    reasons.push("Sector is explicitly excluded.");
  }

  if (params.rules.requiredIndustries.length) {
    const hit = params.rules.requiredIndustries.some((term) => industryNorm.includes(term));
    if (hit) {
      score += 0.18;
      reasons.push("Required industry match.");
    } else {
      score -= 0.2;
      reasons.push("Missing required industry.");
    }
  }
  if (params.rules.excludedIndustries.some((term) => industryNorm.includes(term))) {
    score -= 0.3;
    excludeHits += 1;
    reasons.push("Industry is explicitly excluded.");
  }

  for (const keyword of params.rules.includeKeywords) {
    if (!keyword || !blob.includes(keyword)) continue;
    includeHits += 1;
    score += 0.08;
  }
  for (const keyword of params.rules.excludeKeywords) {
    if (!keyword || !blob.includes(keyword)) continue;
    excludeHits += 1;
    score -= 0.16;
  }
  if (params.rules.includeKeywords.length && includeHits === 0) {
    score -= 0.1;
    reasons.push("No include keyword match.");
  }
  if (excludeHits > 0) {
    reasons.push(`Excluded keyword/constraint hits: ${excludeHits}.`);
  }
  if (includeHits > 0) {
    reasons.push(`Include keyword hits: ${includeHits}.`);
  }

  const membershipScore = clamp(score, 0, 1);
  const confidence = clamp(
    0.45 +
      0.2 * Math.min(1, includeHits / Math.max(1, params.rules.includeKeywords.length || 1)) +
      0.2 * (params.rules.tickerAllowlist.includes(ticker) ? 1 : 0) +
      0.15 * (params.rules.requiredSectors.length ? 1 : 0) +
      0.1 * (params.rules.requiredIndustries.length ? 1 : 0) -
      0.2 * Math.min(1, excludeHits / 2),
    0.05,
    1,
  );
  let status: ThemeConstituent["status"] = "candidate";
  if (membershipScore >= params.minScore) status = "active";
  else if (membershipScore <= Math.max(0.12, params.minScore * 0.55)) status = "excluded";
  return {
    membershipScore,
    confidence,
    status,
    rationale: reasons.join(" "),
    metadata: {
      include_hits: includeHits,
      exclude_hits: excludeHits,
      sector: params.sector,
      industry: params.industry,
      name: params.name,
    },
  };
};

export const refreshThemeMembership = (params: {
  theme: string;
  version?: number;
  tickers?: string[];
  minMembershipScore?: number;
  source?: string;
  dbPath?: string;
}): ThemeMembershipRefreshResult => {
  const theme = resolveThemeDefinition({
    theme: params.theme,
    version: params.version,
    includeInactive: true,
    dbPath: params.dbPath,
  });
  if (!theme) throw new Error("Theme definition not found.");
  const db = openResearchDb(params.dbPath);
  const minScore = clamp(
    typeof params.minMembershipScore === "number"
      ? params.minMembershipScore
      : theme.rules.minMembershipScore,
    0.2,
    0.95,
  );
  const source = params.source?.trim() || "rule_engine";
  const instruments = loadInstruments({
    tickers: params.tickers,
    dbPath: params.dbPath,
  });
  if (!instruments.length) throw new Error("No instruments available to score.");
  const now = Date.now();
  const today = new Date().toISOString().slice(0, 10);
  const seenTickers = new Set<string>();
  for (const instrument of instruments) {
    const evaluation = evaluateMembership({
      ticker: instrument.ticker,
      name: instrument.name,
      sector: instrument.sector,
      industry: instrument.industry,
      rules: theme.rules,
      minScore,
      themeKey: theme.themeKey,
    });
    const ticker = normalizeTicker(instrument.ticker);
    seenTickers.add(ticker);
    const existing = db
      .prepare(
        `SELECT id
         FROM theme_constituents
         WHERE theme_key=? AND theme_version=? AND ticker=?
         LIMIT 1`,
      )
      .get(theme.themeKey, theme.version, ticker) as { id?: number } | undefined;
    if (typeof existing?.id === "number") {
      db.prepare(
        `UPDATE theme_constituents
         SET membership_score=?, confidence=?, status=?, rationale=?, source=?, valid_from=?,
             valid_to='', metadata=?, updated_at=?
         WHERE id=?`,
      ).run(
        evaluation.membershipScore,
        evaluation.confidence,
        evaluation.status,
        evaluation.rationale,
        source,
        today,
        JSON.stringify(evaluation.metadata),
        now,
        existing.id,
      );
    } else {
      db.prepare(
        `INSERT INTO theme_constituents (
           theme_key, theme_version, ticker, membership_score, confidence, status, rationale,
           source, valid_from, valid_to, metadata, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)`,
      ).run(
        theme.themeKey,
        theme.version,
        ticker,
        evaluation.membershipScore,
        evaluation.confidence,
        evaluation.status,
        evaluation.rationale,
        source,
        today,
        JSON.stringify(evaluation.metadata),
        now,
        now,
      );
    }
  }
  if (params.tickers?.length) {
    const staleRows = db
      .prepare(
        `SELECT id, ticker
         FROM theme_constituents
         WHERE theme_key=? AND theme_version=?`,
      )
      .all(theme.themeKey, theme.version) as Array<{ id: number; ticker: string }>;
    for (const row of staleRows) {
      if (seenTickers.has(normalizeTicker(row.ticker))) continue;
      db.prepare(
        `UPDATE theme_constituents
         SET status='inactive', valid_to=?, updated_at=?
         WHERE id=?`,
      ).run(today, now, row.id);
    }
  }

  const constituents = getThemeConstituents({
    theme: theme.themeKey,
    version: theme.version,
    includeInactive: true,
    dbPath: params.dbPath,
  });
  const activeCount = constituents.filter((row) => row.status === "active").length;
  const candidateCount = constituents.filter((row) => row.status === "candidate").length;
  const excludedCount = constituents.filter((row) => row.status === "excluded").length;
  try {
    appendProvenanceEvent({
      eventType: "theme_membership_refresh",
      entityType: "theme",
      entityId: `${theme.themeKey}:v${theme.version}`,
      payload: {
        theme: theme.themeKey,
        version: theme.version,
        scored: instruments.length,
        active: activeCount,
        candidate: candidateCount,
        excluded: excludedCount,
        min_score: minScore,
      },
      metadata: {
        source,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Keep membership refresh path resilient to provenance write failures.
  }
  return {
    generatedAt: new Date().toISOString(),
    theme,
    candidatesScored: instruments.length,
    activeCount,
    candidateCount,
    excludedCount,
    constituents,
  };
};

export const getThemeConstituents = (params: {
  theme: string;
  version?: number;
  status?: ThemeConstituent["status"] | "all";
  minMembershipScore?: number;
  includeInactive?: boolean;
  limit?: number;
  dbPath?: string;
}): ThemeConstituent[] => {
  const theme = resolveThemeDefinition({
    theme: params.theme,
    version: params.version,
    includeInactive: true,
    dbPath: params.dbPath,
  });
  if (!theme) return [];
  const db = openResearchDb(params.dbPath);
  const status = params.status ?? "active";
  const includeInactive = Boolean(params.includeInactive);
  const minScore =
    typeof params.minMembershipScore === "number" && Number.isFinite(params.minMembershipScore)
      ? params.minMembershipScore
      : 0;
  const limit = Math.max(1, Math.round(params.limit ?? 500));
  const rows = db
    .prepare(
      `SELECT
         id, theme_key, theme_version, ticker, membership_score, confidence, status,
         rationale, source, valid_from, valid_to, metadata, created_at, updated_at
       FROM theme_constituents
       WHERE theme_key=?
         AND theme_version=?
         AND (? = 1 OR status <> 'inactive')
         AND (? = 'all' OR status = ?)
         AND membership_score >= ?
       ORDER BY membership_score DESC, ticker ASC
       LIMIT ?`,
    )
    .all(
      theme.themeKey,
      theme.version,
      includeInactive ? 1 : 0,
      status,
      status,
      minScore,
      limit,
    ) as Array<{
    id: number;
    theme_key: string;
    theme_version: number;
    ticker: string;
    membership_score: number;
    confidence: number;
    status: string;
    rationale: string;
    source: string;
    valid_from: string;
    valid_to: string;
    metadata: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map(mapThemeConstituent);
};

export const resolveThemeTickerUniverse = (params: {
  theme: string;
  version?: number;
  minMembershipScore?: number;
  limit?: number;
  dbPath?: string;
}): string[] =>
  getThemeConstituents({
    theme: params.theme,
    version: params.version,
    minMembershipScore: params.minMembershipScore,
    status: "active",
    limit: params.limit,
    dbPath: params.dbPath,
  }).map((row) => row.ticker);
