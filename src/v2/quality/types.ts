export type ReportKindV2 = "company" | "theme";

export type ThemeEntityTypeV2 =
  | "equity"
  | "crypto_asset"
  | "protocol"
  | "private_company"
  | "index"
  | "other";

export type ThemeUniverseEntityV2 = {
  /** Stable id, e.g. "eq:SHOP" or "protocol:uniswap". */
  id: string;
  type: ThemeEntityTypeV2;
  /** Display label, e.g. "Shopify" or "Uniswap". */
  label: string;
  /** Optional symbol/ticker for tradables (equities/crypto/index). */
  symbol?: string;
  /** Optional canonical URLs (docs/homepage/docs). */
  urls?: string[];
  notes?: string[];
};

export type QualitySeverity = "error" | "warn";

export type QualityIssue = {
  severity: QualitySeverity;
  code: string;
  message: string;
  /** JSON pointer-ish path (best-effort). */
  path?: string;
  /** Concrete suggested fix. */
  fix?: string;
};

export type QualityGateResult = {
  passed: boolean;
  issues: QualityIssue[];
};
