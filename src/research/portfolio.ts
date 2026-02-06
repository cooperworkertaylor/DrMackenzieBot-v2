import { getCatalystSummary } from "./catalyst.js";
import { computeValuation } from "./valuation.js";
import { computeVariantPerception } from "./variant.js";

export type PortfolioPlan = {
  ticker: string;
  generatedAt: string;
  confidence: number;
  expectedUpsidePct?: number;
  catalystExpectedImpactPct: number;
  recommendedWeightPct: number;
  maxRiskBudgetPct: number;
  stopLossPct: number;
  timeHorizonDays: number;
  stance: "long" | "short" | "watch" | "insufficient-evidence";
  reviewTriggers: string[];
  rationale: string[];
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

export const computePortfolioPlan = (params: {
  ticker: string;
  dbPath?: string;
}): PortfolioPlan => {
  const ticker = params.ticker.trim().toUpperCase();
  const valuation = computeValuation({ ticker, dbPath: params.dbPath });
  const variant = computeVariantPerception({ ticker, dbPath: params.dbPath });
  const catalysts = getCatalystSummary({ ticker, dbPath: params.dbPath });

  const expectedUpsidePct =
    valuation.expectedUpsideWithCatalystsPct ?? valuation.expectedUpsidePct ?? undefined;
  const confidence = clamp(
    0.5 * valuation.confidence + 0.35 * variant.confidence + 0.15 * catalysts.weightedConfidence,
    0,
    1,
  );
  const convictionScore =
    typeof expectedUpsidePct === "number"
      ? clamp((expectedUpsidePct * 100) / 25, -1, 1) * confidence
      : 0;

  let stance: PortfolioPlan["stance"] = "watch";
  if (
    typeof expectedUpsidePct !== "number" ||
    confidence < 0.45 ||
    variant.stance === "insufficient-evidence"
  ) {
    stance = "insufficient-evidence";
  } else if (convictionScore >= 0.15) {
    stance = "long";
  } else if (convictionScore <= -0.15) {
    stance = "short";
  }

  const absConviction = Math.abs(convictionScore);
  const recommendedWeightPct =
    stance === "insufficient-evidence" ? 0 : clamp(absConviction * 10, 0.5, 10);
  const maxRiskBudgetPct =
    stance === "insufficient-evidence" ? 0 : clamp(recommendedWeightPct * 0.45, 0.5, 4);
  const stopLossPct =
    stance === "short"
      ? clamp(0.08 + (1 - confidence) * 0.1, 0.08, 0.2)
      : clamp(0.09 + (1 - confidence) * 0.11, 0.09, 0.22);
  const timeHorizonDays = catalysts.openCount > 0 ? 90 : 120;

  const reviewTriggers = [
    `Re-evaluate if variant confidence drops below ${(confidence - 0.15).toFixed(2)}.`,
    `Re-evaluate if ${stance === "short" ? "upside" : "downside"} move exceeds ${(stopLossPct * 100).toFixed(1)}% from entry.`,
    `Re-evaluate if catalyst expected impact changes by more than 150 bps.`,
    `Re-evaluate if valuation implied stance flips versus current stance (${valuation.impliedExpectations?.stance ?? "n/a"}).`,
  ];

  const rationale: string[] = [];
  if (typeof expectedUpsidePct === "number") {
    rationale.push(
      `Expected upside (catalyst-adjusted): ${(expectedUpsidePct * 100).toFixed(1)}%.`,
    );
  } else {
    rationale.push("Expected upside unavailable due to missing valuation coverage.");
  }
  rationale.push(
    `Variant stance: ${variant.stance} (confidence ${variant.confidence.toFixed(2)}).`,
  );
  rationale.push(
    `Catalyst profile: ${catalysts.openCount} open events, expected impact ${(catalysts.expectedImpactPct * 100).toFixed(2)}%.`,
  );

  return {
    ticker,
    generatedAt: new Date().toISOString(),
    confidence,
    expectedUpsidePct,
    catalystExpectedImpactPct: catalysts.expectedImpactPct,
    recommendedWeightPct,
    maxRiskBudgetPct,
    stopLossPct,
    timeHorizonDays,
    stance,
    reviewTriggers,
    rationale,
  };
};
