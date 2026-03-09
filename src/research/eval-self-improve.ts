import fs from "node:fs";
import path from "node:path";
import {
  renderResearchEvalScorecard,
  runResearchEvalTaskSet,
  type ResearchEvalHarnessResult,
  type ResearchEvalImprovementProfile,
  type ResearchEvalTaskSet,
} from "./eval-harness.js";

type TunableSpec = {
  path: "report.lookbackDays" | "report.maxSources" | "report.maxClaims" | "report.maxEvents" | "report.maxFacts" | "watchlistBrief.lookbackDays";
  min: number;
  max: number;
  step: number;
};

export type ResearchSelfImproveDecision = "keep" | "revert";

export type ResearchSelfImproveAttempt = {
  attempt: number;
  mutationPath: TunableSpec["path"];
  previousValue: number;
  candidateValue: number;
  decision: ResearchSelfImproveDecision;
  reason: string;
  result: {
    score: number;
    passed: number;
    total: number;
    failedChecks: number;
    passedGate: boolean;
  };
};

export type ResearchSelfImproveRun = {
  generatedAt: string;
  taskSetName: string;
  profilePath: string;
  baseline: ResearchEvalHarnessResult;
  best: ResearchEvalHarnessResult;
  initialProfile: ResearchEvalImprovementProfile;
  finalProfile: ResearchEvalImprovementProfile;
  appliedImprovement: boolean;
  keepRules: string[];
  revertRules: string[];
  attempts: ResearchSelfImproveAttempt[];
  markdown: string;
};

const TUNABLES: TunableSpec[] = [
  { path: "report.lookbackDays", min: 21, max: 120, step: 7 },
  { path: "report.maxSources", min: 3, max: 12, step: 1 },
  { path: "report.maxClaims", min: 4, max: 12, step: 1 },
  { path: "report.maxEvents", min: 2, max: 8, step: 1 },
  { path: "report.maxFacts", min: 2, max: 8, step: 1 },
  { path: "watchlistBrief.lookbackDays", min: 1, max: 7, step: 1 },
];

const DEFAULT_PROFILE: ResearchEvalImprovementProfile = {
  version: 1,
  report: {
    lookbackDays: 45,
    maxSources: 8,
    maxClaims: 8,
    maxEvents: 6,
    maxFacts: 6,
  },
  watchlistBrief: {
    lookbackDays: 1,
  },
};

const clamp = (value: number, min: number, max: number): number => Math.max(min, Math.min(max, value));

const cloneProfile = (profile: ResearchEvalImprovementProfile): ResearchEvalImprovementProfile =>
  JSON.parse(JSON.stringify(profile)) as ResearchEvalImprovementProfile;

const normalizeProfile = (
  input?: Partial<ResearchEvalImprovementProfile> | null,
): ResearchEvalImprovementProfile => ({
  version: 1,
  report: {
    lookbackDays: clamp(Math.round(input?.report?.lookbackDays ?? DEFAULT_PROFILE.report!.lookbackDays!), 21, 120),
    maxSources: clamp(Math.round(input?.report?.maxSources ?? DEFAULT_PROFILE.report!.maxSources!), 3, 12),
    maxClaims: clamp(Math.round(input?.report?.maxClaims ?? DEFAULT_PROFILE.report!.maxClaims!), 4, 12),
    maxEvents: clamp(Math.round(input?.report?.maxEvents ?? DEFAULT_PROFILE.report!.maxEvents!), 2, 8),
    maxFacts: clamp(Math.round(input?.report?.maxFacts ?? DEFAULT_PROFILE.report!.maxFacts!), 2, 8),
  },
  watchlistBrief: {
    lookbackDays: clamp(
      Math.round(input?.watchlistBrief?.lookbackDays ?? DEFAULT_PROFILE.watchlistBrief!.lookbackDays!),
      1,
      7,
    ),
  },
});

const hashSeed = (value: string): number => {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
};

const createRng = (seedText: string) => {
  let state = hashSeed(seedText) || 1;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x1_0000_0000;
  };
};

const getTunableValue = (profile: ResearchEvalImprovementProfile, tunable: TunableSpec): number => {
  switch (tunable.path) {
    case "report.lookbackDays":
      return profile.report?.lookbackDays ?? DEFAULT_PROFILE.report!.lookbackDays!;
    case "report.maxSources":
      return profile.report?.maxSources ?? DEFAULT_PROFILE.report!.maxSources!;
    case "report.maxClaims":
      return profile.report?.maxClaims ?? DEFAULT_PROFILE.report!.maxClaims!;
    case "report.maxEvents":
      return profile.report?.maxEvents ?? DEFAULT_PROFILE.report!.maxEvents!;
    case "report.maxFacts":
      return profile.report?.maxFacts ?? DEFAULT_PROFILE.report!.maxFacts!;
    case "watchlistBrief.lookbackDays":
      return profile.watchlistBrief?.lookbackDays ?? DEFAULT_PROFILE.watchlistBrief!.lookbackDays!;
  }
};

const setTunableValue = (
  profile: ResearchEvalImprovementProfile,
  tunable: TunableSpec,
  value: number,
): void => {
  if (tunable.path.startsWith("report.")) {
    profile.report ??= {};
  } else {
    profile.watchlistBrief ??= {};
  }
  switch (tunable.path) {
    case "report.lookbackDays":
      profile.report!.lookbackDays = value;
      return;
    case "report.maxSources":
      profile.report!.maxSources = value;
      return;
    case "report.maxClaims":
      profile.report!.maxClaims = value;
      return;
    case "report.maxEvents":
      profile.report!.maxEvents = value;
      return;
    case "report.maxFacts":
      profile.report!.maxFacts = value;
      return;
    case "watchlistBrief.lookbackDays":
      profile.watchlistBrief!.lookbackDays = value;
  }
};

const pickCandidateMutation = (
  profile: ResearchEvalImprovementProfile,
  attempt: number,
  rng: () => number,
): { tunable: TunableSpec; previousValue: number; candidateValue: number } => {
  for (let tries = 0; tries < TUNABLES.length * 2; tries += 1) {
    const tunable = TUNABLES[(attempt - 1 + tries) % TUNABLES.length]!;
    const previousValue = getTunableValue(profile, tunable);
    const direction = rng() >= 0.5 ? 1 : -1;
    const candidateValue = clamp(previousValue + direction * tunable.step, tunable.min, tunable.max);
    if (candidateValue !== previousValue) {
      return { tunable, previousValue, candidateValue };
    }
  }
  const fallback = TUNABLES[attempt % TUNABLES.length]!;
  const previousValue = getTunableValue(profile, fallback);
  const candidateValue =
    previousValue + fallback.step <= fallback.max
      ? previousValue + fallback.step
      : previousValue - fallback.step;
  return {
    tunable: fallback,
    previousValue,
    candidateValue: clamp(candidateValue, fallback.min, fallback.max),
  };
};

const loadProfileFromPath = (profilePath: string): ResearchEvalImprovementProfile => {
  if (!fs.existsSync(profilePath)) {
    return cloneProfile(DEFAULT_PROFILE);
  }
  const parsed = JSON.parse(fs.readFileSync(profilePath, "utf8")) as Partial<ResearchEvalImprovementProfile>;
  return normalizeProfile(parsed);
};

export const saveResearchEvalImprovementProfile = (params: {
  profile: ResearchEvalImprovementProfile;
  profilePath: string;
}): void => {
  const resolved = path.resolve(params.profilePath);
  fs.mkdirSync(path.dirname(resolved), { recursive: true });
  fs.writeFileSync(resolved, `${JSON.stringify(normalizeProfile(params.profile), null, 2)}\n`, "utf8");
};

export const loadResearchEvalImprovementProfile = (profilePath: string): ResearchEvalImprovementProfile =>
  loadProfileFromPath(path.resolve(profilePath));

const shouldKeepCandidate = (params: {
  candidate: ResearchEvalHarnessResult;
  best: ResearchEvalHarnessResult;
  minImprovement: number;
}): { keep: boolean; reason: string } => {
  if (!params.candidate.passedGate) {
    return { keep: false, reason: "candidate failed the eval gate" };
  }
  const scoreDelta = params.candidate.score - params.best.score;
  if (scoreDelta >= params.minImprovement) {
    return {
      keep: true,
      reason: `score improved from ${params.best.score.toFixed(3)} to ${params.candidate.score.toFixed(3)}`,
    };
  }
  if (Math.abs(scoreDelta) < 1e-9 && params.candidate.failedChecks < params.best.failedChecks) {
    return {
      keep: true,
      reason: `score held at ${params.candidate.score.toFixed(3)} while failed checks dropped from ${params.best.failedChecks} to ${params.candidate.failedChecks}`,
    };
  }
  if (
    Math.abs(scoreDelta) < 1e-9 &&
    params.candidate.failedChecks === params.best.failedChecks &&
    params.candidate.passed > params.best.passed
  ) {
    return {
      keep: true,
      reason: `score held at ${params.candidate.score.toFixed(3)} while passed checks increased from ${params.best.passed} to ${params.candidate.passed}`,
    };
  }
  return {
    keep: false,
    reason: `candidate did not beat best score=${params.best.score.toFixed(3)} failed_checks=${params.best.failedChecks}`,
  };
};

const renderSelfImproveMarkdown = (run: ResearchSelfImproveRun): string => {
  const lines: string[] = [];
  lines.push(`# Research Self-Improve Run: ${run.taskSetName}`);
  lines.push("");
  lines.push(`- Generated at: ${run.generatedAt}`);
  lines.push(`- Profile path: ${run.profilePath}`);
  lines.push(`- Applied improvement: ${run.appliedImprovement ? "yes" : "no"}`);
  lines.push(
    `- Baseline score: ${(run.baseline.score * 100).toFixed(1)}% | Best score: ${(run.best.score * 100).toFixed(1)}%`,
  );
  lines.push("");
  lines.push("## Keep Rules");
  lines.push("");
  run.keepRules.forEach((rule) => lines.push(`- ${rule}`));
  lines.push("");
  lines.push("## Revert Rules");
  lines.push("");
  run.revertRules.forEach((rule) => lines.push(`- ${rule}`));
  lines.push("");
  lines.push("## Attempts");
  lines.push("");
  run.attempts.forEach((attempt) => {
    lines.push(
      `- #${attempt.attempt} ${attempt.mutationPath}: ${attempt.previousValue} -> ${attempt.candidateValue} | ${attempt.decision.toUpperCase()} | score=${(attempt.result.score * 100).toFixed(1)}% | failed=${attempt.result.failedChecks} | ${attempt.reason}`,
    );
  });
  if (!run.attempts.length) {
    lines.push("- No attempts executed.");
  }
  lines.push("");
  lines.push("## Baseline Scorecard");
  lines.push("");
  lines.push(renderResearchEvalScorecard(run.baseline));
  lines.push("");
  lines.push("## Best Scorecard");
  lines.push("");
  lines.push(renderResearchEvalScorecard(run.best));
  lines.push("");
  return lines.join("\n");
};

export const runResearchEvalSelfImproveLoop = async (params: {
  taskSet: ResearchEvalTaskSet;
  profilePath: string;
  attempts?: number;
  minImprovement?: number;
  seed?: string;
  dbPath?: string;
  writeBest?: boolean;
}): Promise<ResearchSelfImproveRun> => {
  const resolvedProfilePath = path.resolve(params.profilePath);
  const initialProfile = loadProfileFromPath(resolvedProfilePath);
  const baseline = await runResearchEvalTaskSet({
    taskSet: params.taskSet,
    dbPath: params.dbPath,
    profile: initialProfile,
  });
  const rng = createRng(
    `${params.seed ?? "research-self-improve"}:${params.taskSet.name}:${resolvedProfilePath}`,
  );
  const maxAttempts = Math.max(1, Math.round(params.attempts ?? 8));
  const minImprovement = Math.max(0, params.minImprovement ?? 0.005);
  let bestProfile = cloneProfile(initialProfile);
  let bestResult = baseline;
  const attempts: ResearchSelfImproveAttempt[] = [];

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const candidateProfile = cloneProfile(bestProfile);
    const mutation = pickCandidateMutation(candidateProfile, attempt, rng);
    setTunableValue(candidateProfile, mutation.tunable, mutation.candidateValue);
    const candidate = await runResearchEvalTaskSet({
      taskSet: params.taskSet,
      dbPath: params.dbPath,
      profile: candidateProfile,
    });
    const decision = shouldKeepCandidate({
      candidate,
      best: bestResult,
      minImprovement,
    });
    if (decision.keep) {
      bestProfile = normalizeProfile(candidateProfile);
      bestResult = candidate;
    }
    attempts.push({
      attempt,
      mutationPath: mutation.tunable.path,
      previousValue: mutation.previousValue,
      candidateValue: mutation.candidateValue,
      decision: decision.keep ? "keep" : "revert",
      reason: decision.reason,
      result: {
        score: candidate.score,
        passed: candidate.passed,
        total: candidate.total,
        failedChecks: candidate.failedChecks,
        passedGate: candidate.passedGate,
      },
    });
  }

  const appliedImprovement =
    bestResult.passedGate &&
    (bestResult.score > baseline.score ||
      bestResult.failedChecks < baseline.failedChecks ||
      bestResult.passed > baseline.passed);
  if (params.writeBest !== false) {
    saveResearchEvalImprovementProfile({
      profile: appliedImprovement ? bestProfile : initialProfile,
      profilePath: resolvedProfilePath,
    });
  }
  const run: ResearchSelfImproveRun = {
    generatedAt: new Date().toISOString(),
    taskSetName: params.taskSet.name,
    profilePath: resolvedProfilePath,
    baseline,
    best: bestResult,
    initialProfile,
    finalProfile: appliedImprovement ? bestProfile : initialProfile,
    appliedImprovement,
    keepRules: [
      `candidate must pass the eval gate`,
      `candidate score must improve by at least ${minImprovement.toFixed(3)}, or hold score while reducing failed checks`,
      `mutations are limited to whitelisted numeric tuning knobs in the profile file`,
    ],
    revertRules: [
      `revert any candidate that fails the eval gate`,
      `revert any candidate that does not beat the current best result under the keep rules`,
      `if no accepted candidate improves on baseline, restore the original profile`,
    ],
    attempts,
    markdown: "",
  };
  run.markdown = renderSelfImproveMarkdown(run);
  return run;
};
