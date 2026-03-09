import { listProfilesForProvider, loadAuthProfileStore } from "../agents/auth-profiles.js";
import { loadConfig } from "../config/config.js";
import { openResearchDb } from "./db.js";
import { resolveResearchDbPath } from "./db.js";
import {
  listResearchUserPreferences,
  upsertResearchUserPreference,
} from "./external-research-personalization.js";

export type ResearchExecutionProfile = {
  key: string;
  label: string;
  modelRef: string;
  profileId?: string;
  source: "default" | "stored" | "preset" | "custom";
};

export type ResearchExecutionProfilePreset = {
  key: string;
  label: string;
  description: string;
  modelRef: string;
  profileId?: string;
};

const RESEARCH_MODEL_PROFILE_KEY = "research_model_profile";
const DEFAULT_RESEARCH_MODEL_REF = "openai/gpt-5.4";

const parseModelRef = (raw: string): { provider: string; model: string } | null => {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const idx = trimmed.indexOf("/");
  if (idx <= 0 || idx === trimmed.length - 1) return null;
  const provider = trimmed.slice(0, idx).trim().toLowerCase();
  const model = trimmed.slice(idx + 1).trim();
  if (!provider || !model) return null;
  return { provider, model };
};

const normalizeProfileKey = (value: string): string => value.trim().toLowerCase();

const defaultAuthProfileForProvider = (provider: string): string | undefined => {
  const store = loadAuthProfileStore();
  const ids = listProfilesForProvider(store, provider);
  return ids[0]?.trim() || undefined;
};

const ensureAuthProfileExists = (params: { provider: string; profileId?: string }) => {
  const profileId = params.profileId?.trim();
  if (!profileId) return;
  const store = loadAuthProfileStore();
  const ids = listProfilesForProvider(store, params.provider);
  if (!ids.includes(profileId)) {
    throw new Error(
      `Auth profile "${profileId}" is not available for provider "${params.provider}".`,
    );
  }
};

export const listResearchExecutionProfilePresets = (): ResearchExecutionProfilePreset[] => {
  const openrouterDefault = defaultAuthProfileForProvider("openrouter");
  return [
    {
      key: "primary",
      label: "Primary",
      description: "Direct OpenAI GPT-5.4 path for highest consistency on final memos.",
      modelRef: DEFAULT_RESEARCH_MODEL_REF,
    },
    {
      key: "openrouter-auto",
      label: "OpenRouter Auto",
      description: "OpenRouter managed routing for cheaper experiments and fallback runs.",
      modelRef: "openrouter/auto",
      ...(openrouterDefault ? { profileId: openrouterDefault } : {}),
    },
  ];
};

export const resolveResearchExecutionProfilePreset = (
  rawKey: string,
): ResearchExecutionProfilePreset | null => {
  const key = normalizeProfileKey(rawKey);
  return listResearchExecutionProfilePresets().find((preset) => preset.key === key) ?? null;
};

const coerceStoredProfile = (
  valueJson: Record<string, unknown>,
): ResearchExecutionProfile | null => {
  const modelRef = String(valueJson.modelRef ?? "").trim();
  if (!parseModelRef(modelRef)) return null;
  const key = String(valueJson.key ?? "").trim() || "custom";
  const label = String(valueJson.label ?? "").trim() || key;
  const sourceValue = String(valueJson.source ?? "")
    .trim()
    .toLowerCase();
  const source: ResearchExecutionProfile["source"] =
    sourceValue === "preset" ||
    sourceValue === "custom" ||
    sourceValue === "default" ||
    sourceValue === "stored"
      ? (sourceValue as ResearchExecutionProfile["source"])
      : "stored";
  const profileIdRaw = String(valueJson.profileId ?? "").trim();
  return {
    key,
    label,
    modelRef,
    ...(profileIdRaw ? { profileId: profileIdRaw } : {}),
    source,
  };
};

export const getStoredResearchExecutionProfile = (
  params: {
    dbPath?: string;
  } = {},
): ResearchExecutionProfile | null => {
  const rows = listResearchUserPreferences({ dbPath: params.dbPath });
  const row = rows.find((entry) => entry.key === RESEARCH_MODEL_PROFILE_KEY);
  if (!row) return null;
  return coerceStoredProfile(row.valueJson);
};

export const setStoredResearchExecutionProfile = (params: {
  profile: ResearchExecutionProfile;
  dbPath?: string;
}): ResearchExecutionProfile => {
  const parsed = parseModelRef(params.profile.modelRef);
  if (!parsed) {
    throw new Error(`Invalid research model ref: ${JSON.stringify(params.profile.modelRef)}`);
  }
  const row = upsertResearchUserPreference({
    key: RESEARCH_MODEL_PROFILE_KEY,
    valueText: params.profile.key,
    valueJson: {
      key: params.profile.key,
      label: params.profile.label,
      modelRef: params.profile.modelRef,
      ...(params.profile.profileId ? { profileId: params.profile.profileId } : {}),
      source: params.profile.source,
      provider: parsed.provider,
      model: parsed.model,
    },
    dbPath: params.dbPath,
  });
  return coerceStoredProfile(row.valueJson) ?? params.profile;
};

export const clearStoredResearchExecutionProfile = (
  params: {
    dbPath?: string;
  } = {},
): void => {
  const db = openResearchDb(params.dbPath);
  db.prepare(`DELETE FROM research_user_preferences WHERE preference_key=?`).run(
    RESEARCH_MODEL_PROFILE_KEY,
  );
};

export const resolveResearchExecutionProfile = (
  params: {
    dbPath?: string;
    env?: NodeJS.ProcessEnv;
  } = {},
): ResearchExecutionProfile => {
  const env = params.env ?? process.env;
  const stored = getStoredResearchExecutionProfile({ dbPath: params.dbPath });
  if (stored) {
    return {
      ...stored,
      source: stored.source === "default" ? "stored" : stored.source,
    };
  }

  const cfg = loadConfig();
  const configuredModel =
    env.OPENCLAW_RESEARCH_V2_MODEL?.trim() ||
    String(cfg.agents?.defaults?.model?.primary ?? "").trim() ||
    DEFAULT_RESEARCH_MODEL_REF;
  const configuredProfileId = env.OPENCLAW_RESEARCH_V2_PROFILE?.trim() || undefined;

  return {
    key: configuredProfileId ? "env-profile" : "primary",
    label: configuredProfileId ? "Configured profile" : "Primary",
    modelRef: configuredModel,
    ...(configuredProfileId ? { profileId: configuredProfileId } : {}),
    source: "default",
  };
};

export const buildResearchExecutionProfile = (params: {
  rawSelection: string;
  profileId?: string;
}): ResearchExecutionProfile => {
  const selection = params.rawSelection.trim();
  if (!selection) {
    throw new Error("research profile selection is required");
  }

  const preset = resolveResearchExecutionProfilePreset(selection);
  if (preset) {
    const selectedProfileId = params.profileId?.trim() || preset.profileId;
    const parsedPreset = parseModelRef(preset.modelRef)!;
    ensureAuthProfileExists({
      provider: parsedPreset.provider,
      profileId: selectedProfileId,
    });
    return {
      key: preset.key,
      label: preset.label,
      modelRef: preset.modelRef,
      ...(selectedProfileId ? { profileId: selectedProfileId } : {}),
      source: "preset",
    };
  }

  if (!parseModelRef(selection)) {
    throw new Error(
      `Invalid research profile target ${JSON.stringify(selection)}. Use a preset or provider/model.`,
    );
  }

  const parsed = parseModelRef(selection)!;
  const defaultProfileId =
    params.profileId?.trim() ||
    (parsed.provider === "openrouter" ? defaultAuthProfileForProvider("openrouter") : undefined);
  ensureAuthProfileExists({ provider: parsed.provider, profileId: defaultProfileId });

  return {
    key: `custom:${selection.toLowerCase()}`,
    label: selection,
    modelRef: selection,
    ...(defaultProfileId ? { profileId: defaultProfileId } : {}),
    source: "custom",
  };
};

export const resolveActiveResearchDbPath = (dbPath?: string): string =>
  resolveResearchDbPath(
    dbPath ??
      process.env.RESEARCH_DB_PATH?.trim() ??
      process.env.OPENCLAW_RESEARCH_DB_PATH?.trim() ??
      undefined,
  );
