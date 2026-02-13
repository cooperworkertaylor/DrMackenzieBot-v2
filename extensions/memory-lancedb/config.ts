import fs from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export type MemoryConfig = {
  embedding: {
    provider: "openai";
    model?: string;
    apiKey: string;
  };
  dbPath?: string;
  autoCapture?: boolean;
  autoRecall?: boolean;
  retrieval?: {
    hybridEnabled?: boolean;
    maxResults?: number;
    maxTokensPerSnippet?: number;
    vectorLimit?: number;
    ftsLimit?: number;
    rewriteCount?: number;
    rrfK?: number;
  };
  chunking?: {
    targetTokens?: number;
    minTokens?: number;
    maxTokens?: number;
    overlapRatio?: number;
  };
};

export const MEMORY_CATEGORIES = ["preference", "fact", "decision", "entity", "other"] as const;
export type MemoryCategory = (typeof MEMORY_CATEGORIES)[number];

const DEFAULT_MODEL = "text-embedding-3-small";
const LEGACY_STATE_DIRS: string[] = [];

function resolveDefaultDbPath(): string {
  const home = homedir();
  const preferred = join(home, ".openclaw", "memory", "lancedb");
  try {
    if (fs.existsSync(preferred)) {
      return preferred;
    }
  } catch {
    // best-effort
  }

  for (const legacy of LEGACY_STATE_DIRS) {
    const candidate = join(home, legacy, "memory", "lancedb");
    try {
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    } catch {
      // best-effort
    }
  }

  return preferred;
}

const DEFAULT_DB_PATH = resolveDefaultDbPath();

const EMBEDDING_DIMENSIONS: Record<string, number> = {
  "text-embedding-3-small": 1536,
  "text-embedding-3-large": 3072,
};

function assertAllowedKeys(value: Record<string, unknown>, allowed: string[], label: string) {
  const unknown = Object.keys(value).filter((key) => !allowed.includes(key));
  if (unknown.length === 0) {
    return;
  }
  throw new Error(`${label} has unknown keys: ${unknown.join(", ")}`);
}

export function vectorDimsForModel(model: string): number {
  const dims = EMBEDDING_DIMENSIONS[model];
  if (!dims) {
    throw new Error(`Unsupported embedding model: ${model}`);
  }
  return dims;
}

function resolveEnvVars(value: string): string {
  return value.replace(/\$\{([^}]+)\}/g, (_, envVar) => {
    const envValue = process.env[envVar];
    if (!envValue) {
      throw new Error(`Environment variable ${envVar} is not set`);
    }
    return envValue;
  });
}

function resolveEmbeddingModel(embedding: Record<string, unknown>): string {
  const model = typeof embedding.model === "string" ? embedding.model : DEFAULT_MODEL;
  vectorDimsForModel(model);
  return model;
}

export const memoryConfigSchema = {
  parse(value: unknown): MemoryConfig {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("memory config required");
    }
    const cfg = value as Record<string, unknown>;
    assertAllowedKeys(
      cfg,
      ["embedding", "dbPath", "autoCapture", "autoRecall", "retrieval", "chunking"],
      "memory config",
    );

    const embedding = cfg.embedding as Record<string, unknown> | undefined;
    if (!embedding || typeof embedding.apiKey !== "string") {
      throw new Error("embedding.apiKey is required");
    }
    assertAllowedKeys(embedding, ["apiKey", "model"], "embedding config");

    const model = resolveEmbeddingModel(embedding);

    return {
      embedding: {
        provider: "openai",
        model,
        apiKey: resolveEnvVars(embedding.apiKey),
      },
      dbPath: typeof cfg.dbPath === "string" ? cfg.dbPath : DEFAULT_DB_PATH,
      autoCapture: cfg.autoCapture !== false,
      autoRecall: cfg.autoRecall !== false,
      retrieval: {
        hybridEnabled:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.hybridEnabled ===
          "boolean"
            ? Boolean((cfg.retrieval as Record<string, unknown>).hybridEnabled)
            : true,
        maxResults:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.maxResults === "number"
            ? Number((cfg.retrieval as Record<string, unknown>).maxResults)
            : 8,
        maxTokensPerSnippet:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.maxTokensPerSnippet ===
          "number"
            ? Number((cfg.retrieval as Record<string, unknown>).maxTokensPerSnippet)
            : 220,
        vectorLimit:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.vectorLimit === "number"
            ? Number((cfg.retrieval as Record<string, unknown>).vectorLimit)
            : 40,
        ftsLimit:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.ftsLimit === "number"
            ? Number((cfg.retrieval as Record<string, unknown>).ftsLimit)
            : 40,
        rewriteCount:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.rewriteCount === "number"
            ? Number((cfg.retrieval as Record<string, unknown>).rewriteCount)
            : 5,
        rrfK:
          typeof (cfg.retrieval as Record<string, unknown> | undefined)?.rrfK === "number"
            ? Number((cfg.retrieval as Record<string, unknown>).rrfK)
            : 60,
      },
      chunking: {
        targetTokens:
          typeof (cfg.chunking as Record<string, unknown> | undefined)?.targetTokens === "number"
            ? Number((cfg.chunking as Record<string, unknown>).targetTokens)
            : 700,
        minTokens:
          typeof (cfg.chunking as Record<string, unknown> | undefined)?.minTokens === "number"
            ? Number((cfg.chunking as Record<string, unknown>).minTokens)
            : 500,
        maxTokens:
          typeof (cfg.chunking as Record<string, unknown> | undefined)?.maxTokens === "number"
            ? Number((cfg.chunking as Record<string, unknown>).maxTokens)
            : 900,
        overlapRatio:
          typeof (cfg.chunking as Record<string, unknown> | undefined)?.overlapRatio === "number"
            ? Number((cfg.chunking as Record<string, unknown>).overlapRatio)
            : 0.12,
      },
    };
  },
  uiHints: {
    "embedding.apiKey": {
      label: "OpenAI API Key",
      sensitive: true,
      placeholder: "sk-proj-...",
      help: "API key for OpenAI embeddings (or use ${OPENAI_API_KEY})",
    },
    "embedding.model": {
      label: "Embedding Model",
      placeholder: DEFAULT_MODEL,
      help: "OpenAI embedding model to use",
    },
    dbPath: {
      label: "Database Path",
      placeholder: "~/.openclaw/memory/lancedb",
      advanced: true,
    },
    autoCapture: {
      label: "Auto-Capture",
      help: "Automatically capture important information from conversations",
    },
    autoRecall: {
      label: "Auto-Recall",
      help: "Automatically inject relevant memories into context",
    },
    "retrieval.maxResults": {
      label: "Retrieval Result Limit",
      help: "Maximum snippets returned to the model (budget guardrail)",
      advanced: true,
    },
    "retrieval.hybridEnabled": {
      label: "Hybrid Search Enabled",
      help: "Enable BM25 + vector hybrid retrieval with RRF (set false for vector-only fallback)",
      advanced: true,
    },
    "retrieval.maxTokensPerSnippet": {
      label: "Max Snippet Tokens",
      help: "Truncate each snippet to this approximate token limit",
      advanced: true,
    },
    "retrieval.vectorLimit": {
      label: "Vector Candidate Limit",
      advanced: true,
    },
    "retrieval.ftsLimit": {
      label: "BM25 Candidate Limit",
      advanced: true,
    },
    "retrieval.rewriteCount": {
      label: "Query Rewrites",
      advanced: true,
    },
    "retrieval.rrfK": {
      label: "RRF K",
      advanced: true,
    },
    "chunking.targetTokens": {
      label: "Chunk Target Tokens",
      advanced: true,
    },
    "chunking.overlapRatio": {
      label: "Chunk Overlap Ratio",
      advanced: true,
    },
  },
};
