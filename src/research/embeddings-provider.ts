const OPENAI_EMBEDDINGS_URL = "https://api.openai.com/v1/embeddings";
const DEFAULT_OPENAI_MODEL = "text-embedding-3-large";
const DEFAULT_HASH_DIMS = 256;
const DEFAULT_OPENAI_BATCH_SIZE = 64;
const DEFAULT_OPENAI_MAX_INPUT_TOKENS = 1800;

export type ResearchEmbeddingProvider = {
  id: "openai" | "hash";
  model: string;
  dims: number;
  embedQuery: (text: string) => Promise<number[]>;
  embedBatch: (texts: string[]) => Promise<number[][]>;
};

export type EmbeddingProviderSelection = {
  provider: ResearchEmbeddingProvider;
  warning?: string;
};

const l2Normalize = (vec: number[]): number[] => {
  const mag = Math.sqrt(vec.reduce((sum, value) => sum + value * value, 0));
  if (!Number.isFinite(mag) || mag <= 1e-9) return vec.map(() => 0);
  return vec.map((value) => value / mag);
};

const sanitizeVector = (vec: number[]): number[] =>
  l2Normalize(vec.map((value) => (Number.isFinite(value) ? value : 0)));

const hashEmbed = (text: string, dims: number): number[] => {
  const out = Array.from({ length: dims }, () => 0);
  const tokens = text
    .toLowerCase()
    .split(/[^a-z0-9_]+/)
    .filter(Boolean);
  for (const token of tokens) {
    let h = 2166136261;
    for (let i = 0; i < token.length; i += 1) {
      h ^= token.charCodeAt(i);
      h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
    }
    const idx = Math.abs(h) % dims;
    out[idx] += 1;
  }
  return sanitizeVector(out);
};

export const createHashEmbeddingProvider = (
  dims = DEFAULT_HASH_DIMS,
): ResearchEmbeddingProvider => ({
  id: "hash",
  model: `hash-v1-${dims}`,
  dims,
  embedQuery: async (text: string) => hashEmbed(text, dims),
  embedBatch: async (texts: string[]) => texts.map((text) => hashEmbed(text, dims)),
});

const chunk = <T>(items: T[], size: number): T[][] => {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
};

const tokenizeApprox = (text: string): string[] =>
  text
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);

const splitTextForEmbedding = (text: string, maxTokens: number): string[] => {
  const tokens = tokenizeApprox(text);
  if (tokens.length <= maxTokens) return [text];
  const segments: string[] = [];
  for (let i = 0; i < tokens.length; i += maxTokens) {
    const part = tokens
      .slice(i, i + maxTokens)
      .join(" ")
      .trim();
    if (part) segments.push(part);
  }
  return segments.length > 0 ? segments : [text];
};

const parseOpenAiBatchSize = (): number => {
  const raw = process.env.RESEARCH_EMBED_BATCH_SIZE;
  const value = raw ? Number.parseInt(raw, 10) : DEFAULT_OPENAI_BATCH_SIZE;
  if (!Number.isFinite(value) || value <= 0) return DEFAULT_OPENAI_BATCH_SIZE;
  return Math.max(1, Math.min(2048, value));
};

const parseOpenAiMaxInputTokens = (): number => {
  const raw = process.env.RESEARCH_EMBED_MAX_INPUT_TOKENS;
  const value = raw ? Number.parseInt(raw, 10) : DEFAULT_OPENAI_MAX_INPUT_TOKENS;
  if (!Number.isFinite(value) || value <= 256) return DEFAULT_OPENAI_MAX_INPUT_TOKENS;
  return Math.max(256, Math.min(6000, value));
};

const averageVectors = (vectors: number[][], weights: number[]): number[] => {
  if (!vectors.length) return [];
  const dims = vectors[0]?.length ?? 0;
  if (dims <= 0) return [];
  const out = Array.from({ length: dims }, () => 0);
  let totalWeight = 0;
  for (let i = 0; i < vectors.length; i += 1) {
    const vec = vectors[i] ?? [];
    const weight = Math.max(1, Math.round(weights[i] ?? 1));
    if (vec.length !== dims) continue;
    totalWeight += weight;
    for (let d = 0; d < dims; d += 1) {
      out[d] = (out[d] ?? 0) + (vec[d] ?? 0) * weight;
    }
  }
  if (totalWeight <= 0) return sanitizeVector(out);
  for (let d = 0; d < dims; d += 1) {
    out[d] = (out[d] ?? 0) / totalWeight;
  }
  return sanitizeVector(out);
};

const embedOpenAi = async (params: {
  apiKey: string;
  model: string;
  input: string[];
}): Promise<number[][]> => {
  if (params.input.length === 0) return [];
  const res = await fetch(OPENAI_EMBEDDINGS_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${params.apiKey}`,
    },
    body: JSON.stringify({
      model: params.model,
      input: params.input,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`OpenAI embeddings HTTP ${res.status}: ${body}`);
  }
  const payload = (await res.json()) as {
    data?: Array<{ embedding?: number[] }>;
  };
  const vectors = (payload.data ?? []).map((entry) => sanitizeVector(entry.embedding ?? []));
  if (vectors.length !== params.input.length) {
    throw new Error(
      `OpenAI embeddings size mismatch: expected=${params.input.length} got=${vectors.length}`,
    );
  }
  return vectors;
};

const createOpenAiProvider = async (
  apiKey: string,
  model: string,
): Promise<ResearchEmbeddingProvider> => {
  const probe = await embedOpenAi({
    apiKey,
    model,
    input: ["financial embedding dimension probe"],
  });
  const dims = probe[0]?.length ?? 0;
  if (!Number.isFinite(dims) || dims <= 0) {
    throw new Error("OpenAI embeddings returned zero dimensions");
  }
  const batchSize = parseOpenAiBatchSize();
  const maxInputTokens = parseOpenAiMaxInputTokens();
  return {
    id: "openai",
    model,
    dims,
    embedQuery: async (text: string) => {
      const segments = splitTextForEmbedding(text, maxInputTokens);
      const vectors = await embedOpenAi({
        apiKey,
        model,
        input: segments,
      });
      if (vectors.length <= 1) return vectors[0] ?? Array.from({ length: dims }, () => 0);
      const weights = segments.map((segment) => tokenizeApprox(segment).length);
      return averageVectors(vectors, weights);
    },
    embedBatch: async (texts: string[]) => {
      if (texts.length === 0) return [];
      const expandedInputs: string[] = [];
      const spans: Array<{ start: number; length: number; weights: number[] }> = [];
      for (const text of texts) {
        const segments = splitTextForEmbedding(text, maxInputTokens);
        spans.push({
          start: expandedInputs.length,
          length: segments.length,
          weights: segments.map((segment) => tokenizeApprox(segment).length),
        });
        expandedInputs.push(...segments);
      }
      const batches = chunk(expandedInputs, batchSize);
      const expandedVectors: number[][] = [];
      for (const batch of batches) {
        const out = await embedOpenAi({
          apiKey,
          model,
          input: batch,
        });
        expandedVectors.push(...out);
      }
      const vectors: number[][] = [];
      for (const span of spans) {
        if (span.length <= 1) {
          vectors.push(expandedVectors[span.start] ?? Array.from({ length: dims }, () => 0));
          continue;
        }
        const segmentVectors = expandedVectors.slice(span.start, span.start + span.length);
        vectors.push(averageVectors(segmentVectors, span.weights));
      }
      return vectors;
    },
  };
};

export const createResearchEmbeddingProvider = async (): Promise<EmbeddingProviderSelection> => {
  const requested = (process.env.RESEARCH_EMBED_PROVIDER ?? "auto").trim().toLowerCase();
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  const model = (process.env.RESEARCH_EMBED_MODEL ?? DEFAULT_OPENAI_MODEL).trim();

  if (requested === "hash") {
    return { provider: createHashEmbeddingProvider() };
  }

  if (requested === "openai") {
    if (!apiKey) {
      return {
        provider: createHashEmbeddingProvider(),
        warning:
          "RESEARCH_EMBED_PROVIDER=openai but OPENAI_API_KEY is missing; using hash fallback",
      };
    }
    const provider = await createOpenAiProvider(apiKey, model);
    return { provider };
  }

  if (apiKey) {
    try {
      const provider = await createOpenAiProvider(apiKey, model);
      return { provider };
    } catch (err) {
      return {
        provider: createHashEmbeddingProvider(),
        warning: `OpenAI embeddings unavailable (${String(err)}); using hash fallback`,
      };
    }
  }

  return {
    provider: createHashEmbeddingProvider(),
    warning: "OPENAI_API_KEY not set; using hash embeddings",
  };
};

export const __testOnly = {
  splitTextForEmbedding,
  averageVectors,
};
