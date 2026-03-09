import { execFileSync } from "node:child_process";
import { openResearchDb } from "./db.js";

export type ResearchCapability =
  | "newsletter_cookie_access"
  | "newsletter_authenticated_fetch"
  | "newsletter_archive_fetch";

export type ResearchWorkflow = "newsletter_sync";

export type ResearchApprovalDecision = "allow-once" | "allow-always" | "deny";

export type ResearchApprovalRequest = {
  id: number;
  workflow: ResearchWorkflow;
  capabilityKey: ResearchCapability;
  subject: string;
  status: "pending" | "approved" | "denied";
  requestedBy: string;
  resolvedBy?: string;
  details: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type ResearchToolRun = {
  id: number;
  workflow: string;
  toolName: string;
  capabilityKey: string;
  status: "ok" | "error";
  subject: string;
  latencyMs: number;
  requestMetadata: Record<string, unknown>;
  responseMetadata: Record<string, unknown>;
  errorText?: string;
  createdAt: number;
};

type SecretResolver = (ref: string) => string;

const CAPABILITY_POLICIES: Record<
  ResearchWorkflow,
  { allowed: ResearchCapability[]; approvalRequired: ResearchCapability[] }
> = {
  newsletter_sync: {
    allowed: [
      "newsletter_archive_fetch",
      "newsletter_authenticated_fetch",
      "newsletter_cookie_access",
    ],
    approvalRequired: ["newsletter_authenticated_fetch", "newsletter_cookie_access"],
  },
};

const parseJsonObject = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "string" || !value.trim()) return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
};

const resolveWithOnePasswordCli = (ref: string): string =>
  execFileSync("op", ["read", ref], {
    encoding: "utf8",
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();

const toApprovalRef = (id: number): string => `research-approval:${id}`;

const parseApprovalRef = (value: string): number | null => {
  const trimmed = value.trim();
  const match = trimmed.match(/^research-approval:(\d+)$/i);
  if (!match?.[1]) return null;
  const id = Number.parseInt(match[1], 10);
  return Number.isFinite(id) && id > 0 ? id : null;
};

export const recordResearchToolRun = (params: {
  workflow: string;
  toolName: string;
  capabilityKey: string;
  status: "ok" | "error";
  subject: string;
  latencyMs: number;
  requestMetadata?: Record<string, unknown>;
  responseMetadata?: Record<string, unknown>;
  errorText?: string;
  dbPath?: string;
}): ResearchToolRun => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const row = db
    .prepare(
      `INSERT INTO research_tool_runs (
         workflow, tool_name, capability_key, status, subject, latency_ms,
         request_metadata, response_metadata, error_text, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.workflow,
      params.toolName,
      params.capabilityKey,
      params.status,
      params.subject,
      Math.max(0, Math.round(params.latencyMs)),
      JSON.stringify(params.requestMetadata ?? {}),
      JSON.stringify(params.responseMetadata ?? {}),
      (params.errorText ?? "").trim(),
      now,
    ) as { id: number };
  return {
    id: row.id,
    workflow: params.workflow,
    toolName: params.toolName,
    capabilityKey: params.capabilityKey,
    status: params.status,
    subject: params.subject,
    latencyMs: Math.max(0, Math.round(params.latencyMs)),
    requestMetadata: params.requestMetadata ?? {},
    responseMetadata: params.responseMetadata ?? {},
    errorText: params.errorText?.trim() || undefined,
    createdAt: now,
  };
};

export const ensureResearchCapabilityAllowed = (params: {
  workflow: ResearchWorkflow;
  capability: ResearchCapability;
}): void => {
  const policy = CAPABILITY_POLICIES[params.workflow];
  if (!policy || !policy.allowed.includes(params.capability)) {
    throw new Error(
      `capability ${params.capability} is not allowed for workflow ${params.workflow}`,
    );
  }
};

export const createResearchApprovalRequest = (params: {
  workflow: ResearchWorkflow;
  capability: ResearchCapability;
  subject: string;
  requestedBy?: string;
  details?: Record<string, unknown>;
  dbPath?: string;
}): ResearchApprovalRequest => {
  ensureResearchCapabilityAllowed({
    workflow: params.workflow,
    capability: params.capability,
  });
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const existing = db
    .prepare(
      `SELECT id, workflow, capability_key, subject, status, requested_by, resolved_by, details, created_at, updated_at
       FROM research_approval_requests
       WHERE workflow=? AND capability_key=? AND subject=? AND status='pending'
       ORDER BY id DESC
       LIMIT 1`,
    )
    .get(params.workflow, params.capability, params.subject) as
    | {
        id: number;
        workflow: ResearchWorkflow;
        capability_key: ResearchCapability;
        subject: string;
        status: "pending" | "approved" | "denied";
        requested_by: string;
        resolved_by?: string;
        details?: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (existing) {
    return {
      id: existing.id,
      workflow: existing.workflow,
      capabilityKey: existing.capability_key,
      subject: existing.subject,
      status: existing.status,
      requestedBy: existing.requested_by,
      resolvedBy: existing.resolved_by || undefined,
      details: parseJsonObject(existing.details),
      createdAt: existing.created_at,
      updatedAt: existing.updated_at,
    };
  }
  const row = db
    .prepare(
      `INSERT INTO research_approval_requests (
         workflow, capability_key, subject, status, requested_by, resolved_by, details, created_at, updated_at
       ) VALUES (?, ?, ?, 'pending', ?, '', ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.workflow,
      params.capability,
      params.subject,
      (params.requestedBy ?? "").trim(),
      JSON.stringify(params.details ?? {}),
      now,
      now,
    ) as { id: number };
  return {
    id: row.id,
    workflow: params.workflow,
    capabilityKey: params.capability,
    subject: params.subject,
    status: "pending",
    requestedBy: (params.requestedBy ?? "").trim(),
    details: params.details ?? {},
    createdAt: now,
    updatedAt: now,
  };
};

export const approveResearchApprovalRequest = (params: {
  approvalRef: string;
  decision: ResearchApprovalDecision;
  resolvedBy: string;
  dbPath?: string;
}): ResearchApprovalRequest => {
  const id = parseApprovalRef(params.approvalRef);
  if (!id) throw new Error(`invalid research approval ref: ${params.approvalRef}`);
  const db = openResearchDb(params.dbPath);
  const existing = db
    .prepare(
      `SELECT id, workflow, capability_key, subject, status, requested_by, resolved_by, details, created_at, updated_at
       FROM research_approval_requests
       WHERE id=?`,
    )
    .get(id) as
    | {
        id: number;
        workflow: ResearchWorkflow;
        capability_key: ResearchCapability;
        subject: string;
        status: "pending" | "approved" | "denied";
        requested_by: string;
        resolved_by?: string;
        details?: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (!existing) throw new Error(`research approval not found: ${params.approvalRef}`);
  const status = params.decision === "deny" ? "denied" : "approved";
  const now = Date.now();
  db.prepare(
    `UPDATE research_approval_requests
     SET status=?, resolved_by=?, updated_at=?
     WHERE id=?`,
  ).run(status, params.resolvedBy.trim(), now, id);
  return {
    id: existing.id,
    workflow: existing.workflow,
    capabilityKey: existing.capability_key,
    subject: existing.subject,
    status,
    requestedBy: existing.requested_by,
    resolvedBy: params.resolvedBy.trim(),
    details: parseJsonObject(existing.details),
    createdAt: existing.created_at,
    updatedAt: now,
  };
};

export const resolveResearchApproval = (params: {
  approvalRef: string;
  workflow: ResearchWorkflow;
  capability: ResearchCapability;
  subject: string;
  dbPath?: string;
}): ResearchApprovalRequest | null => {
  const id = parseApprovalRef(params.approvalRef);
  if (!id) return null;
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `SELECT id, workflow, capability_key, subject, status, requested_by, resolved_by, details, created_at, updated_at
       FROM research_approval_requests
       WHERE id=?`,
    )
    .get(id) as
    | {
        id: number;
        workflow: ResearchWorkflow;
        capability_key: ResearchCapability;
        subject: string;
        status: "pending" | "approved" | "denied";
        requested_by: string;
        resolved_by?: string;
        details?: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (!row) return null;
  if (
    row.workflow !== params.workflow ||
    row.capability_key !== params.capability ||
    row.subject !== params.subject
  ) {
    return null;
  }
  return {
    id: row.id,
    workflow: row.workflow,
    capabilityKey: row.capability_key,
    subject: row.subject,
    status: row.status,
    requestedBy: row.requested_by,
    resolvedBy: row.resolved_by || undefined,
    details: parseJsonObject(row.details),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const resolveGovernedSecret = (params: {
  refOrValue: string;
  workflow: ResearchWorkflow;
  capability: ResearchCapability;
  subject: string;
  dbPath?: string;
  secretResolver?: SecretResolver;
}): string => {
  ensureResearchCapabilityAllowed({
    workflow: params.workflow,
    capability: params.capability,
  });
  const startedAt = Date.now();
  try {
    const value = params.refOrValue.trim();
    if (!value) {
      recordResearchToolRun({
        workflow: params.workflow,
        toolName: "secret.resolve",
        capabilityKey: params.capability,
        status: "ok",
        subject: params.subject,
        latencyMs: Date.now() - startedAt,
        requestMetadata: { secretRef: false },
        responseMetadata: { resolved: false },
        dbPath: params.dbPath,
      });
      return "";
    }
    let resolved = value;
    if (value.startsWith("op://")) {
      const resolver = params.secretResolver ?? resolveWithOnePasswordCli;
      resolved = resolver(value);
    }
    recordResearchToolRun({
      workflow: params.workflow,
      toolName: "secret.resolve",
      capabilityKey: params.capability,
      status: "ok",
      subject: params.subject,
      latencyMs: Date.now() - startedAt,
      requestMetadata: { secretRef: value.startsWith("op://") },
      responseMetadata: { resolved: Boolean(resolved), provider: value.startsWith("op://") ? "1password" : "inline" },
      dbPath: params.dbPath,
    });
    return resolved.trim();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    recordResearchToolRun({
      workflow: params.workflow,
      toolName: "secret.resolve",
      capabilityKey: params.capability,
      status: "error",
      subject: params.subject,
      latencyMs: Date.now() - startedAt,
      requestMetadata: { secretRef: params.refOrValue.trim().startsWith("op://") },
      errorText: message,
      dbPath: params.dbPath,
    });
    throw err;
  }
};

export const requireApprovedResearchCapability = (params: {
  workflow: ResearchWorkflow;
  capability: ResearchCapability;
  subject: string;
  approvalRef?: string;
  requestedBy?: string;
  details?: Record<string, unknown>;
  dbPath?: string;
}): { approved: true } | { approved: false; approvalRef: string } => {
  ensureResearchCapabilityAllowed({
    workflow: params.workflow,
    capability: params.capability,
  });
  const policy = CAPABILITY_POLICIES[params.workflow];
  if (!policy.approvalRequired.includes(params.capability)) {
    return { approved: true };
  }
  if (params.approvalRef) {
    const approval = resolveResearchApproval({
      approvalRef: params.approvalRef,
      workflow: params.workflow,
      capability: params.capability,
      subject: params.subject,
      dbPath: params.dbPath,
    });
    if (approval?.status === "approved") {
      return { approved: true };
    }
  }
  const request = createResearchApprovalRequest({
    workflow: params.workflow,
    capability: params.capability,
    subject: params.subject,
    requestedBy: params.requestedBy,
    details: params.details,
    dbPath: params.dbPath,
  });
  return { approved: false, approvalRef: toApprovalRef(request.id) };
};
