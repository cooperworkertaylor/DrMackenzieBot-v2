// Default service labels (canonical + legacy compatibility)
export const GATEWAY_LAUNCH_AGENT_LABEL = "ai.openclaw.gateway";
export const GATEWAY_SYSTEMD_SERVICE_NAME = "openclaw-gateway";
export const GATEWAY_WINDOWS_TASK_NAME = "OpenClaw Gateway";
export const GATEWAY_SERVICE_MARKER = "openclaw";
export const GATEWAY_SERVICE_KIND = "gateway";
export const NODE_LAUNCH_AGENT_LABEL = "ai.openclaw.node";
export const NODE_SYSTEMD_SERVICE_NAME = "openclaw-node";
export const NODE_WINDOWS_TASK_NAME = "OpenClaw Node";
export const NODE_SERVICE_MARKER = "openclaw";
export const NODE_SERVICE_KIND = "node";
export const NODE_WINDOWS_TASK_SCRIPT_NAME = "node.cmd";
export const RESEARCH_WORKER_LAUNCH_AGENT_LABEL = "ai.openclaw.research-worker";
export const RESEARCH_WORKER_SYSTEMD_SERVICE_NAME = "openclaw-research-worker";
export const RESEARCH_WORKER_WINDOWS_TASK_NAME = "OpenClaw Research Worker";
export const RESEARCH_WORKER_SERVICE_KIND = "research-worker";
export const RESEARCH_WORKER_WINDOWS_TASK_SCRIPT_NAME = "research-worker.cmd";
export const RESEARCH_SCHEDULER_LAUNCH_AGENT_LABEL = "ai.openclaw.research-scheduler";
export const RESEARCH_SCHEDULER_SYSTEMD_SERVICE_NAME = "openclaw-research-scheduler";
export const RESEARCH_SCHEDULER_WINDOWS_TASK_NAME = "OpenClaw Research Scheduler";
export const RESEARCH_SCHEDULER_SERVICE_KIND = "research-scheduler";
export const RESEARCH_SCHEDULER_WINDOWS_TASK_SCRIPT_NAME = "research-scheduler.cmd";
export const LEGACY_GATEWAY_LAUNCH_AGENT_LABELS: string[] = [];
export const LEGACY_GATEWAY_SYSTEMD_SERVICE_NAMES: string[] = [];
export const LEGACY_GATEWAY_WINDOWS_TASK_NAMES: string[] = [];

export function normalizeGatewayProfile(profile?: string): string | null {
  const trimmed = profile?.trim();
  if (!trimmed || trimmed.toLowerCase() === "default") {
    return null;
  }
  return trimmed;
}

export function resolveGatewayProfileSuffix(profile?: string): string {
  const normalized = normalizeGatewayProfile(profile);
  return normalized ? `-${normalized}` : "";
}

export function resolveGatewayLaunchAgentLabel(profile?: string): string {
  const normalized = normalizeGatewayProfile(profile);
  if (!normalized) {
    return GATEWAY_LAUNCH_AGENT_LABEL;
  }
  return `ai.openclaw.${normalized}`;
}

export function resolveLegacyGatewayLaunchAgentLabels(profile?: string): string[] {
  void profile;
  return [];
}

export function resolveGatewaySystemdServiceName(profile?: string): string {
  const suffix = resolveGatewayProfileSuffix(profile);
  if (!suffix) {
    return GATEWAY_SYSTEMD_SERVICE_NAME;
  }
  return `openclaw-gateway${suffix}`;
}

export function resolveGatewayWindowsTaskName(profile?: string): string {
  const normalized = normalizeGatewayProfile(profile);
  if (!normalized) {
    return GATEWAY_WINDOWS_TASK_NAME;
  }
  return `OpenClaw Gateway (${normalized})`;
}

export function formatGatewayServiceDescription(params?: {
  profile?: string;
  version?: string;
}): string {
  const profile = normalizeGatewayProfile(params?.profile);
  const version = params?.version?.trim();
  const parts: string[] = [];
  if (profile) {
    parts.push(`profile: ${profile}`);
  }
  if (version) {
    parts.push(`v${version}`);
  }
  if (parts.length === 0) {
    return "OpenClaw Gateway";
  }
  return `OpenClaw Gateway (${parts.join(", ")})`;
}

export function resolveNodeLaunchAgentLabel(): string {
  return NODE_LAUNCH_AGENT_LABEL;
}

export function resolveNodeSystemdServiceName(): string {
  return NODE_SYSTEMD_SERVICE_NAME;
}

export function resolveNodeWindowsTaskName(): string {
  return NODE_WINDOWS_TASK_NAME;
}

export function formatNodeServiceDescription(params?: { version?: string }): string {
  const version = params?.version?.trim();
  if (!version) {
    return "OpenClaw Node Host";
  }
  return `OpenClaw Node Host (v${version})`;
}

export function resolveResearchWorkerLaunchAgentLabel(): string {
  return RESEARCH_WORKER_LAUNCH_AGENT_LABEL;
}

export function resolveResearchWorkerSystemdServiceName(): string {
  return RESEARCH_WORKER_SYSTEMD_SERVICE_NAME;
}

export function resolveResearchWorkerWindowsTaskName(): string {
  return RESEARCH_WORKER_WINDOWS_TASK_NAME;
}

export function resolveResearchSchedulerLaunchAgentLabel(): string {
  return RESEARCH_SCHEDULER_LAUNCH_AGENT_LABEL;
}

export function resolveResearchSchedulerSystemdServiceName(): string {
  return RESEARCH_SCHEDULER_SYSTEMD_SERVICE_NAME;
}

export function resolveResearchSchedulerWindowsTaskName(): string {
  return RESEARCH_SCHEDULER_WINDOWS_TASK_NAME;
}

export function formatResearchServiceDescription(params: {
  kind: "worker" | "scheduler";
  version?: string;
}): string {
  const name =
    params.kind === "worker" ? "OpenClaw Research Worker" : "OpenClaw Research Scheduler";
  const version = params.version?.trim();
  if (!version) {
    return name;
  }
  return `${name} (v${version})`;
}
