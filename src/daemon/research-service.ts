import type { GatewayService, GatewayServiceInstallArgs } from "./service.js";
import {
  GATEWAY_SERVICE_MARKER,
  RESEARCH_SCHEDULER_SERVICE_KIND,
  RESEARCH_SCHEDULER_WINDOWS_TASK_SCRIPT_NAME,
  RESEARCH_WORKER_SERVICE_KIND,
  RESEARCH_WORKER_WINDOWS_TASK_SCRIPT_NAME,
  resolveResearchSchedulerLaunchAgentLabel,
  resolveResearchSchedulerSystemdServiceName,
  resolveResearchSchedulerWindowsTaskName,
  resolveResearchWorkerLaunchAgentLabel,
  resolveResearchWorkerSystemdServiceName,
  resolveResearchWorkerWindowsTaskName,
} from "./constants.js";
import { resolveGatewayService } from "./service.js";

export type ResearchServiceKind = "worker" | "scheduler";

function withResearchServiceEnv(
  env: Record<string, string | undefined>,
  kind: ResearchServiceKind,
): Record<string, string | undefined> {
  const isWorker = kind === "worker";
  return {
    ...env,
    OPENCLAW_LAUNCHD_LABEL: isWorker
      ? resolveResearchWorkerLaunchAgentLabel()
      : resolveResearchSchedulerLaunchAgentLabel(),
    OPENCLAW_SYSTEMD_UNIT: isWorker
      ? resolveResearchWorkerSystemdServiceName()
      : resolveResearchSchedulerSystemdServiceName(),
    OPENCLAW_WINDOWS_TASK_NAME: isWorker
      ? resolveResearchWorkerWindowsTaskName()
      : resolveResearchSchedulerWindowsTaskName(),
    OPENCLAW_TASK_SCRIPT_NAME: isWorker
      ? RESEARCH_WORKER_WINDOWS_TASK_SCRIPT_NAME
      : RESEARCH_SCHEDULER_WINDOWS_TASK_SCRIPT_NAME,
    OPENCLAW_LOG_PREFIX: isWorker ? "research-worker" : "research-scheduler",
    OPENCLAW_SERVICE_MARKER: GATEWAY_SERVICE_MARKER,
    OPENCLAW_SERVICE_KIND: isWorker
      ? RESEARCH_WORKER_SERVICE_KIND
      : RESEARCH_SCHEDULER_SERVICE_KIND,
  };
}

function withResearchInstallEnv(
  args: GatewayServiceInstallArgs,
  kind: ResearchServiceKind,
): GatewayServiceInstallArgs {
  const isWorker = kind === "worker";
  return {
    ...args,
    env: withResearchServiceEnv(args.env, kind),
    environment: {
      ...args.environment,
      OPENCLAW_LAUNCHD_LABEL: isWorker
        ? resolveResearchWorkerLaunchAgentLabel()
        : resolveResearchSchedulerLaunchAgentLabel(),
      OPENCLAW_SYSTEMD_UNIT: isWorker
        ? resolveResearchWorkerSystemdServiceName()
        : resolveResearchSchedulerSystemdServiceName(),
      OPENCLAW_WINDOWS_TASK_NAME: isWorker
        ? resolveResearchWorkerWindowsTaskName()
        : resolveResearchSchedulerWindowsTaskName(),
      OPENCLAW_TASK_SCRIPT_NAME: isWorker
        ? RESEARCH_WORKER_WINDOWS_TASK_SCRIPT_NAME
        : RESEARCH_SCHEDULER_WINDOWS_TASK_SCRIPT_NAME,
      OPENCLAW_LOG_PREFIX: isWorker ? "research-worker" : "research-scheduler",
      OPENCLAW_SERVICE_MARKER: GATEWAY_SERVICE_MARKER,
      OPENCLAW_SERVICE_KIND: isWorker
        ? RESEARCH_WORKER_SERVICE_KIND
        : RESEARCH_SCHEDULER_SERVICE_KIND,
    },
  };
}

export function resolveResearchService(kind: ResearchServiceKind): GatewayService {
  const base = resolveGatewayService();
  return {
    ...base,
    install: async (args) => {
      return base.install(withResearchInstallEnv(args, kind));
    },
    uninstall: async (args) => {
      return base.uninstall({ ...args, env: withResearchServiceEnv(args.env, kind) });
    },
    stop: async (args) => {
      return base.stop({ ...args, env: withResearchServiceEnv(args.env ?? {}, kind) });
    },
    restart: async (args) => {
      return base.restart({ ...args, env: withResearchServiceEnv(args.env ?? {}, kind) });
    },
    isLoaded: async (args) => {
      return base.isLoaded({ env: withResearchServiceEnv(args.env ?? {}, kind) });
    },
    readCommand: (env) => base.readCommand(withResearchServiceEnv(env, kind)),
    readRuntime: (env) => base.readRuntime(withResearchServiceEnv(env, kind)),
  };
}
