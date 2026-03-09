import type { GatewayDaemonRuntime } from "../commands/daemon-runtime.js";
import { buildResearchInstallPlan } from "../commands/research-daemon-install-helpers.js";
import { resolveIsNixMode } from "../config/paths.js";
import {
  resolveResearchSchedulerLaunchAgentLabel,
  resolveResearchSchedulerSystemdServiceName,
  resolveResearchSchedulerWindowsTaskName,
  resolveResearchWorkerLaunchAgentLabel,
  resolveResearchWorkerSystemdServiceName,
  resolveResearchWorkerWindowsTaskName,
} from "../daemon/constants.js";
import { resolveGatewayLogPaths } from "../daemon/launchd.js";
import { resolveResearchService, type ResearchServiceKind } from "../daemon/research-service.js";
import { renderSystemdUnavailableHints } from "../daemon/systemd-hints.js";
import { isSystemdUserServiceAvailable } from "../daemon/systemd.js";
import { isWSL } from "../infra/wsl.js";
import { defaultRuntime } from "../runtime.js";
import { colorize, isRich, theme } from "../terminal/theme.js";
import { formatCliCommand } from "./command-format.js";
import { buildDaemonServiceSnapshot, createNullWriter, emitDaemonActionJson } from "./daemon-cli/response.js";
import { formatRuntimeStatus } from "./daemon-cli/shared.js";

type ResearchServiceInstallOptions = {
  kind: ResearchServiceKind;
  db?: string;
  intervalMs?: string | number;
  runtime?: string;
  force?: boolean;
  json?: boolean;
};

type ResearchServiceLifecycleOptions = {
  kind: ResearchServiceKind;
  json?: boolean;
};

type ResearchServiceStatusOptions = {
  kind: ResearchServiceKind;
  json?: boolean;
};

const isResearchServiceKind = (value: string | undefined): value is ResearchServiceKind =>
  value === "worker" || value === "scheduler";

const resolveIntervalMs = (value: unknown): number => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(10_000, Math.floor(value));
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return Math.max(10_000, parsed);
    }
  }
  return 300_000;
};

function renderResearchServiceStartHints(kind: ResearchServiceKind): string[] {
  const commandBase =
    kind === "worker"
      ? formatCliCommand("openclaw research service install --kind worker")
      : formatCliCommand("openclaw research service install --kind scheduler");
  const runBase =
    kind === "worker"
      ? formatCliCommand("openclaw research worker")
      : formatCliCommand("openclaw research scheduler");
  switch (process.platform) {
    case "darwin":
      return [
        commandBase,
        runBase,
        `launchctl bootstrap gui/$UID ~/Library/LaunchAgents/${kind === "worker" ? resolveResearchWorkerLaunchAgentLabel() : resolveResearchSchedulerLaunchAgentLabel()}.plist`,
      ];
    case "linux":
      return [
        commandBase,
        runBase,
        `systemctl --user start ${kind === "worker" ? resolveResearchWorkerSystemdServiceName() : resolveResearchSchedulerSystemdServiceName()}.service`,
      ];
    case "win32":
      return [
        commandBase,
        runBase,
        `schtasks /Run /TN "${kind === "worker" ? resolveResearchWorkerWindowsTaskName() : resolveResearchSchedulerWindowsTaskName()}"`,
      ];
    default:
      return [commandBase, runBase];
  }
}

function buildResearchRuntimeHints(
  kind: ResearchServiceKind,
  env: NodeJS.ProcessEnv = process.env,
): string[] {
  if (process.platform === "darwin") {
    const logs = resolveGatewayLogPaths({
      ...env,
      OPENCLAW_LOG_PREFIX: kind === "worker" ? "research-worker" : "research-scheduler",
    });
    return [
      `Launchd stdout (if installed): ${logs.stdoutPath}`,
      `Launchd stderr (if installed): ${logs.stderrPath}`,
    ];
  }
  if (process.platform === "linux") {
    return [
      `Logs: journalctl --user -u ${kind === "worker" ? "openclaw-research-worker" : "openclaw-research-scheduler"}.service -n 200 --no-pager`,
    ];
  }
  if (process.platform === "win32") {
    return [
      `Logs: schtasks /Query /TN "${kind === "worker" ? "OpenClaw Research Worker" : "OpenClaw Research Scheduler"}" /V /FO LIST`,
    ];
  }
  return [];
}

export async function runResearchServiceInstall(opts: ResearchServiceInstallOptions) {
  const json = Boolean(opts.json);
  const warnings: string[] = [];
  const stdout = json ? createNullWriter() : process.stdout;
  const emit = (payload: {
    ok: boolean;
    result?: string;
    message?: string;
    error?: string;
    service?: {
      label: string;
      loaded: boolean;
      loadedText: string;
      notLoadedText: string;
    };
    hints?: string[];
    warnings?: string[];
  }) => {
    if (!json) return;
    emitDaemonActionJson({ action: "install", ...payload });
  };
  const fail = (message: string, hints?: string[]) => {
    if (json) {
      emit({ ok: false, error: message, hints, warnings: warnings.length ? warnings : undefined });
    } else {
      defaultRuntime.error(message);
    }
    defaultRuntime.exit(1);
  };

  if (!isResearchServiceKind(opts.kind)) {
    fail('Invalid --kind (use "worker" or "scheduler")');
    return;
  }
  if (resolveIsNixMode(process.env)) {
    fail("Nix mode detected; service install is disabled.");
    return;
  }
  const runtimeRaw = (opts.runtime ? String(opts.runtime) : "node") as GatewayDaemonRuntime;
  if (runtimeRaw !== "node" && runtimeRaw !== "bun") {
    fail('Invalid --runtime (use "node" or "bun")');
    return;
  }

  const service = resolveResearchService(opts.kind);
  let loaded = false;
  try {
    loaded = await service.isLoaded({ env: process.env });
  } catch (err) {
    fail(`Research service check failed: ${String(err)}`);
    return;
  }
  if (loaded && !opts.force) {
    emit({
      ok: true,
      result: "already-installed",
      message: `Research ${opts.kind} service already ${service.loadedText}.`,
      service: buildDaemonServiceSnapshot(service, loaded),
      warnings: warnings.length ? warnings : undefined,
    });
    if (!json) {
      defaultRuntime.log(`Research ${opts.kind} service already ${service.loadedText}.`);
    }
    return;
  }

  const { programArguments, workingDirectory, environment, description } =
    await buildResearchInstallPlan({
      env: process.env as Record<string, string | undefined>,
      kind: opts.kind,
      dbPath: opts.db,
      intervalMs: resolveIntervalMs(opts.intervalMs),
      runtime: runtimeRaw,
      warn: (message) => {
        if (json) warnings.push(message);
        else defaultRuntime.log(message);
      },
    });

  try {
    await service.install({
      env: process.env as Record<string, string | undefined>,
      stdout,
      programArguments,
      workingDirectory,
      environment,
      description,
    });
  } catch (err) {
    fail(`Research ${opts.kind} install failed: ${String(err)}`);
    return;
  }

  let installed = true;
  try {
    installed = await service.isLoaded({ env: process.env });
  } catch {
    installed = true;
  }
  emit({
    ok: true,
    result: "installed",
    service: buildDaemonServiceSnapshot(service, installed),
    warnings: warnings.length ? warnings : undefined,
  });
}

async function runResearchServiceLifecycle(
  action: "uninstall" | "start" | "stop" | "restart",
  opts: ResearchServiceLifecycleOptions,
) {
  const json = Boolean(opts.json);
  const stdout = json ? createNullWriter() : process.stdout;
  const service = resolveResearchService(opts.kind);
  const emit = (payload: {
    ok: boolean;
    result?: string;
    message?: string;
    error?: string;
    hints?: string[];
    service?: {
      label: string;
      loaded: boolean;
      loadedText: string;
      notLoadedText: string;
    };
  }) => {
    if (!json) return;
    emitDaemonActionJson({ action, ...payload });
  };
  const fail = (message: string, hints?: string[]) => {
    if (json) emit({ ok: false, error: message, hints });
    else defaultRuntime.error(message);
    defaultRuntime.exit(1);
  };

  let loaded = false;
  try {
    loaded = await service.isLoaded({ env: process.env });
  } catch (err) {
    fail(`Research ${opts.kind} service check failed: ${String(err)}`);
    return;
  }

  if (action === "uninstall") {
    if (resolveIsNixMode(process.env)) {
      fail("Nix mode detected; service uninstall is disabled.");
      return;
    }
    if (loaded) {
      try {
        await service.stop({ env: process.env, stdout });
      } catch {
        // best effort
      }
    }
    try {
      await service.uninstall({ env: process.env as Record<string, string | undefined>, stdout });
    } catch (err) {
      fail(`Research ${opts.kind} uninstall failed: ${String(err)}`);
      return;
    }
    emit({
      ok: true,
      result: "uninstalled",
      service: buildDaemonServiceSnapshot(service, false),
    });
    return;
  }

  if (!loaded) {
    let hints = renderResearchServiceStartHints(opts.kind);
    if (process.platform === "linux") {
      const systemdAvailable = await isSystemdUserServiceAvailable().catch(() => false);
      if (!systemdAvailable) {
        hints = [...hints, ...renderSystemdUnavailableHints({ wsl: await isWSL() })];
      }
    }
    emit({
      ok: true,
      result: "not-loaded",
      message: `Research ${opts.kind} service ${service.notLoadedText}.`,
      hints,
      service: buildDaemonServiceSnapshot(service, loaded),
    });
    if (!json) {
      defaultRuntime.log(`Research ${opts.kind} service ${service.notLoadedText}.`);
      for (const hint of hints) defaultRuntime.log(`Start with: ${hint}`);
    }
    return;
  }

  try {
    if (action === "stop") {
      await service.stop({ env: process.env, stdout });
    } else {
      await service.restart({ env: process.env, stdout });
    }
  } catch (err) {
    fail(`Research ${opts.kind} ${action} failed: ${String(err)}`, renderResearchServiceStartHints(opts.kind));
    return;
  }
  emit({
    ok: true,
    result: action === "restart" ? "restarted" : action === "start" ? "started" : "stopped",
    service: buildDaemonServiceSnapshot(service, action !== "stop"),
  });
}

export async function runResearchServiceStart(opts: ResearchServiceLifecycleOptions) {
  return runResearchServiceLifecycle("start", opts);
}

export async function runResearchServiceStop(opts: ResearchServiceLifecycleOptions) {
  return runResearchServiceLifecycle("stop", opts);
}

export async function runResearchServiceRestart(opts: ResearchServiceLifecycleOptions) {
  return runResearchServiceLifecycle("restart", opts);
}

export async function runResearchServiceUninstall(opts: ResearchServiceLifecycleOptions) {
  return runResearchServiceLifecycle("uninstall", opts);
}

export async function runResearchServiceStatus(opts: ResearchServiceStatusOptions) {
  const json = Boolean(opts.json);
  const service = resolveResearchService(opts.kind);
  const [loaded, command, runtime] = await Promise.all([
    service.isLoaded({ env: process.env }).catch(() => false),
    service.readCommand(process.env as Record<string, string | undefined>).catch(() => null),
    service.readRuntime(process.env as Record<string, string | undefined>).catch((err) => ({
      status: "unknown" as const,
      detail: String(err),
      missingUnit: false,
    })),
  ]);

  const payload = {
    kind: opts.kind,
    service: {
      ...buildDaemonServiceSnapshot(service, loaded),
      command,
      runtime,
    },
  };

  if (json) {
    defaultRuntime.log(JSON.stringify(payload, null, 2));
    return;
  }

  const rich = isRich();
  const label = (value: string) => colorize(rich, theme.muted, value);
  const accent = (value: string) => colorize(rich, theme.accent, value);
  const infoText = (value: string) => colorize(rich, theme.info, value);
  const okText = (value: string) => colorize(rich, theme.success, value);
  const warnText = (value: string) => colorize(rich, theme.warn, value);
  const errorText = (value: string) => colorize(rich, theme.error, value);

  const serviceStatus = loaded ? okText(service.loadedText) : warnText(service.notLoadedText);
  defaultRuntime.log(`${label("Service:")} ${accent(`research-${opts.kind}`)} (${serviceStatus})`);
  if (command?.programArguments?.length) {
    defaultRuntime.log(`${label("Command:")} ${infoText(command.programArguments.join(" "))}`);
  }
  if (command?.sourcePath) {
    defaultRuntime.log(`${label("Service file:")} ${infoText(command.sourcePath)}`);
  }
  const runtimeLine = formatRuntimeStatus(runtime);
  if (runtimeLine) {
    defaultRuntime.log(`${label("Runtime:")} ${infoText(runtimeLine)}`);
  }
  if (!loaded) {
    defaultRuntime.log("");
    for (const hint of renderResearchServiceStartHints(opts.kind)) {
      defaultRuntime.log(`${warnText("Start with:")} ${infoText(hint)}`);
    }
    return;
  }
  if (runtime?.status === "stopped" || runtime?.missingUnit) {
    for (const hint of buildResearchRuntimeHints(opts.kind)) {
      defaultRuntime.error(errorText(hint));
    }
  }
}
