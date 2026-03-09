import type { GatewayDaemonRuntime } from "./daemon-runtime.js";
import { resolveGatewayDevMode } from "./daemon-install-helpers.js";
import { formatResearchServiceDescription } from "../daemon/constants.js";
import { resolveResearchProgramArguments } from "../daemon/program-args.js";
import {
  renderSystemNodeWarning,
  resolvePreferredNodePath,
  resolveSystemNodeInfo,
} from "../daemon/runtime-paths.js";
import { buildResearchServiceEnvironment } from "../daemon/service-env.js";
import type { ResearchServiceKind } from "../daemon/research-service.js";

type WarnFn = (message: string, title?: string) => void;

export type ResearchInstallPlan = {
  programArguments: string[];
  workingDirectory?: string;
  environment: Record<string, string | undefined>;
  description?: string;
};

export async function buildResearchInstallPlan(params: {
  env: Record<string, string | undefined>;
  kind: ResearchServiceKind;
  dbPath?: string;
  intervalMs?: number;
  runtime: GatewayDaemonRuntime;
  devMode?: boolean;
  nodePath?: string;
  warn?: WarnFn;
}): Promise<ResearchInstallPlan> {
  const devMode = params.devMode ?? resolveGatewayDevMode();
  const nodePath =
    params.nodePath ??
    (await resolvePreferredNodePath({
      env: params.env,
      runtime: params.runtime,
    }));
  const { programArguments, workingDirectory } = await resolveResearchProgramArguments({
    kind: params.kind,
    dbPath: params.dbPath,
    intervalMs: params.intervalMs,
    dev: devMode,
    runtime: params.runtime,
    nodePath,
  });

  if (params.runtime === "node") {
    const systemNode = await resolveSystemNodeInfo({ env: params.env });
    const warning = renderSystemNodeWarning(systemNode, programArguments[0]);
    if (warning) {
      params.warn?.(warning, "Research daemon runtime");
    }
  }

  const environment = buildResearchServiceEnvironment({
    env: params.env,
    kind: params.kind,
  });
  const description = formatResearchServiceDescription({
    kind: params.kind,
    version: environment.OPENCLAW_SERVICE_VERSION,
  });

  return { programArguments, workingDirectory, environment, description };
}
