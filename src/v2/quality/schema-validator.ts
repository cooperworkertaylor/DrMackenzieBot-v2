import type { ErrorObject } from "ajv";
import Ajv from "ajv/dist/2020.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ReportKindV2 } from "./types.js";

type ValidateFn = ((value: unknown) => boolean) & { errors?: ErrorObject[] | null };

const moduleDir = path.dirname(fileURLToPath(import.meta.url));
const schemaRoot = (): string => {
  const candidates = [
    process.env.OPENCLAW_RESEARCH_SCHEMA_ROOT?.trim() || "",
    path.resolve(process.cwd(), "schemas"),
    path.resolve(moduleDir, "../../../schemas"),
    path.resolve(moduleDir, "../../../../schemas"),
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, "source_item.schema.json"))) {
      return candidate;
    }
  }
  throw new Error(
    `Could not locate v2 schema directory. Tried: ${candidates.join(", ") || "(none)"}`,
  );
};

const readJson = (filePath: string): unknown => {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw) as unknown;
};

type SchemaBundle = {
  ajv: unknown;
  validateCompany: ValidateFn;
  validateTheme: ValidateFn;
};

let bundle: SchemaBundle | null = null;

export function resetSchemaBundleForTests(): void {
  bundle = null;
}

function loadSchemasOnce(): SchemaBundle {
  if (bundle) return bundle;
  // Ajv's ESM dist entrypoint typings can be awkward under NodeNext; treat the ctor as opaque.
  const AjvCtor = Ajv as unknown as new (opts: {
    allErrors: boolean;
    strict: boolean;
    allowUnionTypes: boolean;
  }) => {
    addSchema(schema: unknown, key?: string): void;
    getSchema(key: string): ValidateFn | undefined;
  };

  const ajv = new AjvCtor({
    allErrors: true,
    strict: true,
    allowUnionTypes: false,
  });

  // Load shared schemas first so $ref resolution works.
  const schemaFiles = [
    "source_item.schema.json",
    "evidence_item.schema.json",
    "exhibit.schema.json",
    "report_company.schema.json",
    "report_theme.schema.json",
  ];

  for (const name of schemaFiles) {
    const filePath = path.join(schemaRoot(), name);
    const schema = readJson(filePath) as { $id?: string };
    ajv.addSchema(schema, schema.$id ?? name);
  }

  const validateCompany = ajv.getSchema("report_company.schema.json");
  const validateTheme = ajv.getSchema("report_theme.schema.json");
  if (!validateCompany || !validateTheme) {
    throw new Error("Failed to load v2 report schemas");
  }

  bundle = {
    ajv,
    validateCompany,
    validateTheme,
  };
  return bundle;
}

export function validateReportJsonSchema(params: { kind: ReportKindV2; report: unknown }): {
  valid: boolean;
  errors: string[];
} {
  const { validateCompany, validateTheme } = loadSchemasOnce();
  const validate = params.kind === "company" ? validateCompany : validateTheme;
  const ok = Boolean(validate(params.report));
  const errors =
    validate.errors?.map((err: ErrorObject) => {
      const where = err.instancePath || err.schemaPath || "";
      const msg = err.message || "schema validation error";
      return where ? `${where}: ${msg}` : msg;
    }) ?? [];
  return { valid: ok, errors };
}
