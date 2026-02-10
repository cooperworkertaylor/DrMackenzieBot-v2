import Ajv from "ajv/dist/2020.js";
import fs from "node:fs";
import path from "node:path";
import type { ReportKindV2 } from "./types.js";

type ValidateFn = ReturnType<Ajv["compile"]>;

const schemaRoot = (): string => path.resolve(process.cwd(), "schemas");

const readJson = (filePath: string): unknown => {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw) as unknown;
};

type SchemaBundle = {
  ajv: Ajv;
  validateCompany: ValidateFn;
  validateTheme: ValidateFn;
};

let bundle: SchemaBundle | null = null;

function loadSchemasOnce(): SchemaBundle {
  if (bundle) return bundle;
  const ajv = new Ajv({
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
    validate.errors?.map((err) => {
      const where = err.instancePath || err.schemaPath || "";
      const msg = err.message || "schema validation error";
      return where ? `${where}: ${msg}` : msg;
    }) ?? [];
  return { valid: ok, errors };
}
