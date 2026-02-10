# V2 Research Quality Gates

This document describes the **v2 contract layer** used to enforce institutional-quality research outputs.

## Goals

- **Fail closed**: If sourcing, numeric provenance, or structure is missing, the pipeline must not silently ship a “draft-looking” memo.
- **Reproducible**: A report should be re-renderable from structured JSON artifacts under `runs/<run_id>/`.
- **Auditable**: Every factual paragraph is tied to `sources[]`, and every numeric value is tied to `numeric_facts[]` with a source + timestamp.

## Report Contracts (JSON)

Schemas live in `schemas/`:

- `schemas/source_item.schema.json`
- `schemas/evidence_item.schema.json`
- `schemas/exhibit.schema.json`
- `schemas/report_company.schema.json`
- `schemas/report_theme.schema.json`

The v2 pipeline compiles into `FinalReport.json` which must be schema-valid before any memo rendering.

## Evidence And Citations

- Sources are normalized into `sources[]` with ids `S1`, `S2`, ...
- Reliability tiers:
  - **Tier 1**: SEC filings / audited reports / official releases
  - **Tier 2**: official statistics and datasets (FRED/BLS/etc)
  - **Tier 3**: high-quality journalism / transcripts
  - **Tier 4**: everything else (allowed, but should be labeled)
- Any `FACT` block must include `source_ids` referencing `sources[].id`.

## Numeric Provenance

All numeric values belong in `numeric_facts[]`:

```json
{
  "id": "N1",
  "value": 123,
  "unit": "USD",
  "period": "FY2025",
  "currency": "USD",
  "source_id": "S1",
  "accessed_at": "2026-02-10T00:00:00Z"
}
```

### Prose Convention (No Raw Digits)

To prevent unsourced numbers in prose:

- **Prose blocks may not contain raw digits.**
- To reference a numeric fact inside prose, use a placeholder token and link it:
  - `text: "... {{N1}} ..."`
  - `numeric_refs: ["N1"]`

The markdown renderer is expected to substitute and/or footnote `numeric_facts` during rendering.

Exhibits may include digits, but if they do, they must include `numeric_refs` that back those values.

## Validators

Validators live under `src/v2/quality/validators/` and are orchestrated by:

- `src/v2/quality/quality-gate.ts`

### Schema Validation

The report must satisfy the relevant JSON schema (company/theme). Schema failures are hard errors.

### Section Completeness

Required sections must exist exactly once and in the mandated order.

### Citation Coverage

Every `FACT` block must cite at least one `S#` source id, and all cited ids must exist in `sources[]`.

### Evidence Coverage (Lane 1)

The v2 gate fails closed when primary evidence coverage is missing:

- At least **one Tier 1** source (SEC filings / audited reports / official releases).
- At least **two Tier 1/2** sources total.
- Company reports must include at least one **SEC** source tagged to `company:<TICKER>`.
- Theme reports with a `subject.universe[]` must include at least one Tier 1/2 source tagged `company:<TICKER>` for **each** universe member.

### Numeric Provenance

- Every `numeric_refs` must exist in `numeric_facts[]`.
- Prose may only reference numeric facts via placeholders `{{N#}}`.

### Consistency

Heuristic checks for:

- required exhibit coverage (company vs theme)
- thesis structure constraints (company: `3-5` falsifiable statements with explicit `Falsifier:` markers)

### Style

- Bans a small set of “fluff” phrases.
- Requires each section to include at least one block of each tag: `FACT`, `INTERPRETATION`, `ASSUMPTION` (unless `na_reason` is set).

## Auto-Repair Loop (Bounded)

The v2 gate supports a bounded repair loop:

- `src/v2/quality/repair-loop.ts`
- Maximum attempts: **2**
- Repair input: the prior candidate report JSON + the list of gate issues
- Repair output: a new candidate report JSON

If the report still fails after all attempts, the pipeline must output a **FAILED QUALITY GATE** artifact that includes the final issues.

## PDF Pre-Send Diagnostics (v1 + v2)

Even if markdown gates pass, the final artifact can still fail institutional standards at the PDF layer (e.g., encoding mojibake, missing URLs/citations, placeholder language).

`openclaw research render-pdf` now runs strict PDF diagnostics by default (fail-closed). To bypass for local debugging only, pass `--skip-pdf-diagnostics`.

Use:

```bash
node openclaw.mjs research pdf-diagnostics --in <path-to.pdf> --strict --dump-text tmp/pdfs/<name>.txt
```

`--strict` fails closed with an actionable error list when the PDF violates minimal pre-send requirements:
- no raw markdown tokens (e.g., `###`, ```),
- Sources present + at least one URL,
- at least one structured citation key (`[S#]` or `C#:`),
- at least one `Exhibit N`,
- no dash mojibake (e.g., `2026 n 02 n 10`),
- no placeholder language (e.g., “appendix pass”, “to appear”).
