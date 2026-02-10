# Quality Postmortem (Why Outputs Are Currently 0/10)

This is a failure-mode inventory grounded in concrete artifacts and the current stack map. It focuses on why “institutional quality” breaks in practice (not just theory).

## Evidence: Two Delivered PDFs (Diagnostics)

The following metrics come from `openclaw research pdf-diagnostics` (PDF text extraction heuristics), run against the delivered artifacts:

### 1) PLTR memo PDF
- Path: `/Users/cooptaylor1/Documents/DrMackenzie Vault/tmp/pdfs/pltr.pdf`
- Pages: 9
- Key failures:
  - `markdownHeadingTokens=22`: the PDF contains literal markdown markers (`###`), indicating markdown was not rendered or was rendered incorrectly.
  - `dashMojibakeDateCount=17`: dates appear with mojibake separators (e.g., `2026 n 02 n 06`), consistent with Unicode dash/font encoding problems.
  - `citationKeyCount=0`: no structured source keys (`S#`/`C#`) detected in the extracted text, which prevents systematic citation + provenance enforcement.

### 2) Agentic commerce PDF (part 1)
- Path: `/Users/cooptaylor1/Documents/DrMackenzie Vault/tmp/pdfs/agentic_part1.pdf`
- Pages: 3
- Key failures:
  - `sourcesHeadingPresent=false` and `urlCount=0`: no Sources appendix and no URLs detected.
  - `exhibitTokenCount=0`: missing exhibits entirely.
  - This artifact is too small (`bytes=7792`) to be a complete “deep research” deliverable.

## Root Cause Top 12 (Ranked)

1. **No fail-closed enforcement between “draft text” and “deliverable artifact”.**
   - The current system has quality gates for markdown, but delivery historically allowed artifacts that were not actually validated post-render (PDF).

2. **Markdown rendering pipeline failure (raw markdown tokens leak into PDFs).**
   - Concrete example: PLTR PDF includes literal `###`.
   - This alone is disqualifying for institutional distribution.

3. **Unicode dash / font encoding breaks numeric readability (dates, minus signs).**
   - Concrete example: PLTR PDF shows repeated “`n`” separators inside dates.
   - This breaks auditability and introduces ambiguity.

4. **Missing Sources appendix (or present but non-machine-usable).**
   - Concrete example: agentic commerce PDF has no Sources section at all.
   - Even when Sources exists, lack of structured keys (e.g., `[S1]`) blocks systematic claim binding.

5. **Citation format is not enforced at the paragraph/claim level.**
   - Today’s memo gating ensures “a Source List exists” but does not guarantee that each factual paragraph is grounded in citations.

6. **Numeric provenance is not a first-class object (numbers can exist without traceability).**
   - Institutional standard requires: value + unit + period + currency + source + accessed_at.
   - Without a contract layer, prose can include numbers that are not auditable.

7. **Evidence corpus is often empty or stale, so retrieval degenerates.**
   - In the local dev DB (`data/research.db`) many primary tables can be empty (prices/filings/transcripts/fundamentals), which forces either:
     - brittle “bootstrapping” behavior, or
     - generic memo outputs without primary evidence depth.

8. **Tool outputs exist, but are not guaranteed to be integrated into the final memo body.**
   - Example: external research ingest and newsletter sync populate the DB, but there is no strict “claim backlog -> prove/drop” loop.

9. **Structure drift and inconsistent section contracts.**
   - Current v3 memo structure is section-based, but downstream rendering/delivery does not enforce ordering, required exhibits, or “N/A with reason” semantics for missing sections.

10. **Weak skepticism mechanics at the output layer.**
   - Some risk and falsifier content exists, but it is not enforced to be measurable, ranked, or tied to monitoring triggers (audit trail).

11. **No editorial pass that removes generic filler language.**
   - Even when content is directionally correct, repeated template phrases and generic framing (“best positioned”, “first-class”, etc.) lowers credibility.

12. **Delivery channel reliability is not part of the artifact lifecycle.**
   - Email/attachment failures and “old artifact reuse” are operational failures, but without manifests + delivery confirmation they appear as “quality failures” to the end user.

## What This Implies for a Fix

To reach institutional standards, the system must add a *contracted report object* (strict JSON) + *evidence library* + *hard validators* + *bounded repair loop* before any rendering/delivery.

The fix should not redesign the entire stack. It should wrap existing analyzers/tools with:
- Contract schemas (reports, evidence, exhibits).
- Validators that fail closed with actionable errors.
- A v2 pipeline that can be toggled on without breaking the existing pipeline.

