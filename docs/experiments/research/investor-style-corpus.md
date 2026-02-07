# Investor Style Corpus (Institutional Lens)

Updated: 2026-02-07

This corpus targets the writing quality and investor process patterns from:
Altimeter, a16z, Benedict Evans, Durable Capital, Goldman Sachs, Founders Fund, and Duquesne Family Office (Stanley Druckenmiller).

## Scope Guardrails (Important)

- These references are for style, rigor, structure, and information density only.
- Do not copy or inherit their specific conclusions, recommendations, or portfolio positions.
- Every output must be re-underwritten from current evidence in our own pipeline.
- Citations must always point to the evidence used in the current run.

## Public Primary Sources

### Altimeter / Brad Gerstner
- Altimeter home: https://www.altimeter.com/Home
- Public open letter example (Meta): https://medium.com/@alt.cap/time-to-get-fit-an-open-letter-from-altimeter-to-mark-zuckerberg-and-the-meta-board-of-392d94e80a18
- SEC manager filings (Altimeter): https://www.sec.gov/edgar/browse/?CIK=0001456078

### a16z
- Big Ideas 2025: https://a16z.com/big-ideas-in-tech-2025/
- State of Crypto 2025: https://a16zcrypto.com/posts/article/state-of-crypto-report-2025/
- a16z essays and research index: https://a16z.com/latest/

### Benedict Evans
- Presentations hub: https://www.ben-evans.com/presentations
- 2025 autumn presentation PDF: https://www.ben-evans.com/benedictevans/2025/11/20/ai-eats-the-world
- 2025 spring presentation PDF: https://www.ben-evans.com/benedictevans/2025/6/2/ai-eats-the-world

### Durable Capital
- SEC manager filings (Durable Capital Partners LP): https://www.sec.gov/edgar/browse/?CIK=0001798849
- Recent 13F filing detail (Q3 2025): https://www.sec.gov/Archives/edgar/data/1798849/000179884925000015/0001798849-25-000015-index.html

### Goldman Sachs
- Top of Mind reports hub: https://www.goldmansachs.com/insights/top-of-mind
- US Equity Outlook 2026: https://www.goldmansachs.com/insights/articles/us-equity-outlook-2026
- Goldman Sachs Asset Management 2026 Outlook: https://www.gsam.com/content/gsam/us/en/advisors/insights/article/2026-outlooks.html

### Founders Fund
- Anatomy of Next: https://foundersfund.com/anatomy-of-next/
- Essays and articles archive: https://foundersfund.com/anatomy-of-next/articles-essays-more/
- Transparent Term Sheet (investor/founder framing quality): https://foundersfund.com/termsheet/

### Duquesne Family Office (Stanley Druckenmiller)
- SEC manager filings (Duquesne Family Office LLC): https://www.sec.gov/edgar/browse/?CIK=0001536411
- Recent 13F filing detail (Q3 2025): https://www.sec.gov/Archives/edgar/data/1536411/000153641125000017/0001536411-25-000017-index.html

## Availability Notes

- LP letters from Durable Capital, Founders Fund, and Duquesne Family Office are generally not public.
- Goldman Global Investment Research PDFs are often paywalled/client-only.
- The above list uses official and public sources first; where letters are private, SEC filings and public research outputs are used as institutional proxies.

## Style Targets For Agent Outputs

Use this blend for company/theme/sector reports:

1. Altimeter: founder-centric long-duration thesis with explicit capital allocation implications.
2. a16z: network and platform lens with clear first-principles technology framing.
3. Benedict Evans: high signal density, concise strategic framing, strong trend decomposition.
4. Durable/Duquesne (proxy via filings): position-level discipline, concentration logic, risk asymmetry.
5. Goldman: cross-sectional context, scenario math, benchmark-relative framing, macro linkage.
6. Founders Fund: non-consensus thinking, explicit second-order effects, decision-forcing clarity.

## Style Quality Rubric (Use In Every Deliverable)

- `thesis_clarity`: Thesis stated in one sentence, with explicit variant vs consensus.
- `decision_usefulness`: Clear entry criteria, sizing framework, risk budget, and stop conditions.
- `scenario_specificity`: Base/bull/bear scenarios have named drivers and priced outcomes.
- `evidence_density`: High signal-to-noise with multi-source, dated, auditable citations.
- `counterargument_strength`: Strong disconfirming evidence and direct contradiction handling.
- `benchmark_context`: Relative framing vs peers/index and market-implied expectations.
- `brevity_with_depth`: Dense, structured writing without filler.

## Enforced Output Structure (Investor Lens)

Every institutional report should contain:

- variant perception vs consensus
- explicit base/bull/bear scenario pricing
- benchmark-relative context and peer spread
- disconfirming evidence and contradiction resolution
- position sizing, risk budget, and falsification triggers
- dated, multi-host citations
