import { describe, expect, it } from "vitest";
import {
  buildTimeSeriesExhibit,
  timeSeriesToCsv,
  type CompanyFactsResponse,
} from "./sec-xbrl-timeseries.js";

describe("sec-xbrl-timeseries", () => {
  it("filters by form, dedupes per period, and sorts chronologically", () => {
    const facts: CompanyFactsResponse = {
      cik: 320193,
      entityName: "Apple Inc.",
      facts: {
        "us-gaap": {
          Revenues: {
            label: "Revenues",
            units: {
              USD: [
                {
                  end: "2024-09-28",
                  filed: "2024-11-01",
                  form: "10-K",
                  fy: 2024,
                  fp: "FY",
                  val: 391000,
                  accn: "a",
                },
                {
                  end: "2024-09-28",
                  filed: "2024-11-05",
                  form: "10-K",
                  fy: 2024,
                  fp: "FY",
                  val: 392000,
                  accn: "b",
                },
                {
                  end: "2025-03-29",
                  filed: "2025-05-01",
                  form: "10-Q",
                  fy: 2025,
                  fp: "Q2",
                  val: 92000,
                  accn: "c",
                },
                {
                  end: "2025-03-29",
                  filed: "2025-05-10",
                  form: "8-K",
                  fy: 2025,
                  fp: "Q2",
                  val: 99999,
                  accn: "ignored-8k",
                },
              ],
            },
          },
        },
      },
    };

    const exhibit = buildTimeSeriesExhibit(facts, ["us-gaap:Revenues"], {
      includeForms: ["10-K", "10-Q"],
    });
    expect(exhibit.series).toHaveLength(1);
    expect(exhibit.series[0]?.points).toHaveLength(2);
    expect(exhibit.series[0]?.points[0]?.end).toBe("2024-09-28");
    expect(exhibit.series[0]?.points[0]?.value).toBe(392000);
    expect(exhibit.series[0]?.points[1]?.end).toBe("2025-03-29");
    expect(exhibit.series[0]?.points[1]?.value).toBe(92000);
  });

  it("renders flat csv rows for each point", () => {
    const facts: CompanyFactsResponse = {
      cik: 1,
      entityName: "Example",
      facts: {
        dei: {
          EntityCommonStockSharesOutstanding: {
            label: "Shares Outstanding",
            units: {
              shares: [
                {
                  end: "2025-12-31",
                  filed: "2026-01-31",
                  form: "10-K",
                  val: 100,
                },
              ],
            },
          },
        },
      },
    };
    const exhibit = buildTimeSeriesExhibit(facts, ["dei:EntityCommonStockSharesOutstanding"]);
    const csv = timeSeriesToCsv(exhibit);
    expect(csv).toContain("seriesKey");
    expect(csv).toContain("dei:EntityCommonStockSharesOutstanding");
    expect(csv).toContain("2025-12-31");
  });
});
