import { describe, expect, it } from "vitest";

import { parseError } from "./api";
import { formatAgo, formatDate, formatINR } from "./format";

describe("formatINR", () => {
  it("uses Indian digit grouping", () => {
    expect(formatINR("4500000.00")).toBe("₹45,00,000");
  });
  it("handles empty values", () => {
    expect(formatINR(null)).toBe("—");
    expect(formatINR("abc")).toBe("—");
  });
});

describe("formatDate", () => {
  it("treats plain dates as UTC calendar dates", () => {
    expect(formatDate("2026-11-05")).toBe("05 Nov 2026");
  });
});

describe("formatAgo", () => {
  it("buckets durations", () => {
    const now = Date.parse("2026-10-28T12:00:00Z");
    expect(formatAgo("2026-10-28T11:30:00Z", now)).toBe("30 min ago");
    expect(formatAgo("2026-10-25T12:00:00Z", now)).toBe("3 days ago");
    expect(formatAgo(null, now)).toBe("never");
  });
});

describe("parseError", () => {
  it("maps FastAPI validation errors to fields", () => {
    const e = parseError(422, {
      detail: [{ loc: ["body", "eta"], msg: "Value error, ETA must be on or after ETD" }],
    });
    expect(e.fieldErrors).toEqual({ eta: "ETA must be on or after ETD" });
    expect(e.message).toBe("eta: ETA must be on or after ETD");
  });
  it("passes plain string details through", () => {
    expect(parseError(409, { detail: "This lane already exists" }).message).toBe(
      "This lane already exists",
    );
  });
});
