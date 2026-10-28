const inr = new Intl.NumberFormat("en-IN", {
  style: "currency",
  currency: "INR",
  maximumFractionDigits: 0,
});

export function formatINR(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  const n = typeof value === "string" ? Number(value) : value;
  return Number.isFinite(n) ? inr.format(n) : "—";
}

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso.length === 10 ? `${iso}T00:00:00Z` : iso);
  return d.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric",
    timeZone: "UTC" });
}

export function formatAgo(iso: string | null | undefined, now = Date.now()): string {
  if (!iso) return "never";
  const s = Math.round((now - new Date(iso).getTime()) / 1000);
  if (s < 90) return "just now";
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  if (s < 129600) return `${Math.round(s / 3600)} h ago`;
  return `${Math.round(s / 86400)} days ago`;
}

export function portLabel(p: { unlocode: string; name: string } | null | undefined): string {
  return p ? `${p.name} (${p.unlocode})` : "—";
}

export const STATUS_LABEL: Record<string, string> = {
  planned: "Planned",
  booked: "Booked",
  in_transit: "In transit",
  delivered: "Delivered",
  cancelled: "Cancelled",
};
