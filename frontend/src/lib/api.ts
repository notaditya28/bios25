// Thin fetch wrapper: same-origin /api (proxied to FastAPI), session cookie, CSRF header.

let csrfToken: string | null = null;

export function setCsrfToken(token: string | null) {
  csrfToken = token;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public fieldErrors: Record<string, string> = {},
  ) {
    super(message);
  }
}

type FastApiDetail = string | { loc?: (string | number)[]; msg: string }[] | undefined;

export function parseError(status: number, body: { detail?: FastApiDetail } | null): ApiError {
  const detail = body?.detail;
  if (Array.isArray(detail)) {
    const fieldErrors: Record<string, string> = {};
    for (const d of detail) {
      const field = (d.loc ?? []).filter((x) => x !== "body").join(".") || "form";
      fieldErrors[field] = d.msg.replace(/^Value error, /, "");
    }
    const first = Object.entries(fieldErrors)[0];
    return new ApiError(status, first ? `${first[0]}: ${first[1]}` : "Invalid input", fieldErrors);
  }
  if (typeof detail === "string") return new ApiError(status, detail);
  if (status === 429) return new ApiError(status, "Too many attempts; please wait and retry.");
  return new ApiError(status, `Request failed (${status})`);
}

export async function api<T>(
  path: string,
  opts: { method?: string; json?: unknown; form?: FormData; query?: Record<string, unknown> } = {},
): Promise<T> {
  const method = opts.method ?? (opts.json !== undefined || opts.form ? "POST" : "GET");
  const headers: Record<string, string> = {};
  if (opts.json !== undefined) headers["Content-Type"] = "application/json";
  if (method !== "GET" && csrfToken) headers["X-CSRF-Token"] = csrfToken;

  let url = `/api${path}`;
  if (opts.query) {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(opts.query)) {
      if (v === undefined || v === null || v === "") continue;
      if (Array.isArray(v)) v.forEach((x) => qs.append(k, String(x)));
      else qs.set(k, String(v));
    }
    const s = qs.toString();
    if (s) url += `?${s}`;
  }

  const res = await fetch(url, {
    method,
    headers,
    credentials: "same-origin",
    body: opts.form ?? (opts.json !== undefined ? JSON.stringify(opts.json) : undefined),
  });
  if (res.status === 204) return undefined as T;
  const body = await res.json().catch(() => null);
  if (!res.ok) throw parseError(res.status, body);
  return body as T;
}
