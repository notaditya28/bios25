"use client";

import { useCallback, useEffect, useState } from "react";

import { PortPicker } from "@/components/PortPicker";
import { Alert, Button, Field, Input, PageHeader } from "@/components/ui";
import { api } from "@/lib/api";
import { formatAgo } from "@/lib/format";
import { useSession } from "@/lib/session";
import type { Port, SourceAdminStatus } from "@/lib/types";

function SourceCard({ s, onRun }: { s: SourceAdminStatus; onRun: () => void }) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4 text-sm">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="font-semibold">{s.name}</h3>
          <p className="text-xs text-slate-500">{s.code} · {s.url}</p>
        </div>
        <div className="flex items-center gap-2">
          <span
            className={`rounded px-2 py-0.5 text-xs font-medium ${
              s.last_error ? "bg-red-100 text-red-800" : s.stale ? "bg-amber-100 text-amber-900" : "bg-emerald-100 text-emerald-800"
            }`}
          >
            {s.last_error ? "Last run failed" : s.stale ? `Stale (> ${s.stale_after_hours} h)` : "Fresh"}
          </span>
          <Button variant="secondary" onClick={onRun}>Run now</Button>
        </div>
      </div>
      <p className="mt-2">Last success: {formatAgo(s.last_success_at)}</p>
      <p className="mt-1 text-slate-600"><span className="font-medium">Licence:</span> {s.licence_note}</p>
      {s.last_error && <pre className="mt-2 overflow-x-auto whitespace-pre-wrap rounded bg-red-50 p-2 text-xs text-red-900">{s.last_error}</pre>}
      <details className="mt-2">
        <summary className="cursor-pointer text-slate-600">Recent runs</summary>
        <ul className="mt-1 space-y-0.5 text-xs">
          {s.recent_runs.map((r, i) => (
            <li key={i}>
              {new Date(r.started_at).toLocaleString("en-IN")} — {r.status}
              {r.rows !== null ? ` · ${r.rows} rows` : ""}
              {r.error ? ` · ${r.error.slice(0, 160)}` : ""}
            </li>
          ))}
        </ul>
      </details>
    </div>
  );
}

function PortCoordinates() {
  const [code, setCode] = useState("");
  const [lat, setLat] = useState("");
  const [lon, setLon] = useState("");
  const [note, setNote] = useState("");
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  return (
    <form
      className="max-w-xl space-y-3 rounded-lg border border-slate-200 bg-white p-4"
      onSubmit={async (e) => {
        e.preventDefault();
        try {
          const p = await api<Port>(`/admin/ports/${code}/coordinates`, {
            method: "PUT",
            json: { lat: Number(lat), lon: Number(lon), note },
          });
          setMsg({ ok: true, text: `${p.name} set to ${p.lat}, ${p.lon} (manual). Ingestion will not overwrite it.` });
        } catch (x) {
          setMsg({ ok: false, text: x instanceof Error ? x.message : "Failed" });
        }
      }}
    >
      <h2 className="font-semibold">Set port coordinates manually</h2>
      <p className="text-xs text-slate-600">
        For ports missing from every reference source (e.g. Jebel Ali, AEJEA). Record where the coordinates
        came from; the change is written to the audit log.
      </p>
      {msg && <Alert kind={msg.ok ? "ok" : "error"}>{msg.text}</Alert>}
      <Field label="Port"><PortPicker mode="sea" value={code} onChange={setCode} /></Field>
      <div className="grid grid-cols-2 gap-3">
        <Field label="Latitude"><Input required inputMode="decimal" value={lat} onChange={(e) => setLat(e.target.value)} /></Field>
        <Field label="Longitude"><Input required inputMode="decimal" value={lon} onChange={(e) => setLon(e.target.value)} /></Field>
      </div>
      <Field label="Source / note"><Input required minLength={3} value={note} onChange={(e) => setNote(e.target.value)} placeholder="e.g. Port authority website, terminal gate" /></Field>
      <Button type="submit" disabled={!code}>Save coordinates</Button>
    </form>
  );
}

export default function AdminSourcesPage() {
  const { me } = useSession();
  const [sources, setSources] = useState<SourceAdminStatus[]>([]);
  const [msg, setMsg] = useState<string | null>(null);
  const load = useCallback(() => {
    api<SourceAdminStatus[]>("/admin/sources").then(setSources).catch((e) => setMsg(e.message));
  }, []);
  useEffect(load, [load]);

  if (!me.is_platform_admin) return <Alert>Platform admins only.</Alert>;

  return (
    <>
      <PageHeader title="Data sources">
        <Button variant="secondary" onClick={load}>Refresh</Button>
      </PageHeader>
      {msg && <Alert kind="info">{msg}</Alert>}
      {sources.length === 0 && (
        <Alert kind="info">No source has run yet. Run a port refresh to load reference data.</Alert>
      )}
      <div className="mt-3 grid gap-3 lg:grid-cols-2">
        {sources.map((s) => (
          <SourceCard
            key={s.code}
            s={s}
            onRun={async () => {
              const r = await api<{ message: string }>(`/admin/sources/${s.code}/run`, { method: "POST" });
              setMsg(r.message);
            }}
          />
        ))}
      </div>
      <div className="mt-8">
        <PortCoordinates />
      </div>
    </>
  );
}
