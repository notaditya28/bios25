"use client";

import { useCallback, useEffect, useState } from "react";

import { Alert, Button, PageHeader } from "@/components/ui";
import { api } from "@/lib/api";
import { formatAgo, formatDate } from "@/lib/format";
import { useSession } from "@/lib/session";
import type { MatchStatus, Page, ScreeningMatch, ScreeningSummary } from "@/lib/types";

const SUBJECT_LABEL = { partner: "Partner", company: "Client company", vessel: "Vessel on shipment" };

function ScoreBadge({ score }: { score: number }) {
  const cls = score >= 95 ? "bg-red-100 text-red-800" : score >= 90 ? "bg-orange-100 text-orange-800" : "bg-amber-100 text-amber-900";
  return <span className={`rounded px-2 py-0.5 text-sm font-semibold ${cls}`}>{score.toFixed(1)}</span>;
}

function Facts({ label, values }: { label: string; values: string[] }) {
  if (values.length === 0) return null;
  return (
    <div className="text-sm">
      <span className="text-slate-500">{label}: </span>
      {values.join(", ")}
    </div>
  );
}

function MatchCard({ m, isOwner, onChange }: { m: ScreeningMatch; isOwner: boolean; onChange: () => void }) {
  const { org } = useSession();
  const url = `/orgs/${org.organization_id}/screening/matches/${m.id}`;
  const [note, setNote] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function decide(status: "confirmed" | "dismissed") {
    setBusy(true);
    setError(null);
    try {
      await api(`${url}/decision`, { json: { status, note } });
      onChange();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed");
    } finally {
      setBusy(false);
    }
  }

  const b = m.breakdown;
  return (
    <article className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <ScoreBadge score={m.score} />
        <span className="text-sm text-slate-600">
          {b.method === "imo_exact" ? `Exact IMO number match (${b.imo})` : `Name match · ${b.method.replace(/_/g, " ")}`}
        </span>
        {!m.entry.active && <span className="rounded bg-slate-100 px-2 py-0.5 text-xs">Entry delisted</span>}
        <span className="ml-auto text-xs text-slate-500">Screened {formatAgo(m.screened_at)}</span>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        <section className="rounded-md bg-slate-50 p-3">
          <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Your {SUBJECT_LABEL[m.subject.type].toLowerCase()}
          </h3>
          <p className="font-medium">{m.subject.name || "—"}</p>
          <Facts label="Country" values={m.subject.country ? [m.subject.country] : []} />
          <Facts label="IMO" values={m.subject.imo ? [m.subject.imo] : []} />
        </section>
        <section className="rounded-md bg-red-50/60 p-3">
          <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-red-800">
            {m.entry.list_name} · {m.entry.entity_type}
          </h3>
          <p className="font-medium">{m.entry.primary_name}</p>
          {m.matched_name !== m.entry.primary_name && (
            <p className="text-sm">Matched alias: <span className="font-medium">{m.matched_name}</span></p>
          )}
          <Facts label="Programmes" values={m.entry.programs} />
          <Facts label="Countries" values={m.entry.countries} />
          <Facts label="Dates of birth" values={m.entry.birth_dates} />
          <Facts label="IMO" values={m.entry.imo_numbers} />
          <Facts label="Listed" values={m.entry.listed_on ? [formatDate(m.entry.listed_on)] : []} />
          {m.entry.names.length > 1 && (
            <details className="mt-1 text-sm">
              <summary className="cursor-pointer text-slate-600">All {m.entry.names.length} names</summary>
              <ul className="mt-1 list-disc pl-5">{m.entry.names.map((n, i) => <li key={i}>{n}</li>)}</ul>
            </details>
          )}
          {m.entry.remarks && <p className="mt-1 text-xs text-slate-600">{m.entry.remarks.slice(0, 400)}</p>}
          {m.entry.source_url && (
            <a className="mt-1 inline-block text-sm text-brand-600 underline" href={m.entry.source_url} target="_blank" rel="noreferrer noopener">
              View on official list
            </a>
          )}
        </section>
      </div>

      <details className="mt-3 text-sm">
        <summary className="cursor-pointer text-slate-600">Why this score?</summary>
        <div className="mt-2 space-y-1">
          {b.name_score !== null && <p>Name similarity: {b.name_score}</p>}
          {b.token_pairs && b.token_pairs.length > 0 && (
            <p>
              Word pairs:{" "}
              {b.token_pairs.map(([s, c, v], i) => (
                <span key={i} className="mr-2 inline-block rounded bg-slate-100 px-1.5">
                  {s} ↔ {c} ({Math.round(v * 100)}%)
                </span>
              ))}
            </p>
          )}
          {b.adjustments.map((a, i) => (
            <p key={i}>{a.delta > 0 ? "+" : ""}{a.delta} — {a.reason}</p>
          ))}
        </div>
      </details>

      {m.status === "pending" ? (
        <div className="mt-4 space-y-2 border-t border-slate-100 pt-3">
          {error && <Alert>{error}</Alert>}
          <label className="block text-sm">
            <span className="mb-1 block font-medium">Review note (required — kept in the audit log)</span>
            <textarea
              className="w-full rounded-md border border-slate-300 p-2 text-sm"
              rows={2}
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="e.g. Different entity: buyer's trade licence 123456 issued in Dubai, no link to listed party"
            />
          </label>
          <div className="flex gap-2">
            <Button variant="danger" disabled={busy || note.trim().length < 5} onClick={() => decide("confirmed")}>
              Confirm match
            </Button>
            <Button variant="secondary" disabled={busy || note.trim().length < 5} onClick={() => decide("dismissed")}>
              Dismiss as false positive
            </Button>
          </div>
        </div>
      ) : (
        <div className="mt-4 flex flex-wrap items-center gap-3 border-t border-slate-100 pt-3 text-sm">
          <span className={m.status === "confirmed" ? "font-semibold text-red-800" : "text-slate-700"}>
            {m.status === "confirmed" ? "Confirmed match" : "Dismissed"} by {m.reviewed_by ?? "—"} on {formatDate(m.reviewed_at)}
          </span>
          <span className="text-slate-600">“{m.review_note}”</span>
          {isOwner && (
            <Button
              variant="secondary"
              className="ml-auto"
              onClick={async () => {
                await api(`${url}/reopen`, { json: { note: null } });
                onChange();
              }}
            >
              Reopen
            </Button>
          )}
        </div>
      )}
    </article>
  );
}

export default function ScreeningPage() {
  const s = useSession();
  const base = `/orgs/${s.org.organization_id}/screening`;
  const [tab, setTab] = useState<MatchStatus>("pending");
  const [summary, setSummary] = useState<ScreeningSummary | null>(null);
  const [data, setData] = useState<Page<ScreeningMatch> | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const load = useCallback(() => {
    api<ScreeningSummary>(`${base}/summary`).then(setSummary).catch((e) => setMsg(e.message));
    api<Page<ScreeningMatch>>(`${base}/matches`, { query: { status: tab, limit: 50 } })
      .then(setData)
      .catch((e) => setMsg(e.message));
  }, [base, tab]);
  useEffect(load, [load]);

  return (
    <>
      <PageHeader title="Sanctions screening">
        {s.isOwner && (
          <Button
            variant="secondary"
            onClick={async () => {
              await api(`${base}/rescreen`, { method: "POST" });
              setMsg("Re-screening queued. Results appear here within a minute.");
            }}
          >
            Re-screen everything now
          </Button>
        )}
      </PageHeader>

      {summary?.lists_stale && (
        <Alert>
          One or more sanctions lists could not be refreshed recently, so screening may be missing the newest
          designations. Details are below.
        </Alert>
      )}
      {msg && <Alert kind="info">{msg}</Alert>}

      <p className="mt-2 text-sm text-slate-600">
        Partners, client companies and vessels are screened automatically when saved and again whenever a
        list changes. {summary && <>{summary.subjects_screened} screened · last run {formatAgo(summary.last_run_at)}.</>}
      </p>

      <div className="mt-4 flex gap-1 border-b border-slate-200">
        {(["pending", "confirmed", "dismissed"] as MatchStatus[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`border-b-2 px-3 py-2 text-sm capitalize ${tab === t ? "border-brand-600 font-medium" : "border-transparent text-slate-600"}`}
          >
            {t === "pending" ? "To review" : t} {summary && <span className="ml-1 rounded bg-slate-100 px-1.5 text-xs">{summary[t]}</span>}
          </button>
        ))}
      </div>

      <div className="mt-4 space-y-4">
        {data?.items.length === 0 && (
          <p className="rounded-lg border border-slate-200 bg-white p-6 text-center text-sm text-slate-500">
            {tab === "pending" ? "Nothing to review. No counterparty currently matches a sanctions list." : `No ${tab} matches.`}
          </p>
        )}
        {data?.items.map((m) => <MatchCard key={m.id} m={m} isOwner={s.isOwner} onChange={load} />)}
      </div>

      <section className="mt-10">
        <h2 className="mb-2 font-semibold">Lists screened against</h2>
        <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr><th className="px-3 py-2">List</th><th className="px-3 py-2">Entries</th><th className="px-3 py-2">Publisher&apos;s date</th><th className="px-3 py-2">Last fetched</th><th className="px-3 py-2">Status</th></tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {summary?.lists.map((l) => (
                <tr key={l.code}>
                  <td className="px-3 py-2">{l.name}<div className="text-xs text-slate-500">{l.publisher}</div></td>
                  <td className="px-3 py-2">{l.entry_count.toLocaleString("en-IN")}</td>
                  <td className="px-3 py-2">{formatDate(l.published_at)}</td>
                  <td className="px-3 py-2">{formatAgo(l.fetched_at)}</td>
                  <td className="px-3 py-2">
                    {l.stale ? <span className="text-red-700">Stale</span> : <span className="text-emerald-700">Current</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}
