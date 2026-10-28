"use client";

import { useState } from "react";

import { NeedsCompany } from "@/components/NeedsCompany";
import { Alert, Button, Field, PageHeader, Select, Table } from "@/components/ui";
import { api } from "@/lib/api";
import { useSession } from "@/lib/session";
import type { Company, ImportResult } from "@/lib/types";

const KINDS = {
  partners: "Partners (buyers & suppliers)",
  lanes: "Trade lanes",
  shipments: "Shipments (lanes are created automatically if missing)",
} as const;
type Kind = keyof typeof KINDS;

function Importer({ company }: { company: Company }) {
  const { org } = useSession();
  const [kind, setKind] = useState<Kind>("partners");
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function run(dryRun: boolean) {
    if (!file) return;
    setBusy(true);
    setError(null);
    const form = new FormData();
    form.append("file", file);
    try {
      const r = await api<ImportResult>(`/orgs/${org.organization_id}/imports/${kind}`, {
        form,
        query: { company_id: company.id, dry_run: dryRun },
      });
      setResult(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Upload failed");
      setResult(null);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <PageHeader title="CSV import" />
      <div className="max-w-2xl space-y-4 rounded-lg border border-slate-200 bg-white p-5">
        <p className="text-sm text-slate-600">
          Imports are all-or-nothing: if any row has an error, nothing is saved and you get a row-by-row
          report. Validate first, fix the file, then import. Dates may be YYYY-MM-DD or DD/MM/YYYY; values
          like 45,00,000 are accepted. Ports are UN/LOCODEs (e.g. INNSA).
        </p>
        <Field label="What are you importing?">
          <Select value={kind} onChange={(e) => { setKind(e.target.value as Kind); setResult(null); }}>
            {Object.entries(KINDS).map(([k, l]) => (
              <option key={k} value={k}>{l}</option>
            ))}
          </Select>
        </Field>
        <p className="text-sm">
          <a className="text-brand-600 underline" href={`/api/imports/templates/${kind}.csv`}>
            Download the {kind} template
          </a>
        </p>
        <Field label="CSV file (max 5 MB, 5,000 rows)">
          <input
            type="file"
            accept=".csv,text/csv"
            className="text-sm"
            onChange={(e) => { setFile(e.target.files?.[0] ?? null); setResult(null); }}
          />
        </Field>
        <div className="flex gap-2">
          <Button variant="secondary" disabled={!file || busy} onClick={() => run(true)}>Validate</Button>
          <Button disabled={!file || busy} onClick={() => run(false)}>Import</Button>
        </div>
        {error && <Alert>{error}</Alert>}
        {result && (
          <div className="space-y-3">
            {result.status === "committed" && (
              <Alert kind="ok">Imported {result.created_count} of {result.row_count} rows.</Alert>
            )}
            {result.status === "validated" && (
              <Alert kind="ok">All {result.row_count} rows are valid. Click Import to save them.</Alert>
            )}
            {result.status === "rejected" && (
              <Alert>
                {result.errors.length} problem(s) in {result.row_count} rows{result.dry_run ? "" : "; nothing was saved"}.
              </Alert>
            )}
            {result.errors.length > 0 && (
              <Table head={["Row", "Field", "Problem"]}>
                {result.errors.map((e, i) => (
                  <tr key={i}>
                    <td className="px-3 py-1.5">{e.row}</td>
                    <td className="px-3 py-1.5">{e.field ?? "—"}</td>
                    <td className="px-3 py-1.5">{e.message}</td>
                  </tr>
                ))}
              </Table>
            )}
          </div>
        )}
      </div>
    </>
  );
}

export default function ImportPage() {
  return <NeedsCompany>{(c) => <Importer company={c} />}</NeedsCompany>;
}
