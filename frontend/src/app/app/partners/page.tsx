"use client";

import { useCallback, useEffect, useState } from "react";

import { NeedsCompany } from "@/components/NeedsCompany";
import { PortPicker } from "@/components/PortPicker";
import { Alert, Button, DemoBanner, Field, Input, Modal, PageHeader, Select, Table } from "@/components/ui";
import { api, ApiError } from "@/lib/api";
import { portLabel } from "@/lib/format";
import { useSession } from "@/lib/session";
import type { Company, Page, Partner } from "@/lib/types";

type Form = {
  kind: "buyer" | "supplier";
  name: string;
  country: string;
  city: string;
  port: string;
  contact_email: string;
  notes: string;
};

const empty: Form = { kind: "buyer", name: "", country: "", city: "", port: "", contact_email: "", notes: "" };

function PartnerForm({
  initial,
  onSave,
  onCancel,
}: {
  initial?: Partner;
  onSave: (f: Form) => Promise<void>;
  onCancel: () => void;
}) {
  const [f, setF] = useState<Form>(
    initial
      ? {
          kind: initial.kind,
          name: initial.name,
          country: initial.country,
          city: initial.city ?? "",
          port: initial.port?.unlocode ?? "",
          contact_email: initial.contact_email ?? "",
          notes: initial.notes ?? "",
        }
      : empty,
  );
  const [err, setErr] = useState<ApiError | null>(null);
  const [busy, setBusy] = useState(false);
  const set = (k: keyof Form) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setF({ ...f, [k]: e.target.value });

  return (
    <form
      className="space-y-3"
      onSubmit={async (e) => {
        e.preventDefault();
        setBusy(true);
        setErr(null);
        try {
          await onSave(f);
        } catch (x) {
          setErr(x instanceof ApiError ? x : new ApiError(0, "Save failed"));
        } finally {
          setBusy(false);
        }
      }}
    >
      {err && <Alert>{err.message}</Alert>}
      <div className="grid grid-cols-2 gap-3">
        <Field label="Type">
          <Select value={f.kind} onChange={set("kind")}>
            <option value="buyer">Buyer</option>
            <option value="supplier">Supplier</option>
          </Select>
        </Field>
        <Field label="Country" hint="Name or ISO code, e.g. NL" error={err?.fieldErrors.country}>
          <Input required value={f.country} onChange={set("country")} />
        </Field>
      </div>
      <Field label="Legal name" hint="Exactly as on contracts/invoices — used for sanctions screening." error={err?.fieldErrors.name}>
        <Input required value={f.name} onChange={set("name")} />
      </Field>
      <div className="grid grid-cols-2 gap-3">
        <Field label="City">
          <Input value={f.city} onChange={set("city")} />
        </Field>
        <Field label="Contact email" error={err?.fieldErrors.contact_email}>
          <Input type="email" value={f.contact_email} onChange={set("contact_email")} />
        </Field>
      </div>
      <Field label="Usual port" error={err?.fieldErrors.port}>
        <PortPicker value={f.port} initial={initial?.port} onChange={(port) => setF((p) => ({ ...p, port }))} />
      </Field>
      <Field label="Notes">
        <Input value={f.notes} onChange={set("notes")} />
      </Field>
      <div className="flex justify-end gap-2">
        <Button type="button" variant="secondary" onClick={onCancel}>Cancel</Button>
        <Button type="submit" disabled={busy}>{busy ? "Saving…" : "Save"}</Button>
      </div>
    </form>
  );
}

function Partners({ company }: { company: Company }) {
  const { org } = useSession();
  const base = `/orgs/${org.organization_id}/partners`;
  const [data, setData] = useState<Page<Partner> | null>(null);
  const [q, setQ] = useState("");
  const [editing, setEditing] = useState<Partner | "new" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    api<Page<Partner>>(base, { query: { company_id: company.id, q, limit: 200 } })
      .then(setData)
      .catch((e) => setError(e.message));
  }, [base, company.id, q]);
  useEffect(load, [load]);

  const toBody = (f: Form) => ({
    ...f,
    port: f.port || null,
    city: f.city || null,
    contact_email: f.contact_email || null,
    notes: f.notes || null,
  });

  return (
    <>
      <PageHeader title="Trading partners">
        <Input placeholder="Search name" value={q} onChange={(e) => setQ(e.target.value)} />
        <Button onClick={() => setEditing("new")}>Add partner</Button>
      </PageHeader>
      {error && <Alert>{error}</Alert>}
      <DemoBanner show={!!data?.items.some((p) => p.is_demo)} />
      <div className="mt-3">
        <Table head={["Name", "Type", "Country", "City", "Port", ""]} empty={data?.items.length === 0}>
          {data?.items.map((p) => (
            <tr key={p.id}>
              <td className="px-3 py-2 font-medium">{p.name}</td>
              <td className="px-3 py-2 capitalize">{p.kind}</td>
              <td className="px-3 py-2">{p.country}</td>
              <td className="px-3 py-2">{p.city ?? "—"}</td>
              <td className="px-3 py-2">{portLabel(p.port)}</td>
              <td className="space-x-2 whitespace-nowrap px-3 py-2 text-right">
                <Button variant="secondary" onClick={() => setEditing(p)}>Edit</Button>
                <Button
                  variant="danger"
                  onClick={async () => {
                    if (!confirm(`Delete ${p.name}?`)) return;
                    await api(`${base}/${p.id}`, { method: "DELETE" }).catch((e) => setError(e.message));
                    load();
                  }}
                >
                  Delete
                </Button>
              </td>
            </tr>
          ))}
        </Table>
      </div>
      {editing && (
        <Modal title={editing === "new" ? "Add partner" : "Edit partner"} onClose={() => setEditing(null)}>
          <PartnerForm
            initial={editing === "new" ? undefined : editing}
            onCancel={() => setEditing(null)}
            onSave={async (f) => {
              if (editing === "new") await api(base, { json: { ...toBody(f), company_id: company.id } });
              else await api(`${base}/${editing.id}`, { method: "PUT", json: toBody(f) });
              setEditing(null);
              load();
            }}
          />
        </Modal>
      )}
    </>
  );
}

export default function PartnersPage() {
  return <NeedsCompany>{(c) => <Partners company={c} />}</NeedsCompany>;
}
