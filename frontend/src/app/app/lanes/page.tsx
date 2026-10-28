"use client";

import { useCallback, useEffect, useState } from "react";

import { NeedsCompany } from "@/components/NeedsCompany";
import { PortPicker } from "@/components/PortPicker";
import { Alert, Button, DemoBanner, Field, Input, Modal, PageHeader, Select, Table } from "@/components/ui";
import { api, ApiError } from "@/lib/api";
import { portLabel } from "@/lib/format";
import { useSession } from "@/lib/session";
import type { Company, Lane, Mode, Page, Partner } from "@/lib/types";

type Form = { origin_port: string; dest_port: string; mode: Mode; partner_id: string; label: string; active: boolean };

function LaneForm({
  initial,
  partners,
  onSave,
  onCancel,
}: {
  initial?: Lane;
  partners: Partner[];
  onSave: (f: Form) => Promise<void>;
  onCancel: () => void;
}) {
  const [f, setF] = useState<Form>({
    origin_port: initial?.origin_port.unlocode ?? "",
    dest_port: initial?.dest_port.unlocode ?? "",
    mode: initial?.mode ?? "sea",
    partner_id: initial?.partner_id ?? "",
    label: initial?.label ?? "",
    active: initial?.active ?? true,
  });
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  return (
    <form
      className="space-y-3"
      onSubmit={async (e) => {
        e.preventDefault();
        if (!f.origin_port || !f.dest_port) {
          setErr("Pick both ports from the suggestions.");
          return;
        }
        setBusy(true);
        setErr(null);
        try {
          await onSave(f);
        } catch (x) {
          setErr(x instanceof ApiError ? x.message : "Save failed");
        } finally {
          setBusy(false);
        }
      }}
    >
      {err && <Alert>{err}</Alert>}
      <Field label="Mode">
        <Select value={f.mode} onChange={(e) => setF({ ...f, mode: e.target.value as Mode, origin_port: "", dest_port: "" })}>
          <option value="sea">Sea</option>
          <option value="air">Air</option>
        </Select>
      </Field>
      <Field label={f.mode === "sea" ? "Origin port" : "Origin airport"}>
        <PortPicker key={`o-${f.mode}`} mode={f.mode} value={f.origin_port} initial={initial?.origin_port}
          onChange={(v) => setF((p) => ({ ...p, origin_port: v }))} />
      </Field>
      <Field label={f.mode === "sea" ? "Destination port" : "Destination airport"}>
        <PortPicker key={`d-${f.mode}`} mode={f.mode} value={f.dest_port} initial={initial?.dest_port}
          onChange={(v) => setF((p) => ({ ...p, dest_port: v }))} />
      </Field>
      <Field label="Default partner (optional)">
        <Select value={f.partner_id} onChange={(e) => setF({ ...f, partner_id: e.target.value })}>
          <option value="">—</option>
          {partners.map((p) => (
            <option key={p.id} value={p.id}>{p.name} ({p.kind})</option>
          ))}
        </Select>
      </Field>
      <Field label="Label (optional)">
        <Input value={f.label} onChange={(e) => setF({ ...f, label: e.target.value })} placeholder="e.g. Home textiles to EU" />
      </Field>
      <label className="flex items-center gap-2 text-sm">
        <input type="checkbox" checked={f.active} onChange={(e) => setF({ ...f, active: e.target.checked })} />
        Active (monitored for disruptions)
      </label>
      <div className="flex justify-end gap-2">
        <Button type="button" variant="secondary" onClick={onCancel}>Cancel</Button>
        <Button type="submit" disabled={busy}>{busy ? "Saving…" : "Save"}</Button>
      </div>
    </form>
  );
}

function Lanes({ company }: { company: Company }) {
  const { org } = useSession();
  const base = `/orgs/${org.organization_id}/lanes`;
  const [data, setData] = useState<Page<Lane> | null>(null);
  const [partners, setPartners] = useState<Partner[]>([]);
  const [editing, setEditing] = useState<Lane | "new" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    api<Page<Lane>>(base, { query: { company_id: company.id, limit: 200 } }).then(setData).catch((e) => setError(e.message));
    api<Page<Partner>>(`/orgs/${org.organization_id}/partners`, { query: { company_id: company.id, limit: 200 } })
      .then((p) => setPartners(p.items))
      .catch(() => undefined);
  }, [base, company.id, org.organization_id]);
  useEffect(load, [load]);

  const toBody = (f: Form) => ({ ...f, partner_id: f.partner_id || null, label: f.label || null });

  return (
    <>
      <PageHeader title="Trade lanes">
        <Button onClick={() => setEditing("new")}>Add lane</Button>
      </PageHeader>
      {error && <Alert>{error}</Alert>}
      <DemoBanner show={!!data?.items.some((l) => l.is_demo)} />
      <div className="mt-3">
        <Table head={["Origin", "Destination", "Mode", "Partner", "Label", "Status", ""]} empty={data?.items.length === 0}>
          {data?.items.map((l) => (
            <tr key={l.id}>
              <td className="px-3 py-2">{portLabel(l.origin_port)}</td>
              <td className="px-3 py-2">{portLabel(l.dest_port)}</td>
              <td className="px-3 py-2 capitalize">{l.mode}</td>
              <td className="px-3 py-2">{l.partner_name ?? "—"}</td>
              <td className="px-3 py-2">{l.label ?? "—"}</td>
              <td className="px-3 py-2">{l.active ? "Active" : "Paused"}</td>
              <td className="space-x-2 whitespace-nowrap px-3 py-2 text-right">
                <Button variant="secondary" onClick={() => setEditing(l)}>Edit</Button>
                <Button
                  variant="danger"
                  onClick={async () => {
                    if (!confirm("Delete this lane?")) return;
                    await api(`${base}/${l.id}`, { method: "DELETE" }).catch((e) => setError(e.message));
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
        <Modal title={editing === "new" ? "Add lane" : "Edit lane"} onClose={() => setEditing(null)}>
          <LaneForm
            initial={editing === "new" ? undefined : editing}
            partners={partners}
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

export default function LanesPage() {
  return <NeedsCompany>{(c) => <Lanes company={c} />}</NeedsCompany>;
}
