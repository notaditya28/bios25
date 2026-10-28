"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import { NeedsCompany } from "@/components/NeedsCompany";
import { Alert, Button, DemoBanner, Field, Input, Modal, PageHeader, Select, Table } from "@/components/ui";
import { api, ApiError } from "@/lib/api";
import { formatDate, formatINR, STATUS_LABEL } from "@/lib/format";
import { useSession } from "@/lib/session";
import type { Company, Lane, Page, Partner, Shipment, ShipmentStatus } from "@/lib/types";

type Form = {
  lane_id: string;
  partner_id: string;
  reference: string;
  container_refs: string;
  container_count: string;
  etd: string;
  eta: string;
  value_inr: string;
  cargo_description: string;
  hs_code: string;
  vessel_name: string;
  vessel_imo: string;
  status: ShipmentStatus;
};

function fromShipment(s?: Shipment): Form {
  return {
    lane_id: s?.lane.id ?? "",
    partner_id: s?.partner_id ?? "",
    reference: s?.reference ?? "",
    container_refs: s?.container_refs.join(" ") ?? "",
    container_count: s?.container_count?.toString() ?? "",
    etd: s?.etd ?? "",
    eta: s?.eta ?? "",
    value_inr: s?.value_inr ?? "",
    cargo_description: s?.cargo_description ?? "",
    hs_code: s?.hs_code ?? "",
    vessel_name: s?.vessel_name ?? "",
    vessel_imo: s?.vessel_imo ?? "",
    status: s?.status ?? "planned",
  };
}

function toBody(f: Form) {
  const n = (v: string) => (v.trim() === "" ? null : v.trim());
  return {
    lane_id: f.lane_id,
    partner_id: n(f.partner_id),
    reference: f.reference.trim(),
    container_refs: f.container_refs.split(/[\s,;]+/).filter(Boolean),
    container_count: f.container_count ? Number(f.container_count) : null,
    etd: n(f.etd),
    eta: n(f.eta),
    value_inr: n(f.value_inr.replace(/,/g, "")),
    cargo_description: n(f.cargo_description),
    hs_code: n(f.hs_code),
    vessel_name: n(f.vessel_name),
    vessel_imo: n(f.vessel_imo),
    status: f.status,
  };
}

function ShipmentForm({
  initial,
  lanes,
  partners,
  onSave,
  onCancel,
}: {
  initial?: Shipment;
  lanes: Lane[];
  partners: Partner[];
  onSave: (f: Form) => Promise<void>;
  onCancel: () => void;
}) {
  const [f, setF] = useState<Form>(fromShipment(initial));
  const [err, setErr] = useState<ApiError | null>(null);
  const [busy, setBusy] = useState(false);
  const set = (k: keyof Form) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setF({ ...f, [k]: e.target.value });
  const fe = err?.fieldErrors ?? {};

  if (lanes.length === 0) {
    return (
      <Alert kind="info">
        Create a <Link className="underline" href="/app/lanes">trade lane</Link> first; every shipment moves on a lane.
      </Alert>
    );
  }

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
        <Field label="Booking / reference" error={fe.reference}>
          <Input required value={f.reference} onChange={set("reference")} />
        </Field>
        <Field label="Status">
          <Select value={f.status} onChange={set("status")}>
            {Object.entries(STATUS_LABEL).map(([v, l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </Select>
        </Field>
      </div>
      <Field label="Lane" error={fe.lane_id}>
        <Select required value={f.lane_id} onChange={set("lane_id")}>
          <option value="">Select lane…</option>
          {lanes.map((l) => (
            <option key={l.id} value={l.id}>
              {l.origin_port.name} → {l.dest_port.name} ({l.mode})
            </option>
          ))}
        </Select>
      </Field>
      <Field label="Partner" hint="Defaults to the lane's partner if left empty.">
        <Select value={f.partner_id} onChange={set("partner_id")}>
          <option value="">—</option>
          {partners.map((p) => (
            <option key={p.id} value={p.id}>{p.name}</option>
          ))}
        </Select>
      </Field>
      <div className="grid grid-cols-2 gap-3">
        <Field label="ETD" error={fe.etd}>
          <Input type="date" value={f.etd} onChange={set("etd")} />
        </Field>
        <Field label="ETA" error={fe.eta ?? fe.form}>
          <Input type="date" value={f.eta} onChange={set("eta")} />
        </Field>
      </div>
      <Field label="Container numbers" hint="ISO 6346, separated by spaces, e.g. MSKU1234565" error={fe.container_refs}>
        <Input value={f.container_refs} onChange={set("container_refs")} />
      </Field>
      <div className="grid grid-cols-2 gap-3">
        <Field label="Container count" error={fe.container_count}>
          <Input type="number" min={0} value={f.container_count} onChange={set("container_count")} />
        </Field>
        <Field label="Cargo value (INR)" error={fe.value_inr}>
          <Input inputMode="decimal" value={f.value_inr} onChange={set("value_inr")} />
        </Field>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Field label="Cargo description">
          <Input value={f.cargo_description} onChange={set("cargo_description")} />
        </Field>
        <Field label="HS code" error={fe.hs_code}>
          <Input value={f.hs_code} onChange={set("hs_code")} />
        </Field>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Field label="Vessel name">
          <Input value={f.vessel_name} onChange={set("vessel_name")} />
        </Field>
        <Field label="Vessel IMO" error={fe.vessel_imo}>
          <Input value={f.vessel_imo} onChange={set("vessel_imo")} placeholder="7 digits" />
        </Field>
      </div>
      <div className="flex justify-end gap-2">
        <Button type="button" variant="secondary" onClick={onCancel}>Cancel</Button>
        <Button type="submit" disabled={busy}>{busy ? "Saving…" : "Save"}</Button>
      </div>
    </form>
  );
}

function Shipments({ company }: { company: Company }) {
  const { org } = useSession();
  const orgBase = `/orgs/${org.organization_id}`;
  const [data, setData] = useState<Page<Shipment> | null>(null);
  const [lanes, setLanes] = useState<Lane[]>([]);
  const [partners, setPartners] = useState<Partner[]>([]);
  const [q, setQ] = useState("");
  const [editing, setEditing] = useState<Shipment | "new" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    const scope = { company_id: company.id, limit: 200 };
    api<Page<Shipment>>(`${orgBase}/shipments`, { query: { ...scope, q } }).then(setData).catch((e) => setError(e.message));
    api<Page<Lane>>(`${orgBase}/lanes`, { query: scope }).then((r) => setLanes(r.items)).catch(() => undefined);
    api<Page<Partner>>(`${orgBase}/partners`, { query: scope }).then((r) => setPartners(r.items)).catch(() => undefined);
  }, [orgBase, company.id, q]);
  useEffect(load, [load]);

  return (
    <>
      <PageHeader title="Shipments">
        <Input placeholder="Search reference or vessel" value={q} onChange={(e) => setQ(e.target.value)} />
        <Button onClick={() => setEditing("new")}>Add shipment</Button>
      </PageHeader>
      <Alert kind="info">
        Risk scores, route analysis and alerts appear here from later milestones. For now this is your
        shipment register.
      </Alert>
      {error && <Alert>{error}</Alert>}
      <div className="mt-3 space-y-3">
        <DemoBanner show={!!data?.items.some((s) => s.is_demo)} />
        <Table head={["Reference", "Lane", "Partner", "ETD", "ETA", "Containers", "Value", "Status", ""]} empty={data?.items.length === 0}>
          {data?.items.map((s) => (
            <tr key={s.id}>
              <td className="px-3 py-2 font-medium">{s.reference}</td>
              <td className="px-3 py-2">
                {s.lane.origin_port.unlocode} → {s.lane.dest_port.unlocode}
                <span className="ml-1 text-xs text-slate-500">{s.lane.mode}</span>
              </td>
              <td className="px-3 py-2">{s.partner_name ?? "—"}</td>
              <td className="px-3 py-2">{formatDate(s.etd)}</td>
              <td className="px-3 py-2">{formatDate(s.eta)}</td>
              <td className="px-3 py-2">{s.container_count ?? "—"}</td>
              <td className="px-3 py-2">{formatINR(s.value_inr)}</td>
              <td className="px-3 py-2">{STATUS_LABEL[s.status]}</td>
              <td className="space-x-2 whitespace-nowrap px-3 py-2 text-right">
                <Button variant="secondary" onClick={() => setEditing(s)}>Edit</Button>
                <Button
                  variant="danger"
                  onClick={async () => {
                    if (!confirm(`Delete shipment ${s.reference}?`)) return;
                    await api(`${orgBase}/shipments/${s.id}`, { method: "DELETE" }).catch((e) => setError(e.message));
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
        <Modal title={editing === "new" ? "Add shipment" : `Edit ${editing.reference}`} onClose={() => setEditing(null)}>
          <ShipmentForm
            initial={editing === "new" ? undefined : editing}
            lanes={lanes}
            partners={partners}
            onCancel={() => setEditing(null)}
            onSave={async (f) => {
              if (editing === "new") await api(`${orgBase}/shipments`, { json: { ...toBody(f), company_id: company.id } });
              else await api(`${orgBase}/shipments/${editing.id}`, { method: "PUT", json: toBody(f) });
              setEditing(null);
              load();
            }}
          />
        </Modal>
      )}
    </>
  );
}

export default function ShipmentsPage() {
  return <NeedsCompany>{(c) => <Shipments company={c} />}</NeedsCompany>;
}
