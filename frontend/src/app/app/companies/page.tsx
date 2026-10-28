"use client";

import { useState } from "react";

import { Alert, Button, DemoBanner, Field, Input, Modal, PageHeader, Table } from "@/components/ui";
import { api, ApiError } from "@/lib/api";
import { useSession } from "@/lib/session";
import type { Company } from "@/lib/types";

type Form = { name: string; iec_code: string; gstin: string; contact_email: string };

export default function CompaniesPage() {
  const s = useSession();
  const base = `/orgs/${s.org.organization_id}/companies`;
  const [editing, setEditing] = useState<Company | "new" | null>(null);
  const [f, setF] = useState<Form>({ name: "", iec_code: "", gstin: "", contact_email: "" });
  const [err, setErr] = useState<ApiError | null>(null);

  if (!s.isForwarder) {
    return <Alert kind="info">Client companies are for freight forwarder / CHA organizations.</Alert>;
  }

  function open(c: Company | "new") {
    setErr(null);
    setF(
      c === "new"
        ? { name: "", iec_code: "", gstin: "", contact_email: "" }
        : { name: c.name, iec_code: c.iec_code ?? "", gstin: c.gstin ?? "", contact_email: c.contact_email ?? "" },
    );
    setEditing(c);
  }

  async function save(e: React.FormEvent) {
    e.preventDefault();
    const body = { name: f.name, iec_code: f.iec_code || null, gstin: f.gstin || null, contact_email: f.contact_email || null };
    try {
      if (editing === "new") {
        const c = await api<Company>(base, { json: body });
        await s.reloadCompanies();
        s.selectCompany(c.id);
      } else if (editing) {
        await api(`${base}/${editing.id}`, { method: "PATCH", json: body });
        await s.reloadCompanies();
      }
      setEditing(null);
    } catch (x) {
      setErr(x instanceof ApiError ? x : new ApiError(0, "Save failed"));
    }
  }

  return (
    <>
      <PageHeader title="Client companies">
        <Button onClick={() => open("new")}>Add client company</Button>
      </PageHeader>
      <DemoBanner show={s.companies.some((c) => c.is_demo)} />
      <div className="mt-3">
        <Table head={["Name", "IEC", "GSTIN", "Contact", ""]} empty={s.companies.length === 0}>
          {s.companies.map((c) => (
            <tr key={c.id}>
              <td className="px-3 py-2 font-medium">{c.name}</td>
              <td className="px-3 py-2">{c.iec_code ?? "—"}</td>
              <td className="px-3 py-2">{c.gstin ?? "—"}</td>
              <td className="px-3 py-2">{c.contact_email ?? "—"}</td>
              <td className="space-x-2 whitespace-nowrap px-3 py-2 text-right">
                <Button variant="secondary" onClick={() => open(c)}>Edit</Button>
                {s.isOwner && (
                  <Button
                    variant="danger"
                    onClick={async () => {
                      if (!confirm(`Delete ${c.name} and ALL its partners, lanes and shipments?`)) return;
                      await api(`${base}/${c.id}`, { method: "DELETE" });
                      await s.reloadCompanies();
                    }}
                  >
                    Delete
                  </Button>
                )}
              </td>
            </tr>
          ))}
        </Table>
      </div>
      {editing && (
        <Modal title={editing === "new" ? "Add client company" : "Edit client company"} onClose={() => setEditing(null)}>
          <form className="space-y-3" onSubmit={save}>
            {err && <Alert>{err.message}</Alert>}
            <Field label="Legal name" error={err?.fieldErrors.name}>
              <Input required value={f.name} onChange={(e) => setF({ ...f, name: e.target.value })} />
            </Field>
            <div className="grid grid-cols-2 gap-3">
              <Field label="IEC (optional)" error={err?.fieldErrors.iec_code}>
                <Input value={f.iec_code} onChange={(e) => setF({ ...f, iec_code: e.target.value })} />
              </Field>
              <Field label="GSTIN (optional)" error={err?.fieldErrors.gstin}>
                <Input value={f.gstin} onChange={(e) => setF({ ...f, gstin: e.target.value })} />
              </Field>
            </div>
            <Field label="Contact email (optional)" error={err?.fieldErrors.contact_email}>
              <Input type="email" value={f.contact_email} onChange={(e) => setF({ ...f, contact_email: e.target.value })} />
            </Field>
            <div className="flex justify-end gap-2">
              <Button type="button" variant="secondary" onClick={() => setEditing(null)}>Cancel</Button>
              <Button type="submit">Save</Button>
            </div>
          </form>
        </Modal>
      )}
    </>
  );
}
