"use client";

import { useCallback, useEffect, useState } from "react";

import { Alert, Button, Field, Input, PageHeader, Select, Table } from "@/components/ui";
import { api } from "@/lib/api";
import { formatDate } from "@/lib/format";
import { useSession } from "@/lib/session";
import type { Invite, Member, Role } from "@/lib/types";

export default function TeamPage() {
  const s = useSession();
  const base = `/orgs/${s.org.organization_id}`;
  const [members, setMembers] = useState<Member[]>([]);
  const [invites, setInvites] = useState<Invite[]>([]);
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("staff");
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  const load = useCallback(() => {
    api<Member[]>(`${base}/members`).then(setMembers).catch(() => undefined);
    if (s.isOwner) api<Invite[]>(`${base}/invites`).then(setInvites).catch(() => undefined);
  }, [base, s.isOwner]);
  useEffect(load, [load]);

  const act = async (fn: () => Promise<unknown>, ok: string) => {
    try {
      await fn();
      setMsg({ ok: true, text: ok });
      load();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : "Failed" });
    }
  };

  return (
    <>
      <PageHeader title="Team" />
      {msg && <Alert kind={msg.ok ? "ok" : "error"}>{msg.text}</Alert>}
      <div className="mt-3">
        <Table head={["Name", "Email", "Role", "Joined", ""]}>
          {members.map((m) => (
            <tr key={m.user_id}>
              <td className="px-3 py-2">{m.full_name}</td>
              <td className="px-3 py-2">{m.email}</td>
              <td className="px-3 py-2">
                {s.isOwner && m.user_id !== s.me.id ? (
                  <select
                    className="rounded border border-slate-300 px-1 py-0.5"
                    value={m.role}
                    onChange={(e) =>
                      act(() => api(`${base}/members/${m.user_id}`, { method: "PATCH", json: { role: e.target.value } }), "Role updated")
                    }
                  >
                    <option value="owner">Owner</option>
                    <option value="staff">Staff</option>
                  </select>
                ) : (
                  <span className="capitalize">{m.role}</span>
                )}
              </td>
              <td className="px-3 py-2">{formatDate(m.joined_at)}</td>
              <td className="px-3 py-2 text-right">
                {(s.isOwner || m.user_id === s.me.id) && (
                  <Button
                    variant="danger"
                    onClick={() => {
                      if (confirm(m.user_id === s.me.id ? "Leave this organization?" : `Remove ${m.full_name}?`))
                        act(() => api(`${base}/members/${m.user_id}`, { method: "DELETE" }), "Member removed");
                    }}
                  >
                    {m.user_id === s.me.id ? "Leave" : "Remove"}
                  </Button>
                )}
              </td>
            </tr>
          ))}
        </Table>
      </div>

      {s.isOwner && (
        <section className="mt-8 max-w-2xl space-y-3">
          <h2 className="font-semibold">Invite a colleague</h2>
          <form
            className="flex flex-wrap items-end gap-3"
            onSubmit={(e) => {
              e.preventDefault();
              act(() => api(`${base}/invites`, { json: { email, role } }), `Invitation sent to ${email}`).then(() => setEmail(""));
            }}
          >
            <div className="min-w-64 flex-1">
              <Field label="Email">
                <Input type="email" required value={email} onChange={(e) => setEmail(e.target.value)} />
              </Field>
            </div>
            <Field label="Role">
              <Select value={role} onChange={(e) => setRole(e.target.value as Role)}>
                <option value="staff">Staff</option>
                <option value="owner">Owner</option>
              </Select>
            </Field>
            <Button type="submit">Send invite</Button>
          </form>
          {invites.length > 0 && (
            <Table head={["Pending invitation", "Role", "Expires", ""]}>
              {invites.map((i) => (
                <tr key={i.id}>
                  <td className="px-3 py-2">{i.email}</td>
                  <td className="px-3 py-2 capitalize">{i.role}</td>
                  <td className="px-3 py-2">{formatDate(i.expires_at)}</td>
                  <td className="px-3 py-2 text-right">
                    <Button variant="secondary" onClick={() => act(() => api(`${base}/invites/${i.id}`, { method: "DELETE" }), "Invitation revoked")}>
                      Revoke
                    </Button>
                  </td>
                </tr>
              ))}
            </Table>
          )}
        </section>
      )}
    </>
  );
}
