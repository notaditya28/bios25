"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useState } from "react";

import { AuthCard } from "@/components/AuthCard";
import { Alert, Button, Field, Input, Select } from "@/components/ui";
import { api } from "@/lib/api";

function RegisterForm() {
  const invite = useSearchParams().get("invite");
  const [form, setForm] = useState({
    email: "",
    password: "",
    full_name: "",
    org_name: "",
    org_type: "exporter",
  });
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const set = (k: keyof typeof form) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm({ ...form, [k]: e.target.value });

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const body = invite
        ? { email: form.email, password: form.password, full_name: form.full_name, invite_token: invite }
        : form;
      const r = await api<{ message: string }>("/auth/register", { json: body });
      setDone(r.message);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Registration failed");
    } finally {
      setBusy(false);
    }
  }

  if (done) {
    return (
      <AuthCard title="Check your inbox">
        <Alert kind="ok">{done}</Alert>
        <p className="mt-4 text-sm">
          <Link className="text-brand-600 underline" href="/login">Go to sign in</Link>
        </p>
      </AuthCard>
    );
  }

  return (
    <AuthCard
      title={invite ? "Accept your invitation" : "Create your TradeRadar account"}
      footer={<>Already registered? <Link className="text-brand-600 underline" href="/login">Sign in</Link></>}
    >
      <form className="space-y-4" onSubmit={submit}>
        {error && <Alert>{error}</Alert>}
        <Field label="Your name">
          <Input required value={form.full_name} onChange={set("full_name")} autoComplete="name" />
        </Field>
        <Field label="Work email" hint={invite ? "Use the address the invitation was sent to." : undefined}>
          <Input type="email" required value={form.email} onChange={set("email")} autoComplete="email" />
        </Field>
        <Field label="Password" hint="At least 10 characters. A short phrase works well.">
          <Input type="password" required minLength={10} value={form.password} onChange={set("password")} autoComplete="new-password" />
        </Field>
        {!invite && (
          <>
            <Field label="Company / organization name">
              <Input required value={form.org_name} onChange={set("org_name")} autoComplete="organization" />
            </Field>
            <Field label="You are a…">
              <Select value={form.org_type} onChange={set("org_type")}>
                <option value="exporter">Exporter</option>
                <option value="importer">Importer</option>
                <option value="forwarder">Freight forwarder / CHA (managing client companies)</option>
              </Select>
            </Field>
          </>
        )}
        <Button type="submit" disabled={busy} className="w-full">
          {busy ? "Creating…" : "Create account"}
        </Button>
      </form>
    </AuthCard>
  );
}

export default function RegisterPage() {
  return (
    <Suspense>
      <RegisterForm />
    </Suspense>
  );
}
