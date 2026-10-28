"use client";

import Link from "next/link";
import { useState } from "react";

import { AuthCard } from "@/components/AuthCard";
import { Alert, Button, Field, Input } from "@/components/ui";
import { api } from "@/lib/api";

export default function ForgotPage() {
  const [email, setEmail] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    try {
      const r = await api<{ message: string }>("/auth/forgot-password", { json: { email } });
      setMsg(r.message);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Request failed");
    }
  }

  return (
    <AuthCard title="Reset your password" footer={<Link className="text-brand-600 underline" href="/login">Back to sign in</Link>}>
      {msg ? (
        <Alert kind="ok">{msg}</Alert>
      ) : (
        <form className="space-y-4" onSubmit={submit}>
          {error && <Alert>{error}</Alert>}
          <Field label="Email">
            <Input type="email" required value={email} onChange={(e) => setEmail(e.target.value)} />
          </Field>
          <Button type="submit">Send reset link</Button>
        </form>
      )}
    </AuthCard>
  );
}
