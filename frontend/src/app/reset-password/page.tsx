"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useState } from "react";

import { AuthCard } from "@/components/AuthCard";
import { Alert, Button, Field, Input } from "@/components/ui";
import { api } from "@/lib/api";

function Reset() {
  const token = useSearchParams().get("token") ?? "";
  const [password, setPassword] = useState("");
  const [done, setDone] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      const r = await api<{ message: string }>("/auth/reset-password", { json: { token, password } });
      setDone(r.message);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Reset failed");
    }
  }

  return (
    <AuthCard title="Choose a new password">
      {done ? (
        <>
          <Alert kind="ok">{done}</Alert>
          <p className="mt-4 text-sm"><Link className="text-brand-600 underline" href="/login">Sign in</Link></p>
        </>
      ) : (
        <form className="space-y-4" onSubmit={submit}>
          {error && <Alert>{error}</Alert>}
          <Field label="New password" hint="At least 10 characters. All other sessions will be signed out.">
            <Input type="password" required minLength={10} autoComplete="new-password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </Field>
          <Button type="submit" disabled={!token}>Set password</Button>
        </form>
      )}
    </AuthCard>
  );
}

export default function ResetPage() {
  return (
    <Suspense>
      <Reset />
    </Suspense>
  );
}
