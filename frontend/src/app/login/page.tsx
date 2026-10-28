"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";

import { AuthCard } from "@/components/AuthCard";
import { Alert, Button, Field, Input } from "@/components/ui";
import { api, ApiError } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [unverified, setUnverified] = useState(false);
  const [info, setInfo] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setUnverified(false);
    try {
      await api("/auth/login", { json: { email, password } });
      router.replace("/app/shipments");
    } catch (err) {
      if (err instanceof ApiError && err.message === "email_unverified") {
        setUnverified(true);
      } else {
        setError(err instanceof Error ? err.message : "Sign-in failed");
      }
    } finally {
      setBusy(false);
    }
  }

  async function resend() {
    await api("/auth/resend-verification", { json: { email } }).catch(() => undefined);
    setInfo("If that address needs verifying, a new link is on its way.");
  }

  return (
    <AuthCard
      title="Sign in"
      footer={
        <>
          New here? <Link className="text-brand-600 underline" href="/register">Create an account</Link>
        </>
      }
    >
      <form className="space-y-4" onSubmit={submit}>
        {error && <Alert>{error}</Alert>}
        {unverified && (
          <Alert kind="info">
            Please verify your email first.{" "}
            <button type="button" className="underline" onClick={resend}>
              Resend the link
            </button>
          </Alert>
        )}
        {info && <Alert kind="ok">{info}</Alert>}
        <Field label="Email">
          <Input type="email" autoComplete="email" required value={email} onChange={(e) => setEmail(e.target.value)} />
        </Field>
        <Field label="Password">
          <Input type="password" autoComplete="current-password" required value={password} onChange={(e) => setPassword(e.target.value)} />
        </Field>
        <div className="flex items-center justify-between">
          <Link className="text-sm text-brand-600 underline" href="/forgot-password">
            Forgot password?
          </Link>
          <Button type="submit" disabled={busy}>
            {busy ? "Signing in…" : "Sign in"}
          </Button>
        </div>
      </form>
    </AuthCard>
  );
}
