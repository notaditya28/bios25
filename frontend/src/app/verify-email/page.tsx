"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useEffect, useRef, useState } from "react";

import { AuthCard } from "@/components/AuthCard";
import { Alert } from "@/components/ui";
import { api } from "@/lib/api";

function Verify() {
  const token = useSearchParams().get("token");
  const [state, setState] = useState<{ ok: boolean; msg: string } | null>(null);
  const sent = useRef(false);

  useEffect(() => {
    if (!token || sent.current) return;
    sent.current = true; // tokens are single-use; never submit twice (e.g. StrictMode)
    api<{ message: string }>("/auth/verify-email", { json: { token } })
      .then((r) => setState({ ok: true, msg: r.message }))
      .catch((e) => setState({ ok: false, msg: e.message }));
  }, [token]);

  return (
    <AuthCard title="Email verification">
      {!token && <Alert>Missing verification token.</Alert>}
      {token && !state && <p className="text-sm text-slate-600">Verifying…</p>}
      {state && <Alert kind={state.ok ? "ok" : "error"}>{state.msg}</Alert>}
      <p className="mt-4 text-sm">
        <Link className="text-brand-600 underline" href="/login">Go to sign in</Link>
      </p>
    </AuthCard>
  );
}

export default function VerifyPage() {
  return (
    <Suspense>
      <Verify />
    </Suspense>
  );
}
