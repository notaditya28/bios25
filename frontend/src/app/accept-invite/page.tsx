"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useEffect, useState } from "react";

import { AuthCard } from "@/components/AuthCard";
import { Alert, Button } from "@/components/ui";
import { api, ApiError, setCsrfToken } from "@/lib/api";
import type { Me } from "@/lib/types";

function Accept() {
  const token = useSearchParams().get("token") ?? "";
  const router = useRouter();
  const [me, setMe] = useState<Me | null | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api<Me>("/auth/me")
      .then((m) => {
        setCsrfToken(m.csrf_token);
        setMe(m);
      })
      .catch(() => setMe(null));
  }, []);

  async function accept() {
    try {
      await api("/invites/accept", { json: { token } });
      router.replace("/app/shipments");
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not accept the invitation");
    }
  }

  return (
    <AuthCard title="Join an organization">
      {error && <Alert>{error}</Alert>}
      {me === undefined && <p className="text-sm text-slate-600">Checking your session…</p>}
      {me === null && (
        <div className="space-y-3 text-sm">
          <p>New to TradeRadar? Create your account with the invited email address:</p>
          <Link className="text-brand-600 underline" href={`/register?invite=${encodeURIComponent(token)}`}>
            Create account and join
          </Link>
          <p>Already have an account? <Link className="text-brand-600 underline" href="/login">Sign in</Link>, then open the invitation link again.</p>
        </div>
      )}
      {me && (
        <div className="space-y-3 text-sm">
          <p>Signed in as <strong>{me.email}</strong>.</p>
          <Button onClick={accept}>Accept invitation</Button>
        </div>
      )}
    </AuthCard>
  );
}

export default function AcceptInvitePage() {
  return (
    <Suspense>
      <Accept />
    </Suspense>
  );
}
