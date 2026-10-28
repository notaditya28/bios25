"use client";

import Link from "next/link";
import type { ReactNode } from "react";

import { useSession } from "@/lib/session";
import type { Company } from "@/lib/types";

/** Forwarders must pick (or create) a client company before managing trade data. */
export function NeedsCompany({ children }: { children: (company: Company) => ReactNode }) {
  const s = useSession();
  if (!s.company) {
    return (
      <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm">
        {s.isForwarder ? (
          <>
            Add your first client company to start tracking its partners and shipments.{" "}
            <Link className="text-brand-600 underline" href="/app/companies">
              Add client company
            </Link>
          </>
        ) : (
          "Loading company…"
        )}
      </div>
    );
  }
  return <>{children(s.company)}</>;
}
