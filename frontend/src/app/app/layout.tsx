"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { SessionProvider, useSession } from "@/lib/session";

function Shell({ children }: { children: ReactNode }) {
  const s = useSession();
  const path = usePathname();
  const nav = [
    { href: "/app/shipments", label: "Shipments" },
    { href: "/app/lanes", label: "Lanes" },
    { href: "/app/partners", label: "Partners" },
    { href: "/app/screening", label: "Sanctions screening" },
    { href: "/app/import", label: "CSV import" },
    ...(s.isForwarder ? [{ href: "/app/companies", label: "Client companies" }] : []),
    { href: "/app/team", label: "Team" },
    ...(s.me.is_platform_admin ? [{ href: "/app/admin/sources", label: "Admin · Data sources" }] : []),
  ];

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center gap-4 px-4 py-3">
          <Link href="/app/shipments" className="text-lg font-bold text-brand-900">
            TradeRadar
          </Link>
          {s.me.memberships.length > 1 ? (
            <select
              aria-label="Organization"
              className="rounded border border-slate-300 px-2 py-1 text-sm"
              value={s.org.organization_id}
              onChange={(e) => s.selectOrg(e.target.value)}
            >
              {s.me.memberships.map((m) => (
                <option key={m.organization_id} value={m.organization_id}>
                  {m.organization_name}
                </option>
              ))}
            </select>
          ) : (
            <span className="text-sm text-slate-600">{s.org.organization_name}</span>
          )}
          {s.isForwarder && (
            <select
              aria-label="Client company"
              className="rounded border border-slate-300 px-2 py-1 text-sm"
              value={s.company?.id ?? ""}
              onChange={(e) => s.selectCompany(e.target.value)}
            >
              {s.companies.length === 0 && <option value="">No client companies yet</option>}
              {s.companies.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
          )}
          <div className="ml-auto flex items-center gap-3 text-sm">
            <span className="text-slate-600">{s.me.full_name}</span>
            <button className="text-brand-600 underline" onClick={s.logout}>
              Sign out
            </button>
          </div>
        </div>
        <nav className="mx-auto flex max-w-7xl gap-1 overflow-x-auto px-4">
          {nav.map((n) => (
            <Link
              key={n.href}
              href={n.href}
              className={`whitespace-nowrap border-b-2 px-3 py-2 text-sm ${
                path.startsWith(n.href)
                  ? "border-brand-600 font-medium text-brand-900"
                  : "border-transparent text-slate-600 hover:text-slate-900"
              }`}
            >
              {n.label}
            </Link>
          ))}
        </nav>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6">{children}</main>
      <footer className="mx-auto max-w-7xl px-4 pb-8 text-xs text-slate-500">
        Port reference data: UN/LOCODE (UNECE), World Port Index (US NGA), Made with Natural Earth.
        Sanctions lists: US Treasury OFAC; UN Security Council; European Commission; UK FCDO (contains
        public sector information licensed under the Open Government Licence v3.0).
      </footer>
    </div>
  );
}

export default function AppLayout({ children }: { children: ReactNode }) {
  return (
    <SessionProvider>
      <Shell>{children}</Shell>
    </SessionProvider>
  );
}
