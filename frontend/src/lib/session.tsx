"use client";

import { useRouter } from "next/navigation";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { api, ApiError, setCsrfToken } from "./api";
import type { Company, Me, Membership } from "./types";

interface SessionValue {
  me: Me;
  org: Membership;
  companies: Company[];
  company: Company | null;
  isOwner: boolean;
  isForwarder: boolean;
  selectOrg: (id: string) => void;
  selectCompany: (id: string) => void;
  reloadCompanies: () => Promise<void>;
  logout: () => Promise<void>;
}

const Ctx = createContext<SessionValue | null>(null);

function stored(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function store(key: string, value: string) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    /* private mode: selection just won't persist */
  }
}

export function SessionProvider({ children }: { children: ReactNode }) {
  const router = useRouter();
  const [me, setMe] = useState<Me | null>(null);
  const [orgId, setOrgId] = useState<string | null>(null);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [companyId, setCompanyId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api<Me>("/auth/me")
      .then((m) => {
        setCsrfToken(m.csrf_token);
        setMe(m);
        const saved = stored("tr.org");
        const pick = m.memberships.find((x) => x.organization_id === saved) ?? m.memberships[0];
        setOrgId(pick?.organization_id ?? null);
      })
      .catch((e) => {
        if (e instanceof ApiError && (e.status === 401 || e.status === 403)) {
          router.replace("/login");
        } else {
          setError("Could not reach the server. Retry in a moment.");
        }
      });
  }, [router]);

  const reloadCompanies = useCallback(async () => {
    if (!orgId) return;
    const list = await api<Company[]>(`/orgs/${orgId}/companies`);
    setCompanies(list);
    const saved = stored(`tr.company.${orgId}`);
    setCompanyId((cur) => {
      const keep = list.find((c) => c.id === (cur ?? saved));
      return keep?.id ?? list[0]?.id ?? null;
    });
  }, [orgId]);

  useEffect(() => {
    reloadCompanies().catch(() => setError("Could not load companies."));
  }, [reloadCompanies]);

  const value = useMemo<SessionValue | null>(() => {
    if (!me || !orgId) return null;
    const org = me.memberships.find((m) => m.organization_id === orgId);
    if (!org) return null;
    return {
      me,
      org,
      companies,
      company: companies.find((c) => c.id === companyId) ?? null,
      isOwner: org.role === "owner",
      isForwarder: org.organization_type === "forwarder",
      selectOrg: (id) => {
        store("tr.org", id);
        setCompanies([]);
        setCompanyId(null);
        setOrgId(id);
      },
      selectCompany: (id) => {
        store(`tr.company.${orgId}`, id);
        setCompanyId(id);
      },
      reloadCompanies,
      logout: async () => {
        await api("/auth/logout", { method: "POST" }).catch(() => undefined);
        setCsrfToken(null);
        router.replace("/login");
      },
    };
  }, [me, orgId, companies, companyId, reloadCompanies, router]);

  if (error) return <div className="p-8 text-red-700">{error}</div>;
  if (me && me.memberships.length === 0) {
    return (
      <div className="p-8">You are not a member of any organization yet. Ask an owner to invite you.</div>
    );
  }
  if (!value) return <div className="p-8 text-slate-500">Loading…</div>;
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useSession(): SessionValue {
  const v = useContext(Ctx);
  if (!v) throw new Error("useSession outside SessionProvider");
  return v;
}
