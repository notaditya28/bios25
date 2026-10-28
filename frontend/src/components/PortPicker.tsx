"use client";

import { useEffect, useId, useState } from "react";

import { api } from "@/lib/api";
import { portLabel } from "@/lib/format";
import type { Mode, Port } from "@/lib/types";

/** Searchable UN/LOCODE picker. Value is the 5-letter code. */
export function PortPicker({
  value,
  onChange,
  mode,
  initial,
}: {
  value: string;
  onChange: (code: string) => void;
  mode?: Mode;
  initial?: Port | null;
}) {
  const [query, setQuery] = useState(initial ? portLabel(initial) : value);
  const [results, setResults] = useState<Port[]>([]);
  const [open, setOpen] = useState(false);
  const [selected, setSelected] = useState<Port | null>(initial ?? null);
  const listId = useId();

  useEffect(() => {
    if (!open || query.trim().length < 2) {
      setResults([]);
      return;
    }
    const t = setTimeout(() => {
      api<Port[]>("/ports", { query: { q: query.trim(), mode } })
        .then(setResults)
        .catch(() => setResults([]));
    }, 200);
    return () => clearTimeout(t);
  }, [query, mode, open]);

  return (
    <div className="relative">
      <input
        role="combobox"
        aria-expanded={open}
        aria-controls={listId}
        className="w-full rounded-md border border-slate-300 px-2.5 py-1.5 text-sm focus:border-brand-600 focus:outline-none focus:ring-1 focus:ring-brand-600"
        placeholder="Search port name or UN/LOCODE, e.g. Nhava Sheva / INNSA"
        value={query}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        onChange={(e) => {
          setQuery(e.target.value);
          setSelected(null);
          onChange("");
        }}
      />
      {open && results.length > 0 && (
        <ul id={listId} role="listbox" className="absolute z-20 mt-1 max-h-64 w-full overflow-y-auto rounded-md border border-slate-200 bg-white shadow-lg">
          {results.map((p) => (
            <li
              key={p.unlocode}
              role="option"
              aria-selected={p.unlocode === value}
              className="cursor-pointer px-3 py-1.5 text-sm hover:bg-brand-50"
              onMouseDown={() => {
                setSelected(p);
                setQuery(portLabel(p));
                onChange(p.unlocode);
                setOpen(false);
              }}
            >
              <span className="font-medium">{p.name}</span>{" "}
              <span className="text-slate-500">
                {p.unlocode} · {p.country_iso2}
                {p.is_airport && !p.is_seaport ? " · airport" : ""}
              </span>
            </li>
          ))}
        </ul>
      )}
      {selected && selected.lat === null && (
        <p className="mt-1 text-xs text-amber-700">
          No coordinates for this port in our reference sources yet. You can still save it; route
          analysis for it will start once an admin sets its location.
        </p>
      )}
    </div>
  );
}
