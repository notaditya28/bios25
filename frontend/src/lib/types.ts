export type OrgType = "exporter" | "importer" | "forwarder";
export type Role = "owner" | "staff";
export type Mode = "sea" | "air";
export type ShipmentStatus = "planned" | "booked" | "in_transit" | "delivered" | "cancelled";

export interface Membership {
  organization_id: string;
  organization_name: string;
  organization_type: OrgType;
  role: Role;
}

export interface Me {
  id: string;
  email: string;
  full_name: string;
  email_verified: boolean;
  is_platform_admin: boolean;
  csrf_token: string;
  memberships: Membership[];
}

export interface Company {
  id: string;
  name: string;
  is_self: boolean;
  iec_code: string | null;
  gstin: string | null;
  contact_email: string | null;
  is_demo: boolean;
}

export interface Port {
  unlocode: string;
  name: string;
  country_iso2: string;
  subdivision: string | null;
  is_seaport: boolean;
  is_airport: boolean;
  lat: number | null;
  lon: number | null;
  coord_source: "manual" | "wpi" | "unlocode" | "natural_earth" | null;
  harbor_size: string | null;
}

export interface Partner {
  id: string;
  company_id: string;
  kind: "buyer" | "supplier";
  name: string;
  country: string;
  city: string | null;
  port: Port | null;
  address: string | null;
  contact_email: string | null;
  notes: string | null;
  is_demo: boolean;
}

export interface Lane {
  id: string;
  company_id: string;
  origin_port: Port;
  dest_port: Port;
  mode: Mode;
  partner_id: string | null;
  partner_name: string | null;
  label: string | null;
  active: boolean;
  is_demo: boolean;
}

export interface Shipment {
  id: string;
  company_id: string;
  lane: Lane;
  partner_id: string | null;
  partner_name: string | null;
  reference: string;
  container_refs: string[];
  container_count: number | null;
  etd: string | null;
  eta: string | null;
  value_inr: string | null;
  cargo_description: string | null;
  hs_code: string | null;
  vessel_name: string | null;
  vessel_imo: string | null;
  status: ShipmentStatus;
  is_demo: boolean;
}

export interface Page<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
}

export interface Member {
  user_id: string;
  email: string;
  full_name: string;
  role: Role;
  joined_at: string;
}

export interface Invite {
  id: string;
  email: string;
  role: Role;
  expires_at: string;
}

export interface ImportResult {
  id: string | null;
  kind: string;
  dry_run: boolean;
  row_count: number;
  created_count: number;
  errors: { row: number; field: string | null; message: string }[];
  status: "validated" | "rejected" | "committed";
}

export interface SourceStatus {
  code: string;
  name: string;
  enabled: boolean;
  attribution: string | null;
  last_success_at: string | null;
  last_data_as_of: string | null;
  stale: boolean;
  stale_after_hours: number;
}

export interface SourceAdminStatus extends SourceStatus {
  url: string;
  licence_note: string;
  last_error: string | null;
  recent_runs: {
    started_at: string;
    finished_at: string | null;
    status: string;
    rows: number | null;
    error: string | null;
  }[];
}
