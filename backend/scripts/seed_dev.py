"""DEVELOPMENT ONLY: create a demo forwarder org with sample partners, lanes and shipments.

Refuses to run unless APP_ENV=development AND ALLOW_DEV_SEED=true. Every row is flagged
is_demo=true and the web UI shows a "DEMO DATA" banner wherever such rows appear.
Port reference data must already be loaded (python -m app.cli refresh-ports).

    docker compose run --rm -e ALLOW_DEV_SEED=true api python scripts/seed_dev.py
"""

import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal
from app.models import (
    ClientCompany,
    Lane,
    Membership,
    Organization,
    OrgType,
    Partner,
    PartnerKind,
    Port,
    Role,
    Shipment,
    ShipmentStatus,
    TransportMode,
    User,
)
from app.security import hash_password

DEMO_EMAIL = "demo@traderadar-demo.example.com"
DEMO_PASSWORD = "demo-password-123"  # noqa: S105 - local development only


def main() -> int:
    s = get_settings()
    if s.app_env != "development" or not s.allow_dev_seed:
        print(
            "Refusing to seed: requires APP_ENV=development and ALLOW_DEV_SEED=true",
            file=sys.stderr,
        )
        return 2
    with SessionLocal() as db:
        if db.scalar(select(User).where(User.email == DEMO_EMAIL)):
            print("Demo data already present.")
            return 0
        ports = {
            p.unlocode: p
            for p in db.scalars(
                select(Port).where(Port.unlocode.in_(["INNSA", "INMUN", "NLRTM", "DEHAM", "AEJEA"]))
            )
        }
        if len(ports) < 5:
            print("Load port data first: python -m app.cli refresh-ports", file=sys.stderr)
            return 1

        user = User(
            email=DEMO_EMAIL,
            full_name="Demo Operator",
            password_hash=hash_password(DEMO_PASSWORD),
            email_verified_at=datetime.now(UTC),
        )
        org = Organization(name="Demo Forwarding (DEMO)", type=OrgType.forwarder, is_demo=True)
        db.add_all([user, org])
        db.flush()
        db.add(Membership(user_id=user.id, organization_id=org.id, role=Role.owner))
        company = ClientCompany(
            organization_id=org.id, name="Demo Textiles Pvt Ltd (DEMO)", is_demo=True
        )
        db.add(company)
        db.flush()
        common = {"organization_id": org.id, "company_id": company.id, "is_demo": True}
        buyer = Partner(
            **common,
            kind=PartnerKind.buyer,
            name="Demo Buyer B.V. (DEMO)",
            country_iso2="NL",
            city="Rotterdam",
            port_id=ports["NLRTM"].id,
        )
        supplier = Partner(
            **common,
            kind=PartnerKind.supplier,
            name="Demo Dyes FZE (DEMO)",
            country_iso2="AE",
            city="Dubai",
            port_id=ports["AEJEA"].id,
        )
        db.add_all([buyer, supplier])
        db.flush()
        l1 = Lane(
            **common,
            origin_port_id=ports["INNSA"].id,
            dest_port_id=ports["NLRTM"].id,
            mode=TransportMode.sea,
            partner_id=buyer.id,
            label="JNPT → Rotterdam",
        )
        l2 = Lane(
            **common,
            origin_port_id=ports["INMUN"].id,
            dest_port_id=ports["DEHAM"].id,
            mode=TransportMode.sea,
            label="Mundra → Hamburg",
        )
        db.add_all([l1, l2])
        db.flush()
        today = date.today()
        for i, (lane, days, value) in enumerate(
            [(l1, 3, "3200000"), (l1, 17, "5400000"), (l2, 9, "2100000")], start=1
        ):
            db.add(
                Shipment(
                    **common,
                    lane_id=lane.id,
                    partner_id=lane.partner_id,
                    reference=f"DEMO-{i:03d}",
                    container_count=2,
                    etd=today + timedelta(days=days),
                    eta=today + timedelta(days=days + 28),
                    value_inr=Decimal(value),
                    status=ShipmentStatus.booked,
                )
            )
        db.commit()
    print(f"Seeded demo org. Sign in as {DEMO_EMAIL} / {DEMO_PASSWORD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
