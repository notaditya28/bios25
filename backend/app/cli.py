"""Operator commands, e.g.  docker compose run --rm api python -m app.cli make-admin you@x.com"""

import argparse
import sys

from sqlalchemy import select

from app.db import SessionLocal
from app.models import User


def make_admin(email: str) -> int:
    with SessionLocal() as db:
        user = db.scalar(select(User).where(User.email == email))
        if user is None:
            print(f"No user with email {email}; sign up first.", file=sys.stderr)
            return 1
        user.is_platform_admin = True
        db.commit()
    print(f"{email} is now a platform admin.")
    return 0


def refresh_ports() -> int:
    from app.tasks.ports import refresh_ports_now

    print(refresh_ports_now())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("make-admin", help="grant platform-admin rights to an existing user")
    p.add_argument("email")
    sub.add_parser("refresh-ports", help="download and load port reference data now")
    args = parser.parse_args()
    if args.cmd == "make-admin":
        return make_admin(args.email)
    return refresh_ports()


if __name__ == "__main__":
    raise SystemExit(main())
