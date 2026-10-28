import uuid
from typing import Any

from sqlalchemy.orm import Session

from app.models import AuditLog


def audit(
    db: Session,
    *,
    actor: uuid.UUID | None,
    org: uuid.UUID | None,
    action: str,
    target_type: str,
    target_id: Any = None,
    before: dict | None = None,
    after: dict | None = None,
    note: str | None = None,
) -> None:
    db.add(
        AuditLog(
            actor_user_id=actor,
            organization_id=org,
            action=action,
            target_type=target_type,
            target_id=str(target_id) if target_id is not None else None,
            before=before,
            after=after,
            note=note,
        )
    )
