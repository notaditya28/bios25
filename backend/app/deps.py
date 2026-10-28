import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import ClientCompany, Membership, Organization, Role, User, UserSession
from app.security import hash_token, tokens_equal

SESSION_COOKIE = "tr_session"
CSRF_HEADER = "X-CSRF-Token"
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


@dataclass
class AuthContext:
    user: User
    session: UserSession


def get_auth(request: Request, db: Session = Depends(get_db)) -> AuthContext:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not signed in")
    sess = db.scalar(select(UserSession).where(UserSession.token_hash == hash_token(token)))
    now = datetime.now(UTC)
    if sess is None or sess.revoked_at is not None or sess.expires_at <= now:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Session expired")
    if not sess.user.is_active:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Account disabled")
    if request.method not in SAFE_METHODS:
        sent = request.headers.get(CSRF_HEADER, "")
        if not sent or not tokens_equal(sent, sess.csrf_token):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "CSRF token missing or invalid")
    return AuthContext(user=sess.user, session=sess)


def get_verified_user(auth: AuthContext = Depends(get_auth)) -> User:
    if auth.user.email_verified_at is None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Verify your email address first")
    return auth.user


@dataclass
class OrgContext:
    user: User
    org: Organization
    role: Role

    def require_owner(self) -> None:
        if self.role != Role.owner:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Owner role required")


def get_org(
    org_id: uuid.UUID, user: User = Depends(get_verified_user), db: Session = Depends(get_db)
) -> OrgContext:
    """Every tenant-scoped route takes org_id in the path; membership is checked here."""
    m = db.scalar(
        select(Membership).where(
            Membership.organization_id == org_id, Membership.user_id == user.id
        )
    )
    if m is None:
        # 404 rather than 403 so org ids cannot be probed.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Organization not found")
    return OrgContext(user=user, org=m.organization, role=m.role)


def get_owner_org(ctx: OrgContext = Depends(get_org)) -> OrgContext:
    ctx.require_owner()
    return ctx


def company_in_org(db: Session, ctx: OrgContext, company_id: uuid.UUID) -> ClientCompany:
    company = db.get(ClientCompany, company_id)
    if company is None or company.organization_id != ctx.org.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Company not found")
    return company


def require_platform_admin(user: User = Depends(get_verified_user)) -> User:
    if not user.is_platform_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Platform admin only")
    return user
