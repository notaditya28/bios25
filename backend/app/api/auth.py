from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.deps import SESSION_COOKIE, AuthContext, get_auth
from app.models import (
    AuthToken,
    ClientCompany,
    Membership,
    Organization,
    OrgType,
    Role,
    TokenType,
    User,
    UserSession,
)
from app.schemas.auth import (
    ChangePasswordIn,
    EmailIn,
    LoginIn,
    MembershipOut,
    MeOut,
    MessageOut,
    RegisterIn,
    ResetIn,
    TokenIn,
)
from app.security import (
    hash_password,
    hash_token,
    needs_rehash,
    new_token,
    password_problems,
    verify_password,
)
from app.services import ratelimit
from app.services.audit import audit
from app.services.email import reset_email, verification_email
from app.services.tokens import consume_token, issue_token
from app.tasks.email import enqueue

router = APIRouter(prefix="/api/auth", tags=["auth"])

GENERIC_EMAIL_MSG = "If that address can receive email, a message is on its way."


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _limit(key: str, limit: int, window_s: int) -> None:
    if not ratelimit.hit(key, limit, window_s):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many attempts; try later")


def _send_verification(db: Session, user: User) -> None:
    s = get_settings()
    raw, _ = issue_token(
        db, TokenType.verify_email, timedelta(hours=s.verify_token_ttl_hours), user_id=user.id
    )
    db.commit()
    enqueue(verification_email(user.email, f"{s.app_base_url}/verify-email?token={raw}"))


def create_org_with_owner(db: Session, user: User, name: str, type_: OrgType) -> Organization:
    org = Organization(name=name, type=type_)
    db.add(org)
    db.flush()
    db.add(Membership(user_id=user.id, organization_id=org.id, role=Role.owner))
    if type_ != OrgType.forwarder:
        db.add(ClientCompany(organization_id=org.id, name=name, is_self=True))
    return org


def _start_session(db: Session, user: User, request: Request, response: Response) -> UserSession:
    s = get_settings()
    raw = new_token()
    sess = UserSession(
        user_id=user.id,
        token_hash=hash_token(raw),
        csrf_token=new_token(),
        expires_at=datetime.now(UTC) + timedelta(hours=s.session_ttl_hours),
        ip=_client_ip(request),
        user_agent=(request.headers.get("user-agent") or "")[:400],
    )
    db.add(sess)
    user.last_login_at = datetime.now(UTC)
    db.commit()
    response.set_cookie(
        SESSION_COOKIE,
        raw,
        max_age=s.session_ttl_hours * 3600,
        httponly=True,
        secure=s.cookie_secure,
        samesite="lax",
        path="/",
    )
    return sess


def _me(db: Session, user: User, sess: UserSession) -> MeOut:
    rows = db.execute(
        select(Membership, Organization)
        .join(Organization, Organization.id == Membership.organization_id)
        .where(Membership.user_id == user.id)
        .order_by(Organization.name)
    ).all()
    return MeOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        email_verified=user.email_verified_at is not None,
        is_platform_admin=user.is_platform_admin,
        csrf_token=sess.csrf_token,
        memberships=[
            MembershipOut(
                organization_id=o.id,
                organization_name=o.name,
                organization_type=o.type,
                role=m.role,
            )
            for m, o in rows
        ],
    )


@router.post("/register", status_code=202, response_model=MessageOut)
def register(body: RegisterIn, request: Request, db: Session = Depends(get_db)) -> MessageOut:
    _limit(f"register:{_client_ip(request)}", 10, 3600)
    problems = password_problems(body.password, body.email)
    if problems:
        raise HTTPException(422, "Password " + "; ".join(problems))

    invite: AuthToken | None = None
    if body.invite_token:
        invite = consume_token(db, body.invite_token, TokenType.invite)
        if (invite.email or "").lower() != body.email.lower():
            raise HTTPException(400, "This invitation was sent to a different email address")

    # Hash before the existence check so both branches take similar time.
    pw_hash = hash_password(body.password)
    existing = db.scalar(select(User).where(User.email == body.email))
    if existing is not None:
        # Do not reveal that the account exists; point the owner at password reset instead.
        db.rollback()
        s = get_settings()
        raw, _ = issue_token(
            db,
            TokenType.reset_password,
            timedelta(minutes=s.reset_token_ttl_minutes),
            user_id=existing.id,
        )
        db.commit()
        enqueue(reset_email(existing.email, f"{s.app_base_url}/reset-password?token={raw}"))
        return MessageOut(message=GENERIC_EMAIL_MSG)

    user = User(email=body.email, full_name=body.full_name, password_hash=pw_hash)
    db.add(user)
    db.flush()
    if invite is not None:
        assert invite.organization_id and invite.role
        db.add(
            Membership(user_id=user.id, organization_id=invite.organization_id, role=invite.role)
        )
        # The invitation link proved control of the mailbox.
        user.email_verified_at = datetime.now(UTC)
        audit(
            db,
            actor=user.id,
            org=invite.organization_id,
            action="member.joined",
            target_type="user",
            target_id=user.id,
            after={"role": invite.role.value},
        )
        db.commit()
        return MessageOut(message="Account created. You can sign in now.")

    assert body.org_name and body.org_type
    org = create_org_with_owner(db, user, body.org_name, body.org_type)
    audit(
        db,
        actor=user.id,
        org=org.id,
        action="org.created",
        target_type="organization",
        target_id=org.id,
        after={"name": org.name, "type": org.type.value},
    )
    _send_verification(db, user)
    return MessageOut(message=GENERIC_EMAIL_MSG)


@router.post("/verify-email", response_model=MessageOut)
def verify_email(body: TokenIn, db: Session = Depends(get_db)) -> MessageOut:
    tok = consume_token(db, body.token, TokenType.verify_email)
    user = db.get(User, tok.user_id)
    if user is None:
        raise HTTPException(400, "This link is invalid or has expired")
    if user.email_verified_at is None:
        user.email_verified_at = datetime.now(UTC)
    db.commit()
    return MessageOut(message="Email verified. You can sign in now.")


@router.post("/resend-verification", status_code=202, response_model=MessageOut)
def resend_verification(
    body: EmailIn, request: Request, db: Session = Depends(get_db)
) -> MessageOut:
    _limit(f"resend:{_client_ip(request)}", 5, 3600)
    _limit(f"resend:{body.email.lower()}", 3, 3600)
    user = db.scalar(select(User).where(User.email == body.email))
    if user is not None and user.email_verified_at is None:
        _send_verification(db, user)
    return MessageOut(message=GENERIC_EMAIL_MSG)


@router.post("/login", response_model=MeOut)
def login(
    body: LoginIn, request: Request, response: Response, db: Session = Depends(get_db)
) -> MeOut:
    s = get_settings()
    _limit(f"login:ip:{_client_ip(request)}", s.login_rate_limit_per_15min * 5, 900)
    _limit(f"login:email:{body.email.lower()}", s.login_rate_limit_per_15min, 900)
    user = db.scalar(select(User).where(User.email == body.email))
    if not verify_password(user.password_hash if user else None, body.password) or user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Incorrect email or password")
    if not user.is_active:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Account disabled")
    if user.email_verified_at is None:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "email_unverified")
    if user.password_hash and needs_rehash(user.password_hash):
        user.password_hash = hash_password(body.password)
    ratelimit.reset(f"login:email:{body.email.lower()}")
    sess = _start_session(db, user, request, response)
    return _me(db, user, sess)


@router.post("/logout", response_model=MessageOut)
def logout(
    response: Response, auth: AuthContext = Depends(get_auth), db: Session = Depends(get_db)
) -> MessageOut:
    auth.session.revoked_at = datetime.now(UTC)
    db.commit()
    response.delete_cookie(SESSION_COOKIE, path="/")
    return MessageOut(message="Signed out")


@router.get("/me", response_model=MeOut)
def me(auth: AuthContext = Depends(get_auth), db: Session = Depends(get_db)) -> MeOut:
    return _me(db, auth.user, auth.session)


@router.post("/forgot-password", status_code=202, response_model=MessageOut)
def forgot_password(body: EmailIn, request: Request, db: Session = Depends(get_db)) -> MessageOut:
    _limit(f"forgot:{_client_ip(request)}", 10, 3600)
    _limit(f"forgot:{body.email.lower()}", 3, 3600)
    user = db.scalar(select(User).where(User.email == body.email))
    if user is not None and user.is_active:
        s = get_settings()
        raw, _ = issue_token(
            db,
            TokenType.reset_password,
            timedelta(minutes=s.reset_token_ttl_minutes),
            user_id=user.id,
        )
        db.commit()
        enqueue(reset_email(user.email, f"{s.app_base_url}/reset-password?token={raw}"))
    return MessageOut(message=GENERIC_EMAIL_MSG)


@router.post("/reset-password", response_model=MessageOut)
def reset_password(body: ResetIn, db: Session = Depends(get_db)) -> MessageOut:
    tok = consume_token(db, body.token, TokenType.reset_password)
    user = db.get(User, tok.user_id)
    if user is None:
        raise HTTPException(400, "This link is invalid or has expired")
    problems = password_problems(body.password, user.email)
    if problems:
        raise HTTPException(422, "Password " + "; ".join(problems))
    user.password_hash = hash_password(body.password)
    # Receiving the reset link also proves control of the mailbox.
    user.email_verified_at = user.email_verified_at or datetime.now(UTC)
    db.execute(
        update(UserSession)
        .where(UserSession.user_id == user.id, UserSession.revoked_at.is_(None))
        .values(revoked_at=datetime.now(UTC))
    )
    audit(
        db,
        actor=user.id,
        org=None,
        action="user.password_reset",
        target_type="user",
        target_id=user.id,
    )
    db.commit()
    return MessageOut(message="Password updated. Sign in with your new password.")


@router.post("/change-password", response_model=MessageOut)
def change_password(
    body: ChangePasswordIn, auth: AuthContext = Depends(get_auth), db: Session = Depends(get_db)
) -> MessageOut:
    user = auth.user
    if not verify_password(user.password_hash, body.current_password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Current password is incorrect")
    problems = password_problems(body.new_password, user.email)
    if problems:
        raise HTTPException(422, "Password " + "; ".join(problems))
    user.password_hash = hash_password(body.new_password)
    # Keep this session, revoke all others.
    db.execute(
        update(UserSession)
        .where(
            UserSession.user_id == user.id,
            UserSession.id != auth.session.id,
            UserSession.revoked_at.is_(None),
        )
        .values(revoked_at=datetime.now(UTC))
    )
    audit(
        db,
        actor=user.id,
        org=None,
        action="user.password_changed",
        target_type="user",
        target_id=user.id,
    )
    db.commit()
    return MessageOut(message="Password changed. Other sessions were signed out.")
