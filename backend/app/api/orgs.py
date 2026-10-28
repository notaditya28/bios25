import uuid
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.auth import create_org_with_owner
from app.config import get_settings
from app.db import get_db
from app.deps import OrgContext, company_in_org, get_org, get_owner_org, get_verified_user
from app.models import (
    AuthToken,
    ClientCompany,
    Membership,
    Organization,
    OrgType,
    Partner,
    Role,
    Shipment,
    SubjectType,
    TokenType,
    User,
)
from app.schemas.auth import MessageOut, TokenIn
from app.schemas.org import (
    CompanyIn,
    CompanyOut,
    InviteIn,
    InviteOut,
    MemberOut,
    OrgIn,
    OrgOut,
    OrgPatch,
    RolePatch,
)
from app.screening.service import forget_subject
from app.services.audit import audit
from app.services.email import invite_email
from app.services.tokens import consume_token, issue_token
from app.tasks.email import enqueue
from app.tasks.sanctions import request_screening

router = APIRouter(prefix="/api", tags=["organizations"])

INVITE_TTL = timedelta(days=7)


@router.get("/orgs", response_model=list[OrgOut])
def my_orgs(user: User = Depends(get_verified_user), db: Session = Depends(get_db)) -> list:
    return list(
        db.scalars(
            select(Organization)
            .join(Membership)
            .where(Membership.user_id == user.id)
            .order_by(Organization.name)
        )
    )


@router.post("/orgs", response_model=OrgOut, status_code=201)
def create_org(
    body: OrgIn, user: User = Depends(get_verified_user), db: Session = Depends(get_db)
) -> Organization:
    org = create_org_with_owner(db, user, body.name, body.type)
    audit(
        db,
        actor=user.id,
        org=org.id,
        action="org.created",
        target_type="organization",
        target_id=org.id,
        after=body.model_dump(mode="json"),
    )
    db.commit()
    return org


@router.get("/orgs/{org_id}", response_model=OrgOut)
def get_org_detail(ctx: OrgContext = Depends(get_org)) -> Organization:
    return ctx.org


@router.patch("/orgs/{org_id}", response_model=OrgOut)
def rename_org(
    body: OrgPatch, ctx: OrgContext = Depends(get_owner_org), db: Session = Depends(get_db)
) -> Organization:
    before = {"name": ctx.org.name}
    ctx.org.name = body.name
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="org.renamed",
        target_type="organization",
        target_id=ctx.org.id,
        before=before,
        after={"name": body.name},
    )
    db.commit()
    return ctx.org


# ---- members & invitations ------------------------------------------------------------


@router.get("/orgs/{org_id}/members", response_model=list[MemberOut])
def list_members(ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)) -> list:
    rows = db.execute(
        select(Membership, User)
        .join(User, User.id == Membership.user_id)
        .where(Membership.organization_id == ctx.org.id)
        .order_by(User.full_name)
    ).all()
    return [
        MemberOut(
            user_id=u.id, email=u.email, full_name=u.full_name, role=m.role, joined_at=m.created_at
        )
        for m, u in rows
    ]


def _owner_count(db: Session, org_id: uuid.UUID) -> int:
    return (
        db.scalar(
            select(func.count()).where(
                Membership.organization_id == org_id, Membership.role == Role.owner
            )
        )
        or 0
    )


def _membership(db: Session, org_id: uuid.UUID, user_id: uuid.UUID) -> Membership:
    m = db.scalar(
        select(Membership).where(
            Membership.organization_id == org_id, Membership.user_id == user_id
        )
    )
    if m is None:
        raise HTTPException(404, "Member not found")
    return m


@router.patch("/orgs/{org_id}/members/{user_id}", response_model=MessageOut)
def change_role(
    user_id: uuid.UUID,
    body: RolePatch,
    ctx: OrgContext = Depends(get_owner_org),
    db: Session = Depends(get_db),
) -> MessageOut:
    m = _membership(db, ctx.org.id, user_id)
    if m.role == Role.owner and body.role != Role.owner and _owner_count(db, ctx.org.id) <= 1:
        raise HTTPException(400, "An organization must keep at least one owner")
    before = {"role": m.role.value}
    m.role = body.role
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="member.role_changed",
        target_type="user",
        target_id=user_id,
        before=before,
        after={"role": body.role.value},
    )
    db.commit()
    return MessageOut(message="Role updated")


@router.delete("/orgs/{org_id}/members/{user_id}", response_model=MessageOut)
def remove_member(
    user_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> MessageOut:
    # Owners can remove anyone; staff can only leave themselves.
    if user_id != ctx.user.id:
        ctx.require_owner()
    m = _membership(db, ctx.org.id, user_id)
    if m.role == Role.owner and _owner_count(db, ctx.org.id) <= 1:
        raise HTTPException(400, "An organization must keep at least one owner")
    db.delete(m)
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="member.removed",
        target_type="user",
        target_id=user_id,
        before={"role": m.role.value},
    )
    db.commit()
    return MessageOut(message="Member removed")


@router.get("/orgs/{org_id}/invites", response_model=list[InviteOut])
def list_invites(ctx: OrgContext = Depends(get_owner_org), db: Session = Depends(get_db)) -> list:
    toks = db.scalars(
        select(AuthToken).where(
            AuthToken.organization_id == ctx.org.id,
            AuthToken.type == TokenType.invite,
            AuthToken.used_at.is_(None),
            AuthToken.expires_at > datetime.now(UTC),
        )
    )
    return [
        InviteOut(id=t.id, email=t.email or "", role=t.role or Role.staff, expires_at=t.expires_at)
        for t in toks
    ]


@router.post("/orgs/{org_id}/invites", response_model=InviteOut, status_code=201)
def invite(
    body: InviteIn, ctx: OrgContext = Depends(get_owner_org), db: Session = Depends(get_db)
) -> InviteOut:
    already = db.scalar(
        select(Membership)
        .join(User, User.id == Membership.user_id)
        .where(Membership.organization_id == ctx.org.id, User.email == body.email)
    )
    if already is not None:
        raise HTTPException(409, "That person is already a member")
    raw, tok = issue_token(
        db,
        TokenType.invite,
        INVITE_TTL,
        email=body.email,
        organization_id=ctx.org.id,
        role=body.role,
    )
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="member.invited",
        target_type="email",
        target_id=body.email,
        after={"role": body.role.value},
    )
    db.commit()
    s = get_settings()
    enqueue(
        invite_email(
            body.email,
            ctx.org.name,
            ctx.user.full_name,
            f"{s.app_base_url}/accept-invite?token={raw}",
        )
    )
    return InviteOut(id=tok.id, email=body.email, role=body.role, expires_at=tok.expires_at)


@router.delete("/orgs/{org_id}/invites/{invite_id}", response_model=MessageOut)
def revoke_invite(
    invite_id: uuid.UUID, ctx: OrgContext = Depends(get_owner_org), db: Session = Depends(get_db)
) -> MessageOut:
    tok = db.get(AuthToken, invite_id)
    if tok is None or tok.organization_id != ctx.org.id or tok.type != TokenType.invite:
        raise HTTPException(404, "Invitation not found")
    tok.used_at = datetime.now(UTC)
    db.commit()
    return MessageOut(message="Invitation revoked")


@router.post("/invites/accept", response_model=MessageOut)
def accept_invite(
    body: TokenIn, user: User = Depends(get_verified_user), db: Session = Depends(get_db)
) -> MessageOut:
    """For people who already have an account; new users accept via /auth/register."""
    tok = consume_token(db, body.token, TokenType.invite)
    if (tok.email or "").lower() != user.email.lower():
        raise HTTPException(400, "This invitation was sent to a different email address")
    assert tok.organization_id and tok.role
    try:
        db.add(Membership(user_id=user.id, organization_id=tok.organization_id, role=tok.role))
        audit(
            db,
            actor=user.id,
            org=tok.organization_id,
            action="member.joined",
            target_type="user",
            target_id=user.id,
            after={"role": tok.role.value},
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "You are already a member of this organization") from None
    return MessageOut(message="Invitation accepted")


# ---- client companies -----------------------------------------------------------------


@router.get("/orgs/{org_id}/companies", response_model=list[CompanyOut])
def list_companies(ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)) -> list:
    return list(
        db.scalars(
            select(ClientCompany)
            .where(ClientCompany.organization_id == ctx.org.id)
            .order_by(ClientCompany.is_self.desc(), ClientCompany.name)
        )
    )


@router.post("/orgs/{org_id}/companies", response_model=CompanyOut, status_code=201)
def create_company(
    body: CompanyIn, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> ClientCompany:
    if ctx.org.type != OrgType.forwarder:
        raise HTTPException(
            400, "Only forwarder/CHA organizations manage multiple client companies"
        )
    company = ClientCompany(organization_id=ctx.org.id, **body.model_dump())
    db.add(company)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "A company with that name already exists") from None
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="company.created",
        target_type="company",
        target_id=company.id,
        after=body.model_dump(mode="json"),
    )
    db.commit()
    request_screening(SubjectType.company, company.id)
    return company


@router.patch("/orgs/{org_id}/companies/{company_id}", response_model=CompanyOut)
def update_company(
    company_id: uuid.UUID,
    body: CompanyIn,
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> ClientCompany:
    company = company_in_org(db, ctx, company_id)
    before = CompanyOut.model_validate(company).model_dump(mode="json")
    for k, v in body.model_dump().items():
        setattr(company, k, v)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "A company with that name already exists") from None
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="company.updated",
        target_type="company",
        target_id=company.id,
        before=before,
        after=body.model_dump(mode="json"),
    )
    db.commit()
    request_screening(SubjectType.company, company.id)
    return company


@router.delete("/orgs/{org_id}/companies/{company_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_company(
    company_id: uuid.UUID, ctx: OrgContext = Depends(get_owner_org), db: Session = Depends(get_db)
) -> None:
    company = company_in_org(db, ctx, company_id)
    if company.is_self:
        raise HTTPException(400, "The organization's own company cannot be deleted")
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="company.deleted",
        target_type="company",
        target_id=company.id,
        before={"name": company.name},
    )
    # Screenings reference subjects without an FK, so clear the company's and its children's.
    child_ids = [company.id]
    child_ids += db.scalars(select(Partner.id).where(Partner.company_id == company.id)).all()
    child_ids += db.scalars(select(Shipment.id).where(Shipment.company_id == company.id)).all()
    for sid in child_ids:
        forget_subject(db, sid)
    db.delete(company)
    db.commit()
