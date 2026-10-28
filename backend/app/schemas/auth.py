import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator

from app.models import OrgType, Role


class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(max_length=256)
    full_name: str = Field(min_length=1, max_length=200)
    org_name: str | None = Field(default=None, min_length=1, max_length=200)
    org_type: OrgType | None = None
    invite_token: str | None = Field(default=None, max_length=100)

    @model_validator(mode="after")
    def _org_or_invite(self) -> "RegisterIn":
        if not self.invite_token and not (self.org_name and self.org_type):
            raise ValueError("org_name and org_type are required unless accepting an invitation")
        return self


class LoginIn(BaseModel):
    email: EmailStr
    password: str = Field(max_length=256)


class EmailIn(BaseModel):
    email: EmailStr


class TokenIn(BaseModel):
    token: str = Field(min_length=10, max_length=100)


class ResetIn(BaseModel):
    token: str = Field(min_length=10, max_length=100)
    password: str = Field(max_length=256)


class ChangePasswordIn(BaseModel):
    current_password: str = Field(max_length=256)
    new_password: str = Field(max_length=256)


class MembershipOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    organization_id: uuid.UUID
    organization_name: str
    organization_type: OrgType
    role: Role


class MeOut(BaseModel):
    id: uuid.UUID
    email: str
    full_name: str
    email_verified: bool
    is_platform_admin: bool
    csrf_token: str
    memberships: list[MembershipOut]


class MessageOut(BaseModel):
    message: str


class SessionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    expires_at: datetime
    ip: str | None
    user_agent: str | None
