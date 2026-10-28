import re
import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from app.models import OrgType, Role

IEC_RE = re.compile(r"^[A-Z0-9]{10}$")
GSTIN_RE = re.compile(r"^[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}$")


class OrgIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    type: OrgType


class OrgPatch(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class OrgOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    type: OrgType
    is_demo: bool
    created_at: datetime


class MemberOut(BaseModel):
    user_id: uuid.UUID
    email: str
    full_name: str
    role: Role
    joined_at: datetime


class RolePatch(BaseModel):
    role: Role


class InviteIn(BaseModel):
    email: EmailStr
    role: Role = Role.staff


class InviteOut(BaseModel):
    id: uuid.UUID
    email: str
    role: Role
    expires_at: datetime


class CompanyIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    iec_code: str | None = None
    gstin: str | None = None
    contact_email: EmailStr | None = None

    @field_validator("iec_code")
    @classmethod
    def _iec(cls, v: str | None) -> str | None:
        if not v:
            return None
        v = v.strip().upper()
        if not IEC_RE.match(v):
            raise ValueError("IEC must be 10 letters/digits")
        return v

    @field_validator("gstin")
    @classmethod
    def _gstin(cls, v: str | None) -> str | None:
        if not v:
            return None
        v = v.strip().upper()
        if not GSTIN_RE.match(v):
            raise ValueError("GSTIN must be 15 characters in the standard format")
        return v


class CompanyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    is_self: bool
    iec_code: str | None
    gstin: str | None
    contact_email: str | None
    is_demo: bool
