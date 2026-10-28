"""Single-use emailed tokens (verification, reset, invitation). Only hashes are stored."""

from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AuthToken, TokenType
from app.security import hash_token, new_token


def issue_token(
    db: Session, type_: TokenType, ttl: timedelta, **fields: object
) -> tuple[str, AuthToken]:
    raw = new_token()
    tok = AuthToken(
        type=type_,
        token_hash=hash_token(raw),
        expires_at=datetime.now(UTC) + ttl,
        **fields,
    )
    db.add(tok)
    db.flush()
    return raw, tok


def consume_token(db: Session, raw: str, type_: TokenType) -> AuthToken:
    tok = db.scalar(
        select(AuthToken)
        .where(AuthToken.token_hash == hash_token(raw), AuthToken.type == type_)
        .with_for_update()
    )
    if tok is None or tok.used_at is not None or tok.expires_at <= datetime.now(UTC):
        raise HTTPException(400, "This link is invalid or has expired")
    tok.used_at = datetime.now(UTC)
    return tok
