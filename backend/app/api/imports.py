import uuid
from typing import Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps import OrgContext, company_in_org, get_org
from app.schemas.trade import ImportResult
from app.services.csv_import import MAX_BYTES, ImportFileError, run_import, template_csv
from app.tasks.sanctions import rescreen_org

router = APIRouter(prefix="/api", tags=["imports"])

Kind = Literal["partners", "lanes", "shipments"]


@router.get("/imports/templates/{kind}.csv", response_class=PlainTextResponse)
def template(kind: Kind) -> PlainTextResponse:
    return PlainTextResponse(
        template_csv(kind),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="traderadar-{kind}.csv"'},
    )


@router.post("/orgs/{org_id}/imports/{kind}", response_model=ImportResult)
def import_csv(
    kind: Kind,
    company_id: uuid.UUID = Query(),
    dry_run: bool = Query(default=True),
    file: UploadFile = File(),
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> ImportResult:
    company = company_in_org(db, ctx, company_id)
    # Sync route: runs in the threadpool, so the blocking read and DB work don't stall the loop.
    content = file.file.read(MAX_BYTES + 1)
    try:
        result = run_import(
            db,
            company=company,
            kind=kind,
            filename=file.filename or "upload.csv",
            content=content,
            user_id=ctx.user.id,
            dry_run=dry_run,
        )
    except ImportFileError as e:
        raise HTTPException(422, str(e)) from None
    if result.status == "committed" and kind in ("partners", "shipments"):
        rescreen_org.delay(str(ctx.org.id))
    return result
