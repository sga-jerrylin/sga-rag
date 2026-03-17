# KM-CUSTOM

from __future__ import annotations

from datetime import datetime
from typing import Any

from api.db.db_models import DB
from api.db.km_models import KmFact
from common.constants import RetCode
from common.misc_utils import get_uuid
from common.time_utils import current_timestamp, get_format_time


class FactKmService:
    @classmethod
    def _fact_to_dict(cls, fact: KmFact) -> dict[str, Any]:
        return {
            "id": fact.id,
            "tenant_id": fact.tenant_id,
            "space_id": fact.kb_id,
            "subject": fact.subject,
            "predicate": fact.predicate,
            "object": fact.object,
            "confidence": float(fact.confidence or 0.0),
            "valid_from": fact.valid_from.isoformat() if fact.valid_from else None,
            "valid_to": fact.valid_to.isoformat() if fact.valid_to else None,
            "source_doc_id": fact.source_doc_id,
            "observed_at": fact.observed_at.isoformat() if fact.observed_at else None,
            "version": int(fact.version or 1),
            "status": fact.status,
            "created_at": fact.create_date.isoformat() if fact.create_date else None,
            "updated_at": fact.update_date.isoformat() if fact.update_date else None,
        }

    @classmethod
    async def upsert(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        subject = ((params or {}).get("subject") or "").strip()
        predicate = ((params or {}).get("predicate") or "").strip()
        obj = ((params or {}).get("object") or "").strip()
        space_id = ((params or {}).get("space_id") or (params or {}).get("kb_id") or "").strip()
        if not all([subject, predicate, obj, space_id]):
            return False, RetCode.ARGUMENT_ERROR, "`space_id`, `subject`, `predicate`, `object` are required.", None

        with DB.connection_context():
            fact = KmFact.get_or_none(
                KmFact.tenant_id == tenant_id,
                KmFact.kb_id == space_id,
                KmFact.subject == subject,
                KmFact.predicate == predicate,
                KmFact.object == obj,
                KmFact.status != "retracted",
            )
            if fact:
                fact.confidence = float((params or {}).get("confidence") or fact.confidence or 0.8)
                fact.valid_from = (params or {}).get("valid_from") or fact.valid_from
                fact.valid_to = (params or {}).get("valid_to") or fact.valid_to
                fact.source_doc_id = (params or {}).get("source_doc_id") or fact.source_doc_id
                fact.observed_at = (params or {}).get("observed_at") or fact.observed_at or datetime.now()
                fact.status = (params or {}).get("status") or fact.status or "active"
                fact.version = int(fact.version or 1) + 1
                fact.update_time = current_timestamp()
                fact.update_date = get_format_time()
                fact.save()
            else:
                fact = KmFact.create(
                    id=get_uuid(),
                    tenant_id=tenant_id,
                    kb_id=space_id,
                    subject=subject,
                    predicate=predicate,
                    object=obj,
                    confidence=float((params or {}).get("confidence") or 0.8),
                    valid_from=(params or {}).get("valid_from"),
                    valid_to=(params or {}).get("valid_to"),
                    source_doc_id=(params or {}).get("source_doc_id"),
                    observed_at=(params or {}).get("observed_at") or datetime.now(),
                    version=1,
                    status=(params or {}).get("status") or "active",
                    create_time=current_timestamp(),
                    create_date=get_format_time(),
                    update_time=current_timestamp(),
                    update_date=get_format_time(),
                )
        return True, RetCode.SUCCESS, "", cls._fact_to_dict(fact)

    @classmethod
    async def search(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        page = max(1, int((params or {}).get("page") or 1))
        size = max(1, min(200, int((params or {}).get("size") or 20)))
        query = ((params or {}).get("query") or "").strip()
        space_id = ((params or {}).get("space_id") or (params or {}).get("kb_id") or "").strip()
        subject = ((params or {}).get("subject") or "").strip()
        predicate = ((params or {}).get("predicate") or "").strip()
        obj = ((params or {}).get("object") or "").strip()
        statuses = (params or {}).get("statuses") or []

        with DB.connection_context():
            facts = KmFact.select().where(KmFact.tenant_id == tenant_id)
            if space_id:
                facts = facts.where(KmFact.kb_id == space_id)
            if subject:
                facts = facts.where(KmFact.subject.contains(subject))
            if predicate:
                facts = facts.where(KmFact.predicate.contains(predicate))
            if obj:
                facts = facts.where(KmFact.object.contains(obj))
            if query:
                facts = facts.where(
                    (KmFact.subject.contains(query)) |
                    (KmFact.predicate.contains(query)) |
                    (KmFact.object.contains(query))
                )
            if statuses:
                facts = facts.where(KmFact.status.in_(list(statuses)))

            total = facts.count()
            rows = list(
                facts.order_by(KmFact.update_time.desc()).paginate(page, size)
            )

        return True, RetCode.SUCCESS, "", {
            "items": [cls._fact_to_dict(fact) for fact in rows],
            "total": total,
            "page": page,
            "size": size,
        }

    @classmethod
    async def delete(cls, tenant_id: str, fact_id: str) -> tuple[bool, int, str, dict | None]:
        with DB.connection_context():
            fact = KmFact.get_or_none(KmFact.id == fact_id, KmFact.tenant_id == tenant_id)
            if not fact:
                return False, RetCode.DATA_ERROR, "Fact not found.", None
            fact.status = "retracted"
            fact.version = int(fact.version or 1) + 1
            fact.update_time = current_timestamp()
            fact.update_date = get_format_time()
            fact.save()
        return True, RetCode.SUCCESS, "", {"id": fact_id, "deleted": True}
