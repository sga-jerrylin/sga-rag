# KM-CUSTOM

from __future__ import annotations

from typing import Any

from api.db.db_models import DB
from api.db.km_models import KmSpace
from common.constants import RetCode
from common.misc_utils import get_uuid
from common.time_utils import current_timestamp, get_format_time


class SpaceKmService:
    @classmethod
    def _space_to_dict(cls, space: KmSpace) -> dict[str, Any]:
        return {
            "id": space.id,
            "tenant_id": space.tenant_id,
            "name": space.name,
            "scope": space.scope,
            "owner_id": space.owner_id,
            "project_id": space.project_id,
            "agent_id": space.agent_id,
            "session_id": space.session_id,
            "description": space.description,
            "labels": list(space.labels or []),
            "memory_profile_id": space.memory_profile_id,
            "is_archived": bool(space.is_archived),
            "is_deleted": bool(space.is_deleted),
            "created_at": space.create_date.isoformat() if space.create_date else None,
            "updated_at": space.update_date.isoformat() if space.update_date else None,
        }

    @classmethod
    async def upsert(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        name = ((params or {}).get("name") or "").strip()
        if not name:
            return False, RetCode.ARGUMENT_ERROR, "`name` is required.", None

        space_id = (params or {}).get("id")
        scope = (params or {}).get("scope") or "personal"
        labels = (params or {}).get("labels") or []
        if not isinstance(labels, list):
            return False, RetCode.ARGUMENT_ERROR, "`labels` must be a list.", None

        fields = {
            "name": name,
            "scope": scope,
            "owner_id": (params or {}).get("owner_id"),
            "project_id": (params or {}).get("project_id"),
            "agent_id": (params or {}).get("agent_id"),
            "session_id": (params or {}).get("session_id"),
            "description": (params or {}).get("description"),
            "labels": labels,
            "memory_profile_id": (params or {}).get("memory_profile_id"),
        }

        with DB.connection_context():
            if space_id:
                space = KmSpace.get_or_none(
                    KmSpace.id == space_id,
                    KmSpace.tenant_id == tenant_id,
                    KmSpace.is_deleted == False,
                )
                if not space:
                    return False, RetCode.DATA_ERROR, "Space not found.", None
                for key, value in fields.items():
                    setattr(space, key, value)
                space.update_time = current_timestamp()
                space.update_date = get_format_time()
                space.save()
            else:
                space = KmSpace.create(
                    id=get_uuid(),
                    tenant_id=tenant_id,
                    is_archived=False,
                    is_deleted=False,
                    create_time=current_timestamp(),
                    create_date=get_format_time(),
                    update_time=current_timestamp(),
                    update_date=get_format_time(),
                    **fields,
                )

        return True, RetCode.SUCCESS, "", cls._space_to_dict(space)

    @classmethod
    async def list_spaces(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        page = max(1, int((params or {}).get("page") or 1))
        size = max(1, min(200, int((params or {}).get("size") or 50)))
        keywords = ((params or {}).get("keywords") or "").strip()
        scope = (params or {}).get("scope")
        owner_id = (params or {}).get("owner_id")
        include_archived = bool((params or {}).get("include_archived", False))

        with DB.connection_context():
            query = KmSpace.select().where(
                KmSpace.tenant_id == tenant_id,
                KmSpace.is_deleted == False,
            )
            if scope:
                query = query.where(KmSpace.scope == scope)
            if owner_id:
                query = query.where(KmSpace.owner_id == owner_id)
            if not include_archived:
                query = query.where(KmSpace.is_archived == False)
            if keywords:
                query = query.where(KmSpace.name.contains(keywords))

            total = query.count()
            spaces = list(
                query.order_by(KmSpace.update_time.desc()).paginate(page, size)
            )

        return True, RetCode.SUCCESS, "", {
            "items": [cls._space_to_dict(space) for space in spaces],
            "total": total,
            "page": page,
            "size": size,
        }

    @classmethod
    async def get(cls, tenant_id: str, space_id: str) -> tuple[bool, int, str, dict | None]:
        with DB.connection_context():
            space = KmSpace.get_or_none(
                KmSpace.id == space_id,
                KmSpace.tenant_id == tenant_id,
                KmSpace.is_deleted == False,
            )
        if not space:
            return False, RetCode.DATA_ERROR, "Space not found.", None
        return True, RetCode.SUCCESS, "", cls._space_to_dict(space)

    @classmethod
    async def existing_ids(cls, tenant_id: str, space_ids: list[str]) -> list[str]:
        if not space_ids:
            return []

        with DB.connection_context():
            rows = (
                KmSpace.select(KmSpace.id)
                .where(
                    KmSpace.tenant_id == tenant_id,
                    KmSpace.id.in_(space_ids),
                    KmSpace.is_deleted == False,
                )
            )
            return [row.id for row in rows]

    @classmethod
    async def delete(cls, tenant_id: str, space_id: str) -> tuple[bool, int, str, dict | None]:
        with DB.connection_context():
            space = KmSpace.get_or_none(
                KmSpace.id == space_id,
                KmSpace.tenant_id == tenant_id,
                KmSpace.is_deleted == False,
            )
            if not space:
                return False, RetCode.DATA_ERROR, "Space not found.", None
            space.is_deleted = True
            space.update_time = current_timestamp()
            space.update_date = get_format_time()
            space.save()
        return True, RetCode.SUCCESS, "", {"id": space_id, "deleted": True}
