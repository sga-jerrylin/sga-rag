#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import json
from typing import Any

from api.apps import current_user
from api.constants import MEMORY_NAME_LIMIT, MEMORY_SIZE_LIMIT
from api.db import TenantPermission
from api.db.db_models import DB, User
from api.db.km_models import KmMemory, KmSpace, KmSpaceProfile
from api.db.services.canvas_service import UserCanvasService
from api.db.services.user_service import UserTenantService
from api.km.services.memory_km_service import KmMemoryService
from api.km.services.space_km_service import SpaceKmService
from common.constants import ForgettingPolicy, MemoryStorageType, MemoryType
from common.exceptions import ArgumentException, NotFoundException
from common.misc_utils import get_uuid
from common.time_utils import current_timestamp, get_format_time, timestamp_to_date


_DEFAULT_MEMORY_SIZE = 5 * 1024 * 1024
_DEFAULT_TEMPERATURE = 0.5
_COMPAT_MESSAGE_KINDS = {"raw", "semantic", "episodic", "procedural", "dialogue", "text"}


def _memory_type_names() -> set[str]:
    return {e.name.lower() for e in MemoryType}


def _normalize_memory_types(memory_types: list[str] | None) -> list[str]:
    memory_types = memory_types or [MemoryType.RAW.name.lower()]
    if not isinstance(memory_types, list):
        raise ArgumentException("Memory type must be a list.")

    normalized = []
    valid_types = _memory_type_names()
    for memory_type in memory_types:
        name = str(memory_type).strip().lower()
        if not name:
            continue
        if name not in valid_types:
            raise ArgumentException(f"Memory type '{name}' is not supported.")
        if name not in normalized:
            normalized.append(name)
    return normalized or [MemoryType.RAW.name.lower()]


def _default_profile_config(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    memory_types = _normalize_memory_types(payload.get("memory_type"))
    return {
        "avatar": payload.get("avatar"),
        "memory_type": memory_types,
        "storage_type": payload.get("storage_type") or MemoryStorageType.TABLE.value,
        "embd_id": payload.get("embd_id") or "",
        "llm_id": payload.get("llm_id") or "",
        "tenant_embd_id": payload.get("tenant_embd_id"),
        "tenant_llm_id": payload.get("tenant_llm_id"),
        "permissions": payload.get("permissions") or TenantPermission.ME.value,
        "description": payload.get("description") or "",
        "memory_size": int(payload.get("memory_size") or _DEFAULT_MEMORY_SIZE),
        "forgetting_policy": payload.get("forgetting_policy") or ForgettingPolicy.FIFO.value,
        "temperature": float(payload.get("temperature") or _DEFAULT_TEMPERATURE),
        "system_prompt": payload.get("system_prompt") or "",
        "user_prompt": payload.get("user_prompt") or "",
    }


def _deduplicate_space_name(tenant_id: str, name: str, exclude_id: str | None = None) -> str:
    base_name = name.strip()
    candidate = base_name
    suffix = 1

    with DB.connection_context():
        while True:
            query = KmSpace.select(KmSpace.id).where(
                KmSpace.tenant_id == tenant_id,
                KmSpace.name == candidate,
                KmSpace.is_deleted == False,
            )
            if exclude_id:
                query = query.where(KmSpace.id != exclude_id)
            if not query.exists():
                return candidate
            candidate = f"{base_name}_{suffix}"
            suffix += 1


def _load_space(space_id: str) -> KmSpace:
    with DB.connection_context():
        space = KmSpace.get_or_none(
            KmSpace.id == space_id,
            KmSpace.is_deleted == False,
        )
    if not space:
        raise NotFoundException(f"Memory '{space_id}' not found.")
    return space


def _get_profile(space: KmSpace) -> dict[str, Any]:
    with DB.connection_context():
        profile = KmSpaceProfile.get_or_none(
            KmSpaceProfile.tenant_id == space.tenant_id,
            KmSpaceProfile.space_id == space.id,
        )
    config = _default_profile_config()
    if profile and isinstance(profile.config, dict):
        config.update(profile.config)
    if not config.get("description"):
        config["description"] = space.description or ""
    return config


def _upsert_profile(space: KmSpace, config: dict[str, Any]) -> dict[str, Any]:
    with DB.connection_context():
        profile = KmSpaceProfile.get_or_none(
            KmSpaceProfile.tenant_id == space.tenant_id,
            KmSpaceProfile.space_id == space.id,
        )
        if profile:
            profile.config = config
            profile.update_time = current_timestamp()
            profile.update_date = get_format_time()
            profile.save()
        else:
            profile = KmSpaceProfile.create(
                id=get_uuid(),
                tenant_id=space.tenant_id,
                space_id=space.id,
                config=config,
                create_time=current_timestamp(),
                create_date=get_format_time(),
                update_time=current_timestamp(),
                update_date=get_format_time(),
            )
    return profile.config if isinstance(profile.config, dict) else config


def _resolve_owner_names(tenant_ids: list[str]) -> dict[str, str]:
    tenant_ids = [tenant_id for tenant_id in tenant_ids if tenant_id]
    if not tenant_ids:
        return {}
    with DB.connection_context():
        users = User.select(User.id, User.nickname).where(User.id.in_(tenant_ids))
        return {user.id: user.nickname for user in users}


def _format_memory(space: KmSpace, profile: dict[str, Any], owner_name: str | None = None) -> dict[str, Any]:
    description = profile.get("description")
    if not description:
        description = space.description or ""
    return {
        "id": space.id,
        "name": space.name,
        "avatar": profile.get("avatar"),
        "tenant_id": space.tenant_id,
        "owner_name": owner_name,
        "memory_type": list(profile.get("memory_type") or [MemoryType.RAW.name.lower()]),
        "storage_type": profile.get("storage_type") or MemoryStorageType.TABLE.value,
        "embd_id": profile.get("embd_id") or "",
        "llm_id": profile.get("llm_id") or "",
        "permissions": profile.get("permissions") or TenantPermission.ME.value,
        "description": description,
        "memory_size": int(profile.get("memory_size") or _DEFAULT_MEMORY_SIZE),
        "forgetting_policy": profile.get("forgetting_policy") or ForgettingPolicy.FIFO.value,
        "temperature": float(profile.get("temperature") or _DEFAULT_TEMPERATURE),
        "system_prompt": profile.get("system_prompt") or "",
        "user_prompt": profile.get("user_prompt") or "",
        "create_time": space.create_time,
        "create_date": space.create_date,
        "update_time": space.update_time,
        "update_date": space.update_date,
    }


def _parse_source_ref(item: KmMemory) -> dict[str, Any]:
    if not item.source_ref:
        return {}
    if isinstance(item.source_ref, dict):
        return item.source_ref
    try:
        return json.loads(item.source_ref)
    except Exception:
        return {}


def _dump_source_ref(meta: dict[str, Any]) -> str:
    return json.dumps(meta, ensure_ascii=False)


def _format_dt(value) -> str:
    if value is None:
        return ""
    try:
        return value.isoformat(sep=" ", timespec="seconds")
    except Exception:
        return str(value)


def _message_meta_from_item(item: KmMemory) -> dict[str, Any]:
    meta = _parse_source_ref(item)
    meta.setdefault("message_type", item.content_type or MemoryType.RAW.name.lower())
    meta.setdefault("user_id", item.owner_id or "")
    meta.setdefault("agent_id", "")
    meta.setdefault("session_id", item.trace_id or "")
    meta.setdefault("valid_at", _format_dt(item.create_date))
    meta.setdefault("invalid_at", "")
    meta.setdefault("forget_at", "")
    meta.setdefault("status", True)
    meta.setdefault("source_id", "-")
    return meta


def _format_message(item: KmMemory, agent_name_mapping: dict[str, str] | None = None, include_content: bool = False) -> dict[str, Any]:
    meta = _message_meta_from_item(item)
    message = {
        "message_id": item.id,
        "message_type": meta.get("message_type") or MemoryType.RAW.name.lower(),
        "source_id": meta.get("source_id") or "-",
        "memory_id": item.space_id,
        "user_id": meta.get("user_id") or "",
        "agent_id": meta.get("agent_id") or "",
        "agent_name": (agent_name_mapping or {}).get(meta.get("agent_id"), "Unknown"),
        "session_id": meta.get("session_id") or "",
        "valid_at": meta.get("valid_at") or _format_dt(item.create_date),
        "invalid_at": meta.get("invalid_at") or "",
        "forget_at": meta.get("forget_at") or "",
        "status": bool(meta.get("status", True)),
        "extract": [],
        "task": None,
    }
    if include_content:
        message["content"] = item.content
    return message


def _query_space_messages(space_id: str) -> list[KmMemory]:
    with DB.connection_context():
        rows = list(
            KmMemory.select()
            .where(
                KmMemory.space_id == space_id,
                KmMemory.is_deleted == False,
            )
            .order_by(KmMemory.create_time.desc())
        )
    return [row for row in rows if (row.content_type or "raw") in _COMPAT_MESSAGE_KINDS]


def _group_spaces_by_tenant(space_ids: list[str]) -> dict[str, list[KmSpace]]:
    grouped: dict[str, list[KmSpace]] = {}
    for space_id in space_ids:
        space = _load_space(space_id)
        grouped.setdefault(space.tenant_id, []).append(space)
    return grouped


def _get_message_item(space_id: str, message_id: str) -> KmMemory:
    with DB.connection_context():
        item = KmMemory.get_or_none(
            KmMemory.id == str(message_id),
            KmMemory.space_id == space_id,
            KmMemory.is_deleted == False,
        )
    if not item:
        raise NotFoundException(f"Message '{message_id}' in memory '{space_id}' not found.")
    return item


def _update_message_meta(space_id: str, message_id: str, updates: dict[str, Any]) -> None:
    item = _get_message_item(space_id, message_id)
    meta = _message_meta_from_item(item)
    meta.update(updates)
    with DB.connection_context():
        item.source_ref = _dump_source_ref(meta)
        item.update_time = current_timestamp()
        item.update_date = get_format_time()
        item.save()


def _match_message_filters(message: dict[str, Any], *, user_id: str = "", agent_id: str = "", session_id: str = "") -> bool:
    if user_id and message.get("user_id") != user_id:
        return False
    if agent_id and message.get("agent_id") != agent_id:
        return False
    if session_id and message.get("session_id") != session_id:
        return False
    return True


async def create_memory(memory_info: dict):
    name = (memory_info.get("name") or "").strip()
    if not name:
        raise ArgumentException("Memory name cannot be empty or whitespace.")
    if len(name) > MEMORY_NAME_LIMIT:
        raise ArgumentException(f"Memory name '{name}' exceeds limit of {MEMORY_NAME_LIMIT}.")

    config = _default_profile_config(memory_info)
    memory_name = _deduplicate_space_name(current_user.id, name)
    ok, code, message, data = await SpaceKmService.upsert(
        current_user.id,
        {
            "name": memory_name,
            "scope": "personal",
            "owner_id": current_user.id,
            "description": config.get("description") or "",
        },
    )
    if not ok or not data:
        return False, message

    space = _load_space(data["id"])
    _upsert_profile(space, config)
    owner_name = getattr(current_user, "nickname", None)
    return True, _format_memory(space, config, owner_name=owner_name)


async def update_memory(memory_id: str, new_memory_setting: dict):
    space = _load_space(memory_id)
    config = _get_profile(space)

    update_config = {}
    if "name" in new_memory_setting:
        name = (new_memory_setting.get("name") or "").strip()
        if not name:
            raise ArgumentException("Memory name cannot be empty or whitespace.")
        if len(name) > MEMORY_NAME_LIMIT:
            raise ArgumentException(f"Memory name '{name}' exceeds limit of {MEMORY_NAME_LIMIT}.")
        space.name = _deduplicate_space_name(space.tenant_id, name, exclude_id=space.id)
    if "permissions" in new_memory_setting and new_memory_setting["permissions"]:
        if new_memory_setting["permissions"] not in [e.value for e in TenantPermission]:
            raise ArgumentException(f"Unknown permission '{new_memory_setting['permissions']}'.")
        update_config["permissions"] = new_memory_setting["permissions"]
    if "memory_type" in new_memory_setting:
        update_config["memory_type"] = _normalize_memory_types(new_memory_setting["memory_type"])
    if "memory_size" in new_memory_setting:
        memory_size = int(new_memory_setting["memory_size"])
        if not 0 < memory_size <= MEMORY_SIZE_LIMIT:
            raise ArgumentException(f"Memory size should be in range (0, {MEMORY_SIZE_LIMIT}] Bytes.")
        update_config["memory_size"] = memory_size
    if "forgetting_policy" in new_memory_setting and new_memory_setting["forgetting_policy"]:
        if new_memory_setting["forgetting_policy"] not in [e.value for e in ForgettingPolicy]:
            raise ArgumentException(f"Forgetting policy '{new_memory_setting['forgetting_policy']}' is not supported.")
        update_config["forgetting_policy"] = new_memory_setting["forgetting_policy"]
    if "temperature" in new_memory_setting:
        temperature = float(new_memory_setting["temperature"])
        if not 0 <= temperature <= 1:
            raise ArgumentException("Temperature should be in range [0, 1].")
        update_config["temperature"] = temperature

    for key in [
        "avatar",
        "embd_id",
        "llm_id",
        "tenant_embd_id",
        "tenant_llm_id",
        "storage_type",
        "system_prompt",
        "user_prompt",
    ]:
        if key in new_memory_setting:
            update_config[key] = new_memory_setting[key]

    if "description" in new_memory_setting:
        space.description = new_memory_setting["description"]
        update_config["description"] = new_memory_setting["description"]

    with DB.connection_context():
        space.update_time = current_timestamp()
        space.update_date = get_format_time()
        space.save()

    config.update(update_config)
    _upsert_profile(space, config)
    owner_name = getattr(current_user, "nickname", None)
    return True, _format_memory(space, config, owner_name=owner_name)


async def delete_memory(memory_id):
    space = _load_space(memory_id)
    with DB.connection_context():
        space.is_deleted = True
        space.update_time = current_timestamp()
        space.update_date = get_format_time()
        space.save()

        KmMemory.update(
            is_deleted=True,
            update_time=current_timestamp(),
            update_date=get_format_time(),
        ).where(
            KmMemory.space_id == memory_id,
            KmMemory.tenant_id == space.tenant_id,
        ).execute()

        KmSpaceProfile.delete().where(
            KmSpaceProfile.space_id == memory_id,
            KmSpaceProfile.tenant_id == space.tenant_id,
        ).execute()
    return True


async def list_memory(filter_params: dict, keywords: str, page: int = 1, page_size: int = 50):
    tenant_ids = filter_params.get("tenant_id")
    if not tenant_ids:
        user_tenants = UserTenantService.get_user_tenant_relation_by_user_id(current_user.id)
        tenant_ids = [tenant["tenant_id"] for tenant in user_tenants]
        if not tenant_ids:
            tenant_ids = [current_user.id]
    elif isinstance(tenant_ids, list) and len(tenant_ids) == 1 and "," in tenant_ids[0]:
        tenant_ids = [tid.strip() for tid in tenant_ids[0].split(",") if tid.strip()]
    elif isinstance(tenant_ids, str):
        tenant_ids = [tenant_ids]

    memory_types = filter_params.get("memory_type")
    if isinstance(memory_types, list) and len(memory_types) == 1 and "," in memory_types[0]:
        memory_types = [item.strip().lower() for item in memory_types[0].split(",") if item.strip()]
    elif isinstance(memory_types, str):
        memory_types = [memory_types.strip().lower()]
    storage_type = filter_params.get("storage_type")

    with DB.connection_context():
        query = KmSpace.select().where(
            KmSpace.tenant_id.in_(tenant_ids),
            KmSpace.is_deleted == False,
        )
        if keywords:
            query = query.where(KmSpace.name.contains(keywords))
        spaces = list(query.order_by(KmSpace.update_time.desc()))

    profiles = {space.id: _get_profile(space) for space in spaces}
    owner_names = _resolve_owner_names([space.tenant_id for space in spaces])

    memory_list = []
    for space in spaces:
        profile = profiles[space.id]
        profile_types = [item.lower() for item in profile.get("memory_type", [])]
        if memory_types and not set(profile_types).intersection(set(memory_types)):
            continue
        if storage_type and profile.get("storage_type") != storage_type:
            continue
        memory_list.append(_format_memory(space, profile, owner_names.get(space.tenant_id)))

    total_count = len(memory_list)
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return {"memory_list": memory_list[start:end], "total_count": total_count}


async def get_memory_config(memory_id):
    space = _load_space(memory_id)
    owner_name = _resolve_owner_names([space.tenant_id]).get(space.tenant_id)
    return _format_memory(space, _get_profile(space), owner_name)


async def get_memory_messages(memory_id, agent_ids: list[str], keywords: str, page: int = 1, page_size: int = 50):
    space = _load_space(memory_id)
    profile = _get_profile(space)
    rows = _query_space_messages(memory_id)
    messages = []
    for row in rows:
        message = _format_message(row, include_content=False)
        if agent_ids and message.get("agent_id") not in agent_ids:
            continue
        if keywords and keywords not in (message.get("session_id") or "") and keywords not in (row.content or ""):
            continue
        messages.append(message)

    agent_ids_in_messages = list({message["agent_id"] for message in messages if message.get("agent_id")})
    agent_mapping = {}
    if agent_ids_in_messages:
        agent_list = UserCanvasService.get_basic_info_by_canvas_ids(agent_ids_in_messages)
        agent_mapping = {agent["id"]: agent["title"] for agent in agent_list}

    for message in messages:
        message["agent_name"] = agent_mapping.get(message["agent_id"], "Unknown")

    total_count = len(messages)
    start = max(page - 1, 0) * page_size
    end = start + page_size
    return {
        "messages": {"message_list": messages[start:end], "total_count": total_count},
        "storage_type": profile.get("storage_type") or MemoryStorageType.TABLE.value,
    }


async def add_message(memory_ids: list[str], message_dict: dict):
    if isinstance(memory_ids, str):
        memory_ids = [memory_ids]
    if not memory_ids:
        return False, "memory_id is required."

    try:
        grouped_spaces = _group_spaces_by_tenant(memory_ids)
    except NotFoundException as exc:
        return False, str(exc)

    valid_at = timestamp_to_date(current_timestamp())
    for tenant_id, spaces in grouped_spaces.items():
        items = []
        for space in spaces:
            meta = {
                "message_type": MemoryType.RAW.name.lower(),
                "source_id": "-",
                "user_id": message_dict.get("user_id", ""),
                "agent_id": message_dict.get("agent_id", ""),
                "session_id": message_dict.get("session_id", ""),
                "valid_at": valid_at,
                "invalid_at": "",
                "forget_at": "",
                "status": True,
            }
            items.append(
                {
                    "space_id": space.id,
                    "scope": "session",
                    "kind": MemoryType.RAW.name.lower(),
                    "owner_id": meta["user_id"],
                    "principal_id": meta["user_id"],
                    "trace_id": meta["session_id"],
                    "source_ref": _dump_source_ref(meta),
                    "content": f"User Input: {message_dict.get('user_input', '')}\nAgent Response: {message_dict.get('agent_response', '')}",
                }
            )

        ok, code, message, data = await KmMemoryService.put_items(tenant_id, {"items": items})
        if not ok:
            return False, message or "Failed to add message."
    return True, "success"


async def forget_message(memory_id: str, message_id: int):
    _load_space(memory_id)
    _update_message_meta(memory_id, str(message_id), {"forget_at": timestamp_to_date(current_timestamp())})
    return True


async def update_message_status(memory_id: str, message_id: int, status: bool):
    _load_space(memory_id)
    _update_message_meta(memory_id, str(message_id), {"status": bool(status)})
    return True


async def search_message(filter_dict: dict, params: dict):
    memory_ids = filter_dict.get("memory_id") or []
    if isinstance(memory_ids, str):
        memory_ids = [memory_ids]
    if not memory_ids:
        return []

    top_n = max(1, int(params.get("top_n") or 5))
    try:
        grouped_spaces = _group_spaces_by_tenant(memory_ids)
    except NotFoundException:
        return []

    messages = []
    for tenant_id, spaces in grouped_spaces.items():
        ok, code, message, data = await KmMemoryService.search_items(
            tenant_id,
            {
                "query": params.get("query") or "",
                "space_ids": [space.id for space in spaces],
                "top_k": max(top_n * 4, top_n),
                "min_similarity": params.get("similarity_threshold") or 0.2,
                "keywords_similarity_weight": params.get("keywords_similarity_weight") or 0.7,
                "mode": "hybrid",
                "include_archived": True,
            },
        )
        if not ok or not data:
            continue

        for item in data.get("items", []):
            with DB.connection_context():
                row = KmMemory.get_or_none(KmMemory.id == item["id"], KmMemory.is_deleted == False)
            if not row:
                continue
            message_row = _format_message(row, include_content=True)
            if not _match_message_filters(
                message_row,
                user_id=filter_dict.get("user_id", ""),
                agent_id=filter_dict.get("agent_id", ""),
                session_id=filter_dict.get("session_id", ""),
            ):
                continue
            message_row["_score"] = item.get("_score") or 0
            messages.append(message_row)

    messages.sort(key=lambda item: float(item.get("_score") or 0), reverse=True)
    return messages[:top_n]


async def get_messages(memory_ids: list[str], agent_id: str = "", session_id: str = "", limit: int = 10):
    if isinstance(memory_ids, str):
        memory_ids = [memory_ids]
    rows = []
    for memory_id in memory_ids:
        try:
            _load_space(memory_id)
        except NotFoundException:
            continue
        rows.extend(_query_space_messages(memory_id))

    rows.sort(key=lambda row: int(row.create_time or 0), reverse=True)
    messages = []
    for row in rows:
        message = _format_message(row, include_content=True)
        if not _match_message_filters(message, agent_id=agent_id or "", session_id=session_id or ""):
            continue
        messages.append(message)
        if len(messages) >= limit:
            break
    return messages


async def get_message_content(memory_id: str, message_id: int):
    _load_space(memory_id)
    item = _get_message_item(memory_id, str(message_id))
    return {"content": item.content, "content_embed": ""}
