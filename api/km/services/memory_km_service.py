# KM-CUSTOM

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from api.db.db_models import DB
from api.db.km_models import KmMemory
from api.db.services.llm_service import LLMBundle
from common import settings
from common.constants import LLMType, RetCode
from common.doc_store.doc_store_base import FusionExpr, MatchDenseExpr, OrderByExpr
from common.misc_utils import get_uuid, thread_pool_exec
from common.time_utils import current_timestamp, get_format_time
from rag.nlp import rag_tokenizer, query as rag_query


_SUPPORTED_EMBED_DIMS = {512, 768, 1024, 1536}
_ITEM_QUERY_FIELDS = ["content_ltks^2", "content_sm_ltks"]


class KmMemoryService:
    @classmethod
    def _normalize_content_type(cls, params: dict) -> str:
        content_type = (params or {}).get("kind") or (params or {}).get("content_type") or "text"
        return str(content_type).strip() or "text"

    @classmethod
    def _normalize_scope(cls, params: dict) -> str:
        scope = (params or {}).get("scope") or "personal"
        return str(scope).strip() or "personal"

    @classmethod
    def _memory_to_dict(cls, m: KmMemory, *, score: float | None = None) -> dict[str, Any]:
        return {
            "id": m.id,
            "tenant_id": m.tenant_id,
            "content": m.content,
            "content_type": m.content_type,
            "kind": m.content_type,
            "scope": m.scope,
            "owner_id": m.owner_id,
            "space_id": m.space_id,
            "ttl_seconds": m.ttl_seconds,
            "expires_at": m.expires_at.isoformat() if m.expires_at else None,
            "importance": float(m.importance or 0.0),
            "access_count": int(m.access_count or 0),
            "last_accessed_at": m.last_accessed_at.isoformat() if m.last_accessed_at else None,
            "embedding_id": m.embedding_id,
            "principal_id": m.principal_id,
            "trace_id": m.trace_id,
            "source_ref": m.source_ref,
            "is_archived": bool(m.is_archived),
            "is_deleted": bool(m.is_deleted),
            "created_at": m.create_date.isoformat() if m.create_date else None,
            "updated_at": m.update_date.isoformat() if m.update_date else None,
            "_score": score,
        }

    @classmethod
    def _compute_expiry(cls, ttl_seconds: int | None) -> datetime | None:
        if ttl_seconds is None:
            return None
        try:
            ttl = int(ttl_seconds)
        except Exception:
            return None
        if ttl <= 0:
            return None
        return datetime.now() + timedelta(seconds=ttl)

    @classmethod
    def _index_name(cls, tenant_id: str) -> str:
        return f"km_memory_{tenant_id}"

    @classmethod
    def _ensure_supported_dims(cls, dims: int) -> tuple[bool, int, str]:
        if dims not in _SUPPORTED_EMBED_DIMS:
            return False, RetCode.OPERATING_ERROR, f"Embedding dims {dims} not supported by conf/mapping.json."
        return True, RetCode.SUCCESS, ""

    @classmethod
    def _build_index_doc(cls, memory: KmMemory, embedding: list[float]) -> dict[str, Any]:
        content_ltks = rag_tokenizer.tokenize(memory.content)
        content_sm_ltks = rag_tokenizer.fine_grained_tokenize(content_ltks)
        dims = len(embedding)
        return {
            "id": memory.id,
            "tenant_id": memory.tenant_id,
            "scope_kwd": memory.scope,
            "owner_id": memory.owner_id or "",
            "space_id": memory.space_id or "",
            "content_type_kwd": memory.content_type,
            "importance_flt": float(memory.importance or 0.0),
            "content_ltks": content_ltks,
            "content_sm_ltks": content_sm_ltks,
            f"q_{dims}_vec": list(embedding),
        }

    @classmethod
    async def _ensure_index(cls, tenant_id: str, dims: int) -> None:
        index_name = cls._index_name(tenant_id)
        if not settings.docStoreConn.index_exist(index_name, ""):
            settings.docStoreConn.create_idx(index_name, "", vector_size=dims, parser_id=None)

    @classmethod
    async def _embed_contents(cls, tenant_id: str, contents: list[str]) -> tuple[bool, int, str, list[list[float]] | None]:
        embd_mdl = LLMBundle(tenant_id, LLMType.EMBEDDING)
        embeddings, _ = await thread_pool_exec(embd_mdl.encode, contents)
        if not embeddings or not embeddings[0]:
            return False, RetCode.SERVER_ERROR, "Embedding generation failed.", None
        ok, code, message = cls._ensure_supported_dims(len(embeddings[0]))
        if not ok:
            return False, code, message, None
        return True, RetCode.SUCCESS, "", embeddings

    @classmethod
    def _build_text_query(cls, query_text: str, min_match: float):
        qryr = rag_query.FulltextQueryer()
        qryr.query_fields = _ITEM_QUERY_FIELDS
        return qryr.question(query_text, min_match=min_match)

    @classmethod
    def _query_filters(cls, tenant_id: str, params: dict) -> dict[str, Any]:
        filters: dict[str, Any] = {"tenant_id": tenant_id}
        scope = (params or {}).get("scope")
        owner_id = (params or {}).get("owner_id")
        if scope:
            filters["scope_kwd"] = scope
        if owner_id:
            filters["owner_id"] = owner_id

        space_ids = (params or {}).get("space_ids")
        if not space_ids and (params or {}).get("space_id"):
            space_ids = [(params or {}).get("space_id")]
        if space_ids:
            filters["space_id"] = list(space_ids)

        content_types = (params or {}).get("content_types") or []
        if not content_types:
            kind = (params or {}).get("kind")
            if kind:
                content_types = [kind]
        if content_types:
            filters["content_type_kwd"] = list(content_types)
        return filters

    @classmethod
    async def put_item(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        content = (params or {}).get("content")
        if not content:
            return False, RetCode.ARGUMENT_ERROR, "`content` is required.", None

        ok, code, message, embeddings = await cls._embed_contents(tenant_id, [content])
        if not ok or not embeddings:
            return False, code, message, None

        embedding = embeddings[0]
        expires_at = cls._compute_expiry((params or {}).get("ttl_seconds"))
        content_type = cls._normalize_content_type(params)
        scope = cls._normalize_scope(params)

        with DB.connection_context():
            memory = KmMemory.create(
                id=get_uuid(),
                tenant_id=tenant_id,
                content=content,
                content_type=content_type,
                scope=scope,
                owner_id=(params or {}).get("owner_id"),
                space_id=(params or {}).get("space_id"),
                ttl_seconds=(params or {}).get("ttl_seconds"),
                expires_at=expires_at,
                importance=float((params or {}).get("importance") or 0.5),
                principal_id=(params or {}).get("principal_id"),
                trace_id=(params or {}).get("trace_id"),
                source_ref=(params or {}).get("source_ref"),
                is_archived=False,
                is_deleted=False,
                create_time=current_timestamp(),
                create_date=get_format_time(),
                update_time=current_timestamp(),
                update_date=get_format_time(),
            )

        await cls._ensure_index(tenant_id, len(embedding))
        index_name = cls._index_name(tenant_id)
        await thread_pool_exec(
            settings.docStoreConn.insert,
            [cls._build_index_doc(memory, embedding)],
            index_name,
            None,
        )
        return True, RetCode.SUCCESS, "", cls._memory_to_dict(memory)

    @classmethod
    async def put_items(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        items = (params or {}).get("items") or []
        if not items or not isinstance(items, list):
            return False, RetCode.ARGUMENT_ERROR, "`items` must be a non-empty list.", None

        contents = []
        for item in items:
            content = (item or {}).get("content")
            if not content:
                return False, RetCode.ARGUMENT_ERROR, "Every item must include `content`.", None
            contents.append(content)

        ok, code, message, embeddings = await cls._embed_contents(tenant_id, contents)
        if not ok or not embeddings:
            return False, code, message, None

        with DB.connection_context():
            memories: list[KmMemory] = []
            for item in items:
                memory = KmMemory.create(
                    id=get_uuid(),
                    tenant_id=tenant_id,
                    content=item["content"],
                    content_type=cls._normalize_content_type(item),
                    scope=cls._normalize_scope(item),
                    owner_id=(item or {}).get("owner_id"),
                    space_id=(item or {}).get("space_id"),
                    ttl_seconds=(item or {}).get("ttl_seconds"),
                    expires_at=cls._compute_expiry((item or {}).get("ttl_seconds")),
                    importance=float((item or {}).get("importance") or 0.5),
                    principal_id=(item or {}).get("principal_id"),
                    trace_id=(item or {}).get("trace_id"),
                    source_ref=(item or {}).get("source_ref"),
                    is_archived=False,
                    is_deleted=False,
                    create_time=current_timestamp(),
                    create_date=get_format_time(),
                    update_time=current_timestamp(),
                    update_date=get_format_time(),
                )
                memories.append(memory)

        await cls._ensure_index(tenant_id, len(embeddings[0]))
        index_name = cls._index_name(tenant_id)
        docs = [cls._build_index_doc(memory, embedding) for memory, embedding in zip(memories, embeddings)]
        await thread_pool_exec(settings.docStoreConn.insert, docs, index_name, None)
        return True, RetCode.SUCCESS, "", {
            "items": [cls._memory_to_dict(memory) for memory in memories],
            "total": len(memories),
        }

    @classmethod
    async def search_items(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        query_text = ((params or {}).get("query") or "").strip()
        if not query_text:
            return False, RetCode.ARGUMENT_ERROR, "`query` is required.", None

        mode = str((params or {}).get("mode") or "hybrid").strip().lower()
        top_k = max(1, min(100, int((params or {}).get("top_k") or 10)))
        min_similarity = float((params or {}).get("min_similarity") or 0.1)
        min_match = float((params or {}).get("min_match") or 0.2)
        include_archived = bool((params or {}).get("include_archived", False))
        vector_weight = (params or {}).get("vector_weight")
        keyword_weight = (params or {}).get("keyword_weight")
        if vector_weight is None and keyword_weight is None and (params or {}).get("keywords_similarity_weight") is not None:
            vector_weight = float((params or {}).get("keywords_similarity_weight"))
        if vector_weight is None:
            vector_weight = 0.65 if keyword_weight is None else max(0.0, min(1.0, 1.0 - float(keyword_weight)))
        vector_weight = max(0.0, min(1.0, float(vector_weight)))
        keyword_weight = max(0.0, min(1.0, 1.0 - vector_weight))

        index_name = cls._index_name(tenant_id)
        if not settings.docStoreConn.index_exist(index_name, ""):
            return True, RetCode.SUCCESS, "", {"items": [], "total": 0, "mode": mode}

        filters = cls._query_filters(tenant_id, params)
        order_by = OrderByExpr()
        match_exprs = []

        if mode in {"keyword", "hybrid"}:
            match_text, _ = cls._build_text_query(query_text, min_match)
            if match_text:
                match_exprs.append(match_text)

        if mode in {"vector", "hybrid", "graph_enhanced"}:
            embd_mdl = LLMBundle(tenant_id, LLMType.EMBEDDING)
            qv, _ = await thread_pool_exec(embd_mdl.encode_queries, query_text)
            if isinstance(qv, list) and qv and isinstance(qv[0], list):
                qv = qv[0]
            if not qv:
                return False, RetCode.SERVER_ERROR, "Embedding generation failed.", None
            ok, code, message = cls._ensure_supported_dims(len(qv))
            if not ok:
                return False, code, message, None
            match_exprs.append(
                MatchDenseExpr(
                    vector_column_name=f"q_{len(qv)}_vec",
                    embedding_data=qv,
                    embedding_data_type="float",
                    distance_type="cosine",
                    topn=top_k,
                    extra_options={"similarity": min_similarity},
                )
            )

        if len(match_exprs) == 2:
            match_exprs.append(
                FusionExpr(
                    "weighted_sum",
                    top_k,
                    {"weights": f"{keyword_weight},{vector_weight}"},
                )
            )

        res = await thread_pool_exec(
            settings.docStoreConn.search,
            ["tenant_id", "scope_kwd", "owner_id", "space_id", "content_type_kwd", "importance_flt", "_score"],
            [],
            filters,
            match_exprs,
            order_by,
            0,
            top_k,
            index_name,
            [],
        )

        ids = settings.docStoreConn.get_doc_ids(res) if res else []
        if not ids:
            return True, RetCode.SUCCESS, "", {"items": [], "total": 0, "mode": mode}

        score_fields = settings.docStoreConn.get_fields(res, ["_score"]) if res else {}
        now = datetime.now()
        with DB.connection_context():
            memories = list(
                KmMemory.select().where(
                    KmMemory.tenant_id == tenant_id,
                    KmMemory.id.in_(ids),
                )
            )

            by_id = {memory.id: memory for memory in memories}
            items = []
            for memory_id in ids:
                memory = by_id.get(memory_id)
                if not memory or memory.is_deleted:
                    continue
                if memory.expires_at and memory.expires_at <= now:
                    continue
                if (not include_archived) and memory.is_archived:
                    continue
                memory.access_count = int(memory.access_count or 0) + 1
                memory.last_accessed_at = now
                memory.update_time = current_timestamp()
                memory.update_date = get_format_time()
                memory.save()
                score = None
                try:
                    score = float((score_fields.get(memory_id) or {}).get("_score"))
                except Exception:
                    score = None
                items.append(cls._memory_to_dict(memory, score=score))

        return True, RetCode.SUCCESS, "", {"items": items, "total": len(items), "mode": mode}

    @classmethod
    async def recent_items(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        limit = max(1, min(100, int((params or {}).get("limit") or 20)))
        scope = (params or {}).get("scope")
        owner_id = (params or {}).get("owner_id")
        include_archived = bool((params or {}).get("include_archived", False))
        space_ids = (params or {}).get("space_ids") or []
        if not space_ids and (params or {}).get("space_id"):
            space_ids = [(params or {}).get("space_id")]

        with DB.connection_context():
            query = KmMemory.select().where(
                KmMemory.tenant_id == tenant_id,
                KmMemory.is_deleted == False,
            )
            if scope:
                query = query.where(KmMemory.scope == scope)
            if owner_id:
                query = query.where(KmMemory.owner_id == owner_id)
            if space_ids:
                query = query.where(KmMemory.space_id.in_(list(space_ids)))
            if not include_archived:
                query = query.where(KmMemory.is_archived == False)

            memories = list(query.order_by(KmMemory.create_time.desc()).limit(limit))

        now = datetime.now()
        items = []
        for memory in memories:
            if memory.expires_at and memory.expires_at <= now:
                continue
            items.append(cls._memory_to_dict(memory))
        return True, RetCode.SUCCESS, "", {"items": items, "total": len(items)}

    @classmethod
    async def get_by_ids(cls, tenant_id: str, item_ids: list[str]) -> tuple[bool, int, str, list[dict]]:
        if not item_ids:
            return True, RetCode.SUCCESS, "", []
        with DB.connection_context():
            memories = list(
                KmMemory.select().where(
                    KmMemory.tenant_id == tenant_id,
                    KmMemory.id.in_(item_ids),
                    KmMemory.is_deleted == False,
                )
            )
        by_id = {memory.id: memory for memory in memories}
        ordered = [cls._memory_to_dict(by_id[item_id]) for item_id in item_ids if item_id in by_id]
        return True, RetCode.SUCCESS, "", ordered

    @classmethod
    async def delete_item(cls, tenant_id: str, item_id: str) -> tuple[bool, int, str, dict | None]:
        if not item_id:
            return False, RetCode.ARGUMENT_ERROR, "`item_id` is required.", None

        with DB.connection_context():
            memory = KmMemory.get_or_none(KmMemory.id == item_id, KmMemory.tenant_id == tenant_id)
            if not memory:
                return False, RetCode.DATA_ERROR, "Memory item not found.", None
            memory.is_deleted = True
            memory.update_time = current_timestamp()
            memory.update_date = get_format_time()
            memory.save()

        index_name = cls._index_name(tenant_id)
        if settings.docStoreConn.index_exist(index_name, ""):
            await thread_pool_exec(settings.docStoreConn.delete, {"id": item_id}, index_name, None)

        return True, RetCode.SUCCESS, "", {"id": item_id, "deleted": True}

    @classmethod
    async def upsert(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        return await cls.put_item(tenant_id, params)

    @classmethod
    async def query(cls, tenant_id: str, params: dict) -> tuple[bool, int, str, dict | None]:
        query_params = {"mode": "vector", **(params or {})}
        return await cls.search_items(tenant_id, query_params)

    @classmethod
    async def delete(cls, tenant_id: str, memory_id: str) -> tuple[bool, int, str, dict | None]:
        return await cls.delete_item(tenant_id, memory_id)
