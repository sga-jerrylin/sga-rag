# KM-CUSTOM

from __future__ import annotations

from typing import Any

from api.db.km_models import KmFact, KmProvenance
from api.km.services.graph_km_service import GraphKmService
from api.km.services.memory_km_service import KmMemoryService
from common.misc_utils import thread_pool_exec


class ContextPackService:
    """Builds a unified context package out of KM items, facts, graph, and provenance."""

    @classmethod
    async def build(
        cls,
        tenant_id: str,
        *,
        query: str | None = None,
        space_ids: list[str] | None = None,
        scope: str | None = None,
        top_k_items: int = 5,
        top_k_facts: int = 5,
        graph_entity: str | None = None,
        graph_kb_id: str | None = None,
        graph_hops: int = 2,
        graph_limit: int = 10,
        include_provenance: bool = True,
        recent_limit: int = 5,
    ) -> dict[str, Any]:
        items = await cls._search_items(
            tenant_id,
            query=query,
            space_ids=space_ids,
            scope=scope,
            top_k=top_k_items,
        )
        recent = await cls._recent_items(
            tenant_id,
            space_ids=space_ids,
            scope=scope,
            limit=recent_limit,
        )
        facts = await cls._fact_snapshot(tenant_id, space_ids, limit=top_k_facts)
        graph = {}
        if graph_entity and graph_kb_id:
            graph = await GraphKmService.neighbors(tenant_id, graph_kb_id, graph_entity, hops=graph_hops, limit=graph_limit)
        provenance = []
        if include_provenance and items:
            provenance = await cls._fetch_provenance(tenant_id, [item["id"] for item in items])
        return {
            "items": items,
            "recent": recent,
            "facts": facts,
            "graph": graph,
            "provenance": provenance,
            "rendered_context": cls._render_context(items, recent, facts),
        }

    @classmethod
    async def _search_items(
        cls,
        tenant_id: str,
        *,
        query: str | None,
        space_ids: list[str] | None,
        scope: str | None,
        top_k: int,
    ) -> list[dict[str, Any]]:
        if not query and not space_ids:
            return []
        params = {
            "query": query or "",
            "scope": scope,
            "space_ids": space_ids or [],
            "top_k": top_k,
            "min_similarity": 0.2,
            "mode": "hybrid",
        }
        ok, code, message, data = await KmMemoryService.search_items(tenant_id, params)
        if not ok or not data:
            return []
        return [cls._format_item(item) for item in data.get("items", [])]

    @classmethod
    async def _recent_items(
        cls,
        tenant_id: str,
        *,
        space_ids: list[str] | None,
        scope: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        params = {
            "scope": scope,
            "space_ids": space_ids or [],
            "limit": limit,
        }
        ok, code, message, data = await KmMemoryService.recent_items(tenant_id, params)
        if not ok or not data:
            return []
        return [cls._format_item(item) for item in data.get("items", [])]

    @staticmethod
    def _format_item(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": item.get("id"),
            "space_id": item.get("space_id"),
            "content": item.get("content"),
            "kind": item.get("content_type"),
            "source_ref": item.get("source_ref"),
            "importance": item.get("importance"),
            "trace_id": item.get("trace_id"),
        }

    @classmethod
    async def _fact_snapshot(cls, tenant_id: str, space_ids: list[str] | None, limit: int) -> list[dict[str, Any]]:
        def _query():
            qs = KmFact.select().where(KmFact.tenant_id == tenant_id)
            if space_ids:
                qs = qs.where(KmFact.kb_id << space_ids)
            qs = qs.order_by(KmFact.create_time.desc()).limit(limit)
            return [cls._fact_to_dict(fact) for fact in qs]

        return await thread_pool_exec(_query)

    @staticmethod
    def _fact_to_dict(fact: KmFact) -> dict[str, Any]:
        return {
            "id": fact.id,
            "space_id": fact.kb_id,
            "subject": fact.subject,
            "predicate": fact.predicate,
            "object": fact.object,
            "confidence": fact.confidence,
            "valid_from": fact.valid_from.isoformat() if fact.valid_from else None,
            "valid_to": fact.valid_to.isoformat() if fact.valid_to else None,
            "status": fact.status,
        }

    @classmethod
    async def _fetch_provenance(cls, tenant_id: str, artifact_ids: list[str]) -> list[dict[str, Any]]:
        if not artifact_ids:
            return []

        def _query():
            qs = (
                KmProvenance.select()
                .where(
                    KmProvenance.tenant_id == tenant_id,
                    KmProvenance.artifact_id << artifact_ids,
                )
                .order_by(KmProvenance.create_time.desc())
                .limit(50)
            )
            return [
                {
                    "artifact_id": rec.artifact_id,
                    "artifact_type": rec.artifact_type,
                    "source_doc_id": rec.source_doc_id,
                    "confidence": rec.confidence,
                    "extraction_method": rec.extraction_method,
                }
                for rec in qs
            ]

        return await thread_pool_exec(_query)

    @staticmethod
    def _render_context(items: list[dict[str, Any]], recent: list[dict[str, Any]], facts: list[dict[str, Any]]) -> str:
        sections: list[str] = []
        if items:
            sections.append("Relevant items:\n" + "\n".join(item["content"] for item in items if item.get("content")))
        if recent:
            sections.append("Recent items:\n" + "\n".join(item["content"] for item in recent if item.get("content")))
        if facts:
            fact_lines = []
            for fact in facts:
                if not all([fact.get("subject"), fact.get("predicate"), fact.get("object")]):
                    continue
                fact_lines.append(f'{fact["subject"]} {fact["predicate"]} {fact["object"]}')
            if fact_lines:
                sections.append("Facts:\n" + "\n".join(fact_lines))
        return "\n\n".join(sections)
