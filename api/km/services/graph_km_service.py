# KM-CUSTOM

from __future__ import annotations

import logging
from collections import deque
from typing import Any

from api.db.db_models import DB
from api.db.km_models import KmOntology
from api.db.services.knowledgebase_service import KnowledgebaseService
from api.db.services.llm_service import LLMBundle
from common import settings
from common.constants import LLMType
from common.graph_store import get_graph_store
from common.misc_utils import get_uuid, thread_pool_exec
from rag.graphrag import search as kg_search
from rag.nlp import search as rag_search


logger = logging.getLogger(__name__)


class GraphKmService:
    """
    KM helper that builds on NebulaGraph/KGSearch for entity and graph tooling.
    """

    @classmethod
    def upsert_ontology_ignore(
        cls,
        *,
        tenant_id: str,
        kb_id: str,
        ontology_type: str,
        name: str,
        description: str | None = None,
        source: str = "auto",
    ) -> dict[str, Any]:
        if not all([tenant_id, kb_id, ontology_type, name]):
            return {"ok": False, "message": "tenant_id/kb_id/ontology_type/name are required."}

        with DB.connection_context():
            KmOntology.insert(
                id=get_uuid(),
                tenant_id=tenant_id,
                kb_id=kb_id,
                ontology_type=ontology_type,
                name=name,
                description=description,
                source=source,
                instance_count=0,
                is_active=True,
            ).on_conflict(action="IGNORE").execute()

            obj = KmOntology.get_or_none(
                KmOntology.tenant_id == tenant_id,
                KmOntology.kb_id == kb_id,
                KmOntology.ontology_type == ontology_type,
                KmOntology.name == name,
            )
            return {"ok": True, "data": obj.to_dict() if obj else None}

    @classmethod
    def list_ontology(cls, *, tenant_id: str, kb_id: str, ontology_type: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        with DB.connection_context():
            qs = KmOntology.select().where(KmOntology.tenant_id == tenant_id, KmOntology.kb_id == kb_id)
            if ontology_type:
                qs = qs.where(KmOntology.ontology_type == ontology_type)
            qs = qs.order_by(KmOntology.create_time.desc()).limit(limit)
            return [o.to_dict() for o in qs]

    @classmethod
    async def neighbors(cls, tenant_id: str, kb_id: str, entity: str, hops: int = 2, limit: int = 50) -> dict[str, Any]:
        graph_store = get_graph_store()
        if graph_store:
            space = graph_store.space_name(tenant_id, kb_id)
            try:
                nodes = await thread_pool_exec(graph_store.k_hop_neighbors, space, entity, hops, limit)
                return {"nodes": nodes, "source": "graph_store"}
            except Exception as exc:
                logger.warning("[GraphKm] graph store neighbors failed: %s", exc)
        fallback = await cls._fallback_neighbors(tenant_id, kb_id, entity, limit)
        return {"nodes": fallback, "source": "kg_search"}

    @classmethod
    async def find_path(
        cls,
        tenant_id: str,
        kb_id: str,
        source: str,
        target: str,
        max_hops: int = 4,
    ) -> dict[str, Any]:
        graph_store = get_graph_store()
        if graph_store:
            space = graph_store.space_name(tenant_id, kb_id)
            path = await cls._bfs_path(space, source, target, graph_store, max_hops)
            if path:
                return {"paths": [path], "source": "graph_store"}
        paths = await cls._fallback_paths(tenant_id, kb_id, source, target)
        return {"paths": paths, "source": "kg_search"}

    @classmethod
    async def timeline(cls, tenant_id: str, kb_id: str, limit: int = 20) -> dict[str, Any]:
        graph_store = get_graph_store()
        if graph_store:
            space = graph_store.space_name(tenant_id, kb_id)
            try:
                nodes = await thread_pool_exec(graph_store.get_all_nodes, space)
                edges = await thread_pool_exec(graph_store.get_all_edges, space)
                nodes = sorted(nodes, key=lambda n: float(n.get("pagerank", 0.0)), reverse=True)[:limit]
                return {"nodes": nodes, "edges": edges[:limit], "source": "graph_store"}
            except Exception as exc:
                logger.warning("[GraphKm] timeline graph store failed: %s", exc)
        fallback = await cls._fallback_timeline(tenant_id, kb_id, limit)
        return {"nodes": fallback.get("nodes", []), "edges": fallback.get("edges", []), "source": "kg_search"}

    @classmethod
    async def _bfs_path(cls, space: str, source: str, target: str, graph_store, max_hops: int) -> list[str] | None:
        queue = deque([(source, [source])])
        visited = {source}
        while queue:
            current, path = queue.popleft()
            if len(path) - 1 >= max_hops:
                continue
            neighbors = await thread_pool_exec(graph_store.k_hop_neighbors, space, current, 1, 50)
            for neighbor in neighbors:
                name = neighbor.get("name")
                if not name or name in visited:
                    continue
                if name == target:
                    return path + [name]
                visited.add(name)
                queue.append((name, path + [name]))
        return None

    @classmethod
    async def _fallback_neighbors(cls, tenant_id: str, kb_id: str, entity: str, limit: int) -> list[dict[str, Any]]:
        kb = cls._fetch_kb(kb_id)
        if not kb:
            return []
        embd = LLMBundle(tenant_id, LLMType.EMBEDDING, llm_name=kb.embd_id)
        kg = kg_search.KGSearch(settings.docStoreConn)
        ents = kg.get_relevant_ents_by_keywords(
            [entity],
            {"kb_ids": [kb_id]},
            [rag_search.index_name(tenant_id)],
            [kb_id],
            embd,
            N=limit,
        )
        return [{"name": name, "pagerank": info.get("pagerank", 0.0), "sim": info.get("sim", 0.0)} for name, info in ents.items()]

    @classmethod
    async def _fallback_paths(cls, tenant_id: str, kb_id: str, source: str, target: str) -> list[list[str]]:
        neighbors = await cls._fallback_neighbors(tenant_id, kb_id, source, 20)
        exists = any(node.get("name") == target for node in neighbors)
        return [[source, target]] if exists else []

    @classmethod
    async def _fallback_timeline(cls, tenant_id: str, kb_id: str, limit: int) -> dict[str, Any]:
        kb = cls._fetch_kb(kb_id)
        if not kb:
            return {"nodes": [], "edges": []}
        embd = LLMBundle(tenant_id, LLMType.EMBEDDING, llm_name=kb.embd_id)
        kg = kg_search.KGSearch(settings.docStoreConn)
        ents = kg.get_relevant_ents_by_types([], {"kb_ids": [kb_id]}, [rag_search.index_name(tenant_id)], [kb_id], N=limit)
        nodes = [
            {"name": name, "pagerank": info.get("pagerank", 0.0), "sim": info.get("sim", 0.0)}
            for name, info in ents.items()
        ]
        return {"nodes": nodes, "edges": []}

    @classmethod
    def _fetch_kb(cls, kb_id: str):
        ok, kb = KnowledgebaseService.get_by_id(kb_id)
        return kb if ok else None
