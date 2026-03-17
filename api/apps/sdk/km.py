# KM-CUSTOM

import asyncio
import json
import logging
from datetime import datetime

from quart import Response, request

from api.db.db_models import DB
from api.db.km_models import (
    KmAuditLog,
    KmIngestEvent,
    KmIngestJob,
    KmMemory,
    KmProvenance,
    KmWebhook,
)
from api.km.services.context_pack_service import ContextPackService
from api.km.services.fact_km_service import FactKmService
from api.km.services.graph_km_service import GraphKmService
from api.km.services.graph_maintenance import GraphMaintenanceService
from api.km.services.ingest_service import IngestService
from api.km.services.job_runner import KmJobRunner
from api.km.services.memory_km_service import KmMemoryService
from api.km.services.metrics_service import MetricsService
from api.km.services.space_km_service import SpaceKmService
from api.utils.api_utils import get_request_json, get_result, token_required
from common.constants import RetCode
from common.graph_store import get_graph_store
from common.misc_utils import thread_pool_exec


_km_runner_task = None


# ---------------------------------------------------------------------------
# KM response helper — produces { status, data, meta } envelope
# that the frontend KmResponse<T> interface expects.
# ---------------------------------------------------------------------------


def _km_result(data=None, status="ok", meta=None, code=0, message="success"):
    """Return a KM-specific JSON envelope compatible with the frontend KmResponse<T>."""
    body = {"status": status, "data": data}
    if meta:
        body["meta"] = meta
    # Also include code/message for backward compat with get_result() callers
    body["code"] = code
    body["message"] = message
    return body


def _km_error(message="Unknown error", code=RetCode.SERVER_ERROR, data=None):
    return _km_result(data=data, status="error", code=code, message=message)


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _request_args_to_dict():
    list_like_keys = {"space_ids", "statuses", "modes", "sources", "content_types", "labels"}
    bool_keys = {"include_archived", "include_provenance"}
    params = {}
    for key in request.args.keys():
        values = request.args.getlist(key)
        if len(values) == 1 and key in list_like_keys and "," in values[0]:
            values = [v.strip() for v in values[0].split(",") if v.strip()]
        if len(values) == 1 and key not in list_like_keys:
            value = values[0]
            params[key] = _to_bool(value) if key in bool_keys else value
            continue
        params[key] = values
    return params


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------


@app.before_serving  # noqa: F821
async def _start_km_job_runner():
    global _km_runner_task
    if _km_runner_task is not None:
        return
    _km_runner_task = asyncio.create_task(KmJobRunner.run_forever())
    logging.info("[KM] KmJobRunner task scheduled")


@app.after_serving  # noqa: F821
async def _stop_km_job_runner():
    global _km_runner_task
    if _km_runner_task:
        _km_runner_task.cancel()
        try:
            await _km_runner_task
        except asyncio.CancelledError:
            pass
        _km_runner_task = None
        logging.info("[KM] KmJobRunner stopped")


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


@manager.route("/km/ingest/submit", methods=["POST"])  # noqa: F821
@token_required
async def ingest_submit(tenant_id):
    files = await request.files
    if files and "file" in files:
        form = await request.form
        form_dict = form.to_dict() if form else {}
        res = await IngestService.submit_files(tenant_id, form_dict, files.getlist("file"))
        if not res.ok:
            return _km_error(message=res.message, code=res.code)
        return _km_result(data=res.data)

    params = await get_request_json()
    res = await IngestService.submit(tenant_id, params or {})
    if not res.ok:
        return _km_error(message=res.message, code=res.code)
    return _km_result(data=res.data)


@manager.route("/km/ingest/<job_id>/status", methods=["GET"])  # noqa: F821
@token_required
async def ingest_status(tenant_id, job_id):
    res = await thread_pool_exec(IngestService.status, tenant_id, job_id)
    if not res.ok:
        return _km_error(message=res.message, code=res.code)
    return _km_result(data=res.data)


@manager.route("/km/ingest/<job_id>/cancel", methods=["POST"])  # noqa: F821
@token_required
async def ingest_cancel(tenant_id, job_id):
    res = await thread_pool_exec(IngestService.cancel, tenant_id, job_id)
    if not res.ok:
        return _km_error(message=res.message, code=res.code)
    return _km_result(data=res.data)


@manager.route("/km/ingest/<job_id>/retry", methods=["POST"])  # noqa: F821
@token_required
async def ingest_retry(tenant_id, job_id):
    res = await thread_pool_exec(IngestService.retry, tenant_id, job_id)
    if not res.ok:
        return _km_error(message=res.message, code=res.code)
    return _km_result(data=res.data)


@manager.route("/km/ingest/jobs", methods=["GET"])  # noqa: F821
@token_required
async def ingest_list(tenant_id):
    """List ingest jobs for this tenant (used by pipeline monitor)."""
    state = request.args.get("state")
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 50))

    def _query():
        with DB.connection_context():
            q = KmIngestJob.select().where(KmIngestJob.tenant_id == tenant_id)
            if state and state != "all":
                q = q.where(KmIngestJob.state == state)
            total = q.count()
            jobs = list(
                q.order_by(KmIngestJob.create_time.desc())
                .paginate(page, size)
            )
            return {
                "total": total,
                "jobs": [IngestService._job_to_dict(j) for j in jobs],
            }

    data = await thread_pool_exec(_query)
    return _km_result(data=data)


@manager.route("/km/ingest/<job_id>/events", methods=["GET"])  # noqa: F821
@token_required
async def ingest_events_sse(tenant_id, job_id):
    async def generate():
        last_create_time = 0
        last_ts_ids: set[str] = set()
        terminal = {"completed", "failed", "cancelled", "partial_completed"}
        while True:
            try:
                # Wrap sync Peewee call in thread_pool_exec to avoid blocking event loop
                events = await thread_pool_exec(
                    lambda: list(
                        KmIngestEvent.select()
                        .where(
                            KmIngestEvent.tenant_id == tenant_id,
                            KmIngestEvent.job_id == job_id,
                            KmIngestEvent.create_time >= last_create_time,
                        )
                        .order_by(KmIngestEvent.create_time, KmIngestEvent.id)
                        .limit(200)
                    )
                )
                for evt in events:
                    ts = int(evt.create_time or 0)
                    if ts < last_create_time:
                        continue
                    if ts == last_create_time and evt.id in last_ts_ids:
                        continue
                    if ts > last_create_time:
                        last_create_time = ts
                        last_ts_ids = set()
                    payload = {
                        "id": evt.id,
                        "event_type": evt.event_type,
                        "payload": evt.payload or {},
                        "job_id": evt.job_id,
                        "tenant_id": evt.tenant_id,
                        "create_time": ts,
                    }
                    yield "data:" + json.dumps(payload, ensure_ascii=False) + "\n\n"
                    last_ts_ids.add(evt.id)

                # Check terminal state via thread_pool_exec
                status = await thread_pool_exec(IngestService.status, tenant_id, job_id)
                if status.ok and (status.data or {}).get("state") in terminal:
                    yield "data:[DONE]\n\n"
                    return

                yield ": ping\n\n"
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                return
            except Exception as e:
                err = {"code": RetCode.SERVER_ERROR, "message": repr(e), "data": False}
                yield "data:" + json.dumps(err, ensure_ascii=False) + "\n\n"
                return

    resp = Response(generate(), mimetype="text/event-stream")
    resp.headers.add_header("Cache-control", "no-cache")
    resp.headers.add_header("Connection", "keep-alive")
    resp.headers.add_header("X-Accel-Buffering", "no")
    resp.headers.add_header("Content-Type", "text/event-stream; charset=utf-8")
    return resp


# ---------------------------------------------------------------------------
# Spaces
# ---------------------------------------------------------------------------


@manager.route("/km/spaces", methods=["POST"])  # noqa: F821
@token_required
async def km_space_upsert(tenant_id):
    params = await get_request_json()
    ok, code, message, data = await SpaceKmService.upsert(tenant_id, params or {})
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/spaces", methods=["GET"])  # noqa: F821
@token_required
async def km_space_list(tenant_id):
    ok, code, message, data = await SpaceKmService.list_spaces(tenant_id, _request_args_to_dict())
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/spaces/<space_id>", methods=["GET"])  # noqa: F821
@token_required
async def km_space_get(tenant_id, space_id):
    ok, code, message, data = await SpaceKmService.get(tenant_id, space_id)
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/spaces/<space_id>", methods=["DELETE"])  # noqa: F821
@token_required
async def km_space_delete(tenant_id, space_id):
    ok, code, message, data = await SpaceKmService.delete(tenant_id, space_id)
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


# ---------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------


@manager.route("/km/items", methods=["POST"])  # noqa: F821
@token_required
async def km_item_put(tenant_id):
    params = await get_request_json()
    ok, code, message, data = await KmMemoryService.put_item(tenant_id, params or {})
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/items/batch", methods=["POST"])  # noqa: F821
@token_required
async def km_item_put_batch(tenant_id):
    params = await get_request_json()
    ok, code, message, data = await KmMemoryService.put_items(tenant_id, params or {})
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/items/search", methods=["POST"])  # noqa: F821
@token_required
async def km_item_search(tenant_id):
    params = await get_request_json()
    ok, code, message, data = await KmMemoryService.search_items(tenant_id, params or {})
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/items/recent", methods=["GET"])  # noqa: F821
@token_required
async def km_item_recent(tenant_id):
    ok, code, message, data = await KmMemoryService.recent_items(tenant_id, _request_args_to_dict())
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/items/<item_id>", methods=["DELETE"])  # noqa: F821
@token_required
async def km_item_delete(tenant_id, item_id):
    ok, code, message, data = await KmMemoryService.delete_item(tenant_id, item_id)
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------


@manager.route("/km/facts", methods=["POST"])  # noqa: F821
@token_required
async def km_fact_upsert(tenant_id):
    params = await get_request_json()
    ok, code, message, data = await FactKmService.upsert(tenant_id, params or {})
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/facts/search", methods=["POST"])  # noqa: F821
@token_required
async def km_fact_search(tenant_id):
    params = await get_request_json()
    ok, code, message, data = await FactKmService.search(tenant_id, params or {})
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


@manager.route("/km/facts/<fact_id>", methods=["DELETE"])  # noqa: F821
@token_required
async def km_fact_delete(tenant_id, fact_id):
    ok, code, message, data = await FactKmService.delete(tenant_id, fact_id)
    if not ok:
        return _km_error(message=message, code=code)
    return _km_result(data=data)


# ---------------------------------------------------------------------------
# Context Pack
# ---------------------------------------------------------------------------


@manager.route("/km/context/pack", methods=["POST"])  # noqa: F821
@token_required
async def km_context_pack(tenant_id):
    params = await get_request_json()
    params = params or {}
    space_ids = params.get("space_ids") or []
    if isinstance(space_ids, str):
        space_ids = [sid.strip() for sid in space_ids.split(",") if sid.strip()]
    data = await ContextPackService.build(
        tenant_id,
        query=params.get("query"),
        space_ids=space_ids,
        scope=params.get("scope"),
        top_k_items=int(params.get("top_k_items", 5)),
        top_k_facts=int(params.get("top_k_facts", 5)),
        graph_entity=params.get("graph_entity") or params.get("entity"),
        graph_kb_id=params.get("graph_kb_id") or params.get("kb_id"),
        graph_hops=int(params.get("graph_hops", 2)),
        graph_limit=int(params.get("graph_limit", 10)),
        include_provenance=_to_bool(params.get("include_provenance", True)),
        recent_limit=int(params.get("recent_limit", 5)),
    )
    return _km_result(data=data)


# ---------------------------------------------------------------------------
# Provenance (stub — returns stored records when backend populates them)
# ---------------------------------------------------------------------------


@manager.route("/km/provenance/<artifact_type>/<artifact_id>", methods=["GET"])  # noqa: F821
@token_required
async def provenance_trace(tenant_id, artifact_type, artifact_id):
    def _query():
        with DB.connection_context():
            records = list(
                KmProvenance.select()
                .where(
                    KmProvenance.tenant_id == tenant_id,
                    KmProvenance.artifact_type == artifact_type,
                    KmProvenance.artifact_id == artifact_id,
                )
                .order_by(KmProvenance.create_time.desc())
                .limit(100)
            )
            return [
                {
                    "source_doc_id": r.source_doc_id,
                    "source_doc_name": r.source_doc_name,
                    "source_page": r.source_page,
                    "source_offset": [r.source_offset_start, r.source_offset_end]
                    if r.source_offset_start is not None
                    else None,
                    "confidence": float(r.confidence) if r.confidence else 0,
                    "extraction_method": r.extraction_method,
                    "job_id": r.job_id,
                    "pipeline_version": r.pipeline_version,
                    "created_at": str(r.create_date) if r.create_date else None,
                }
                for r in records
            ]

    data = await thread_pool_exec(_query)
    return _km_result(data=data)


# ---------------------------------------------------------------------------
# Knowledge Search (stub — delegates to existing RAGFlow search)
# ---------------------------------------------------------------------------


@manager.route("/km/knowledge/search", methods=["POST"])  # noqa: F821
@token_required
async def knowledge_search(tenant_id):
    params = await get_request_json()
    # Stub: return empty result until QA service is wired
    return _km_result(data={
        "answer": "",
        "citations": [],
        "confidence": 0,
    })


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------


@manager.route("/km/graph/query", methods=["POST"])  # noqa: F821
@token_required
async def graph_query(tenant_id):
    params = await get_request_json()
    params = params or {}
    operation = str(params.get("operation") or params.get("query_type") or params.get("mode") or "neighbors").strip().lower()
    kb_id = params.get("kb_id")

    if operation in {"neighbors", "entity_neighbors"}:
        entity = params.get("entity") or params.get("name")
        if not kb_id or not entity:
            return _km_error(message="kb_id and entity are required", code=RetCode.ARGUMENT_ERROR)
        data = await GraphKmService.neighbors(
            tenant_id,
            kb_id,
            entity,
            hops=int(params.get("hops", 2)),
            limit=int(params.get("limit", 50)),
        )
        return _km_result(data=data)

    if operation in {"path", "find_path"}:
        source = params.get("source")
        target = params.get("target")
        if not kb_id or not source or not target:
            return _km_error(message="kb_id, source, target are required", code=RetCode.ARGUMENT_ERROR)
        data = await GraphKmService.find_path(
            tenant_id,
            kb_id,
            source,
            target,
            max_hops=int(params.get("max_hops", 4)),
        )
        return _km_result(data=data)

    if operation == "timeline":
        if not kb_id:
            return _km_error(message="kb_id is required", code=RetCode.ARGUMENT_ERROR)
        data = await GraphKmService.timeline(
            tenant_id,
            kb_id,
            limit=int(params.get("limit", 20)),
        )
        return _km_result(data=data)

    if operation == "ontology":
        if not kb_id:
            return _km_error(message="kb_id is required", code=RetCode.ARGUMENT_ERROR)
        data = await thread_pool_exec(
            GraphKmService.list_ontology,
            tenant_id=tenant_id,
            kb_id=kb_id,
            ontology_type=params.get("ontology_type"),
            limit=int(params.get("limit", 500)),
        )
        return _km_result(data={"items": data})

    return _km_error(message=f"Unsupported graph operation: {operation}", code=RetCode.ARGUMENT_ERROR)


@manager.route("/km/graph/context-pack", methods=["POST"])  # noqa: F821
@token_required
async def graph_context_pack(tenant_id):
    params = await get_request_json()
    params = params or {}
    kb_id = params.get("kb_id")
    entity = params.get("entity") or params.get("name")
    if not kb_id or not entity:
        return _km_error(message="kb_id and entity are required", code=RetCode.ARGUMENT_ERROR)
    data = await GraphKmService.neighbors(
        tenant_id,
        kb_id,
        entity,
        hops=int(params.get("hops", 2)),
        limit=int(params.get("limit", 50)),
    )
    return _km_result(data=data)


@manager.route("/km/graph/recompute-pagerank", methods=["POST"])  # noqa: F821
@token_required
async def graph_recompute_pagerank(tenant_id):
    params = await get_request_json()
    kb_id = (params or {}).get("kb_id")
    if not kb_id:
        return _km_error(message="kb_id is required", code=RetCode.ARGUMENT_ERROR)

    result = await GraphMaintenanceService.recompute_pagerank(tenant_id, kb_id)
    return _km_result(data=result)


@manager.route("/km/graph/precompute-nhop", methods=["POST"])  # noqa: F821
@token_required
async def graph_precompute_nhop(tenant_id):
    params = await get_request_json()
    kb_id = (params or {}).get("kb_id")
    if not kb_id:
        return _km_error(message="kb_id is required", code=RetCode.ARGUMENT_ERROR)

    max_entities = int((params or {}).get("max_entities", 500))
    hop = int((params or {}).get("hop", 2))
    result = await GraphMaintenanceService.precompute_nhop(tenant_id, kb_id, max_entities=max_entities, hop=hop)
    return _km_result(data=result)


@manager.route("/km/graph/spaces", methods=["GET"])  # noqa: F821
@token_required
async def graph_spaces(tenant_id):
    graph_store = get_graph_store()
    if not graph_store:
        return _km_result(data={"enabled": False, "spaces": []})

    def _collect():
        spaces = graph_store.list_spaces()
        return [graph_store.space_stats(space) for space in spaces]

    stats = await thread_pool_exec(_collect)
    return _km_result(data={"enabled": True, "spaces": stats})


@manager.route("/km/graph/health", methods=["GET"])  # noqa: F821
@token_required
async def graph_health(tenant_id):
    graph_store = get_graph_store()
    if not graph_store:
        return _km_result(data={"enabled": False, "status": "disabled"})

    data = await thread_pool_exec(graph_store.health_check)
    data["enabled"] = True
    return _km_result(data=data)


# ---------------------------------------------------------------------------
# Ontology
# ---------------------------------------------------------------------------


@manager.route("/km/ontology/<kb_id>/recommend", methods=["GET"])  # noqa: F821
@token_required
async def ontology_recommend(tenant_id, kb_id):
    entity_types = await thread_pool_exec(
        GraphKmService.list_ontology,
        tenant_id=tenant_id,
        kb_id=kb_id,
        ontology_type="entity_type",
        limit=100,
    )
    relation_types = await thread_pool_exec(
        GraphKmService.list_ontology,
        tenant_id=tenant_id,
        kb_id=kb_id,
        ontology_type="relation_type",
        limit=100,
    )
    return _km_result(data={
        "entity_types": entity_types,
        "relation_types": relation_types,
        "reasoning": "Based on current KM ontology definitions." if entity_types or relation_types else "No ontology recommendations available yet.",
    })


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------


@manager.route("/km/feedback", methods=["POST"])  # noqa: F821
@token_required
async def submit_feedback(tenant_id):
    params = await get_request_json()
    # Stub: acknowledge feedback
    return _km_result(data={"status": "received"})


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------


@manager.route("/km/webhook", methods=["POST"])  # noqa: F821
@token_required
async def webhook_register(tenant_id):
    params = await get_request_json()
    if not params or not params.get("url"):
        return _km_error(message="url is required", code=RetCode.ARGUMENT_ERROR)

    from common.misc_utils import get_uuid

    def _create():
        with DB.connection_context():
            wh = KmWebhook.create(
                id=get_uuid(),
                tenant_id=tenant_id,
                url=params["url"],
                event_types=params.get("event_types", []),
                secret=get_uuid(),
            )
            return {
                "id": wh.id,
                "url": wh.url,
                "event_types": wh.event_types or [],
                "is_active": bool(wh.is_active),
                "failure_count": wh.failure_count,
            }

    data = await thread_pool_exec(_create)
    return _km_result(data=data)


@manager.route("/km/webhook/<webhook_id>", methods=["DELETE"])  # noqa: F821
@token_required
async def webhook_unregister(tenant_id, webhook_id):
    def _delete():
        with DB.connection_context():
            rows = KmWebhook.delete().where(
                KmWebhook.id == webhook_id,
                KmWebhook.tenant_id == tenant_id,
            ).execute()
            return rows > 0

    deleted = await thread_pool_exec(_delete)
    if not deleted:
        return _km_error(message="Webhook not found", code=RetCode.DATA_ERROR)
    return _km_result(data=None)


# ---------------------------------------------------------------------------
# Health & Metrics
# ---------------------------------------------------------------------------


@manager.route("/km/health", methods=["GET"])  # noqa: F821
@token_required
async def km_health(tenant_id):
    def _check():
        checks = {}
        # DB check
        try:
            with DB.connection_context():
                KmIngestJob.select().limit(1).count()
            checks["database"] = "ok"
        except Exception as e:
            checks["database"] = f"error: {e!r}"
        # Redis check
        try:
            from rag.utils.redis_conn import REDIS_CONN
            REDIS_CONN.ping()
            checks["redis"] = "ok"
        except Exception as e:
            checks["redis"] = f"error: {e!r}"

        all_ok = all(v == "ok" for v in checks.values())
        return {
            "status": "healthy" if all_ok else "degraded",
            "checks": checks,
            "version": "0.1.0",
        }

    data = await thread_pool_exec(_check)
    return _km_result(data=data)


@manager.route("/km/metrics", methods=["GET"])  # noqa: F821
@token_required
async def km_metrics(tenant_id):
    def _gather():
        with DB.connection_context():
            total_jobs = KmIngestJob.select().where(KmIngestJob.tenant_id == tenant_id).count()
            active_jobs = KmIngestJob.select().where(
                KmIngestJob.tenant_id == tenant_id,
                KmIngestJob.state.in_(["queued", "downloading", "parsing", "chunking", "embedding", "extracting_kg", "resolving_entities", "upserting_graph"]),
            ).count()
            total_memories = KmMemory.select().where(
                KmMemory.tenant_id == tenant_id,
                KmMemory.is_deleted == False,
            ).count()
            total_provenance = KmProvenance.select().where(KmProvenance.tenant_id == tenant_id).count()
            return {
                "total_ingest_jobs": total_jobs,
                "active_jobs": active_jobs,
                "total_memories": total_memories,
                "total_provenance_records": total_provenance,
            }

    data = await thread_pool_exec(_gather)
    return _km_result(data=data)


@manager.route("/km/audit-logs", methods=["GET"])  # noqa: F821
@token_required
async def km_audit_logs(tenant_id):
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 20))

    def _query():
        with DB.connection_context():
            logs = list(
                KmAuditLog.select()
                .where(KmAuditLog.tenant_id == tenant_id)
                .order_by(KmAuditLog.create_time.desc())
                .paginate(page, size)
            )
            return [
                {
                    "id": log.id,
                    "principal_type": log.principal_type,
                    "principal_id": log.principal_id,
                    "action": log.action,
                    "resource_type": log.resource_type,
                    "resource_id": log.resource_id,
                    "trace_id": log.trace_id,
                    "status_code": log.status_code,
                    "latency_ms": log.latency_ms,
                    "created_at": str(log.create_date) if log.create_date else None,
                }
                for log in logs
            ]

    data = await thread_pool_exec(_query)
    return _km_result(data=data)
