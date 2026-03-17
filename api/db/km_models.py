# KM-CUSTOM
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

from __future__ import annotations

from api.db.db_models import DB, DataBaseModel, JSONField
from peewee import BigIntegerField, BooleanField, CharField, DateTimeField, FloatField, IntegerField, TextField


class KmIngestJob(DataBaseModel):
    """Pipeline task master table (Agent-submitted ingest request)."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)

    # Idempotency: (tenant_id, idempotency_key) unique-ish (NULL allowed).
    idempotency_key = CharField(max_length=128, null=True, index=True)

    # Source
    source_type = CharField(max_length=32, null=False)  # file | url | text
    source_url = TextField(null=True)
    source_filename = CharField(max_length=512, null=True)
    content_hash = CharField(max_length=64, null=True, index=True)

    # Target KB
    kb_id = CharField(max_length=32, null=False, index=True)

    # State machine
    state = CharField(max_length=32, null=False, default="queued", index=True)
    progress_percent = IntegerField(default=0)
    error_code = CharField(max_length=64, null=True)
    error_msg = TextField(null=True)
    retryable = BooleanField(default=True)
    retry_count = IntegerField(default=0)
    max_retries = IntegerField(default=3)

    # RAGFlow resources
    document_id = CharField(max_length=32, null=True, index=True)

    # Pipeline configuration (kept flexible)
    pipeline_config = JSONField(default={})

    # Artifacts stats
    chunk_count = IntegerField(default=0)
    entity_count = IntegerField(default=0)
    relation_count = IntegerField(default=0)

    # Agent identity
    principal_id = CharField(max_length=64, null=True, index=True)
    delegator_id = CharField(max_length=64, null=True)
    trace_id = CharField(max_length=64, null=True, index=True)

    # Timing
    started_at = DateTimeField(null=True)
    completed_at = DateTimeField(null=True)
    eta_seconds = IntegerField(null=True)

    class Meta:
        db_table = "km_ingest_job"
        indexes = (
            (("tenant_id", "idempotency_key"), True),
            (("tenant_id", "content_hash"), False),
        )


class KmIngestEvent(DataBaseModel):
    """Outbox events for webhook + SSE."""

    id = CharField(max_length=32, primary_key=True)
    job_id = CharField(max_length=32, null=False, index=True)
    tenant_id = CharField(max_length=32, null=False, index=True)

    event_type = CharField(max_length=64, null=False, index=True)
    payload = JSONField(default={})

    delivered = BooleanField(default=False, index=True)

    class Meta:
        db_table = "km_ingest_event"


class KmProvenance(DataBaseModel):
    """Provenance ledger for chunk/entity/relation/community artifacts."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)

    artifact_type = CharField(max_length=32, null=False, index=True)  # chunk | entity | relation | community
    artifact_id = CharField(max_length=64, null=False, index=True)

    source_doc_id = CharField(max_length=32, null=False, index=True)
    source_doc_name = CharField(max_length=512, null=True)
    source_offset_start = IntegerField(null=True)
    source_offset_end = IntegerField(null=True)
    source_page = IntegerField(null=True)

    job_id = CharField(max_length=32, null=True, index=True)
    extractor_version = CharField(max_length=32, null=True)
    pipeline_version = CharField(max_length=32, null=True)

    confidence = FloatField(default=1.0)
    extraction_method = CharField(max_length=64, null=True)

    class Meta:
        db_table = "km_provenance"
        indexes = (
            (("tenant_id", "artifact_type", "artifact_id"), False),
        )


class KmSpace(DataBaseModel):
    """Logical namespace for memory, facts, graph, and context retrieval."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)

    name = CharField(max_length=255, null=False, index=True)
    scope = CharField(max_length=32, default="personal", index=True)  # session | personal | team | org | project
    owner_id = CharField(max_length=64, null=True, index=True)

    project_id = CharField(max_length=64, null=True, index=True)
    agent_id = CharField(max_length=64, null=True, index=True)
    session_id = CharField(max_length=64, null=True, index=True)

    description = TextField(null=True)
    labels = JSONField(default=[])
    memory_profile_id = CharField(max_length=64, null=True, index=True)

    is_archived = BooleanField(default=False, index=True)
    is_deleted = BooleanField(default=False, index=True)

    class Meta:
        db_table = "km_space"
        indexes = (
            (("tenant_id", "name"), False),
            (("tenant_id", "scope", "owner_id"), False),
        )


class KmSpaceProfile(DataBaseModel):
    """Profile/config storage for the memory UI backed by KM spaces."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)
    space_id = CharField(max_length=32, null=False, index=True)
    config = JSONField(default={})

    class Meta:
        db_table = "km_space_profile"
        indexes = (
            (("tenant_id", "space_id"), True),
        )


class KmMemory(DataBaseModel):
    """Agent-facing long-term memory item."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)

    content = TextField(null=False)
    content_type = CharField(max_length=32, default="text")

    # Layering decided by caller
    scope = CharField(max_length=32, default="personal", index=True)  # session | personal | team | org
    owner_id = CharField(max_length=64, null=True, index=True)
    space_id = CharField(max_length=64, null=True, index=True)

    ttl_seconds = IntegerField(null=True)
    expires_at = DateTimeField(null=True, index=True)

    importance = FloatField(default=0.5)
    access_count = IntegerField(default=0)
    last_accessed_at = DateTimeField(null=True)

    embedding_id = CharField(max_length=64, null=True)

    principal_id = CharField(max_length=64, null=True, index=True)
    trace_id = CharField(max_length=64, null=True)
    source_ref = TextField(null=True)

    is_archived = BooleanField(default=False, index=True)
    is_deleted = BooleanField(default=False, index=True)

    class Meta:
        db_table = "km_memory"
        indexes = (
            (("tenant_id", "scope", "owner_id"), False),
        )


class KmOntology(DataBaseModel):
    """Ontology definition for entity/relation types."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)
    kb_id = CharField(max_length=32, null=False, index=True)

    ontology_type = CharField(max_length=32, null=False)  # entity_type | relation_type
    name = CharField(max_length=128, null=False)
    description = TextField(null=True)

    source = CharField(max_length=32, default="auto")  # auto | template | manual
    instance_count = IntegerField(default=0)
    is_active = BooleanField(default=True)

    class Meta:
        db_table = "km_ontology"
        indexes = (
            (("tenant_id", "kb_id", "ontology_type", "name"), True),
        )


class KmWebhook(DataBaseModel):
    """Webhook subscriptions."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, null=False, index=True)

    url = CharField(max_length=2048, null=False)
    secret = CharField(max_length=128, null=True)
    event_types = JSONField(default=[])

    is_active = BooleanField(default=True)
    failure_count = IntegerField(default=0)
    last_failure_at = DateTimeField(null=True)

    class Meta:
        db_table = "km_webhook"


class KmFact(DataBaseModel):
    """Versioned knowledge facts."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, index=True)
    kb_id = CharField(max_length=32, index=True)

    subject = CharField(max_length=256, index=True)
    predicate = CharField(max_length=128, index=True)
    object = CharField(max_length=256, index=True)

    confidence = FloatField(default=0.8)
    valid_from = DateTimeField(null=True)
    valid_to = DateTimeField(null=True)

    source_doc_id = CharField(max_length=32, null=True)
    observed_at = DateTimeField()
    version = IntegerField(default=1)

    status = CharField(max_length=32, default="active")  # active | superseded | conflicted | retracted

    class Meta:
        db_table = "km_fact"
        indexes = (
            (("tenant_id", "kb_id", "subject", "predicate", "object"), False),
        )


class KmFeedback(DataBaseModel):
    """Feedback record."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, index=True)
    artifact_type = CharField(max_length=32, index=True)
    artifact_id = CharField(max_length=64, index=True)
    feedback_type = CharField(max_length=32)  # positive | negative | correction
    correction_value = TextField(null=True)
    principal_id = CharField(max_length=64, null=True)
    trace_id = CharField(max_length=64, null=True)

    class Meta:
        db_table = "km_feedback"


class KmQuota(DataBaseModel):
    """Tenant/Agent quotas."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, index=True)

    max_documents = IntegerField(default=10000)
    max_storage_bytes = BigIntegerField(default=10 * 1024**3)
    max_memories = IntegerField(default=100000)

    max_ingest_per_hour = IntegerField(default=100)
    max_query_per_minute = IntegerField(default=60)
    max_concurrent_jobs = IntegerField(default=5)

    current_documents = IntegerField(default=0)
    current_storage_bytes = BigIntegerField(default=0)
    current_memories = IntegerField(default=0)

    class Meta:
        db_table = "km_quota"


class KmAuditLog(DataBaseModel):
    """Audit logs for KM API calls."""

    id = CharField(max_length=32, primary_key=True)
    tenant_id = CharField(max_length=32, index=True)

    principal_type = CharField(max_length=32)
    principal_id = CharField(max_length=64, index=True)
    delegator_id = CharField(max_length=64, null=True, index=True)

    action = CharField(max_length=128, index=True)
    resource_type = CharField(max_length=64, null=True)
    resource_id = CharField(max_length=64, null=True)

    trace_id = CharField(max_length=64, null=True, index=True)
    ip_address = CharField(max_length=64, null=True)
    user_agent = CharField(max_length=256, null=True)

    status_code = IntegerField(default=200)
    error_msg = TextField(null=True)
    latency_ms = IntegerField(default=0)

    class Meta:
        db_table = "km_audit_log"


KM_TABLES = [
    KmIngestJob,
    KmIngestEvent,
    KmProvenance,
    KmSpace,
    KmSpaceProfile,
    KmMemory,
    KmOntology,
    KmWebhook,
    KmFact,
    KmFeedback,
    KmQuota,
    KmAuditLog,
]


@DB.connection_context()
@DB.lock("init_km_tables", 60)
def init_km_tables():
    DB.create_tables(KM_TABLES, safe=True)
