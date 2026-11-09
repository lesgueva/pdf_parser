# PDF Parser Service — Design Document

| | |
|---|---|
| **Status** | Draft v2 (parsing and OCR simplified to PyMuPDF only, whole-document parsing, URL sources removed) |
| **Date** | 2026-11-09 |
| **Author** | Luis |
| **Scope** | Async PDF parsing service (API + queue + workers), AWS implementation, Python client SDK |

---

## 1. Overview

Clients submit one PDF per request. Parsing runs asynchronously, and clients retrieve the result by polling or are told via an optional webhook. The service has three parts: an **HTTP API**, a **queue**, and **background workers**. It follows **ports and adapters (hexagonal) architecture**, so the domain logic doesn't depend on any infrastructure. The first infrastructure implementation targets **AWS**.

A **Python client SDK** is part of the project. It is modeled on the developer experience of the Azure Document Intelligence SDK (`azure-ai-documentintelligence`): a client object, a credential, a `begin_*` long-running operation that returns a poller, and typed result models.

### 1.1 Goals

- Async, document-at-a-time PDF parsing with an Azure-style long-running-operation (LRO) API.
- Extract **text + layout**, **tables**, and a **Markdown** rendering suitable for LLM/RAG pipelines.
- **Hybrid parsing**: native text extraction for born-digital pages, OCR for scanned pages, decided automatically **per page** (and per region within a page) by PyMuPDF.
- Clean hexagonal core: swapping infrastructure (e.g. another cloud, Textract instead of OSS OCR) means writing new adapters, not changing domain code.
- Handle "medium" scale: thousands to tens of thousands of documents per day, including documents with 500+ pages.
- A first-class Python SDK with sync and async clients.

### 1.2 Non-goals (MVP)

- **Batch submission.** Each operation covers exactly one document. Clients that need several documents submit several operations.
- **Key-value pair extraction** (form fields, `Label: value` pairs).
- **Rate limiting** of any kind.
- **Per-tenant concurrency limits or quotas.** The queue is shared, and one tenant can fill it. This is accepted for the MVP.
- **Password-protected PDFs.** PDFs that need a password to open fail fast with `EncryptedPdf`. PDFs encrypted only with an owner password (they open without one, e.g. with copy or print restrictions) are parsed normally.
- **Fetching documents from URLs.** The service never downloads a PDF. Getting the bytes is the client's responsibility: it sends them inline or through a presigned upload.
- Schema-driven / custom-model extraction (e.g. "invoice → typed JSON").
- Splitting a single document across workers (page-range fan-out).
- GPU-accelerated OCR.
- Idempotency keys and content-hash result caching.
- Self-service tenant portal.
- Long-term storage of inputs or results.

---

## 2. Decision Summary

| Area | Decision | Notes |
|---|---|---|
| Unit of work | **One document per operation** | No batch concept on server or SDK |
| Output | Text + layout blocks always; tables and Markdown on request | **No key-value extraction**; no schema extraction |
| Parsing engine | **PyMuPDF 2.x only** (`pymupdf`, formerly PyMuPDF4LLM) | One library for text, layout, tables, Markdown and OCR. PyMuPDF decides per page/region whether OCR is needed (§5) |
| OCR engine | **PyMuPDF built-in hybrid OCR with RapidOCR** (ONNX Runtime, CPU) | pip-installable, no system Tesseract. Tesseract stays possible through `ocr_function` |
| Language | **Python 3.13 everywhere** | Service, worker, SDK, admin CLI |
| CPU architecture | **ARM64 (linux/arm64, Graviton)** for all containers and Lambda functions | PyMuPDF and ONNX Runtime ship Linux ARM64 wheels. Runs natively on Apple Silicon |
| License | **PyMuPDF is AGPL-3.0 or Artifex commercial** | Applies to network use. Decide before production (§17, R11) |
| Tooling | `uv`, `ruff`, `mypy`, `pytest`, `import-linter` | |
| Result delivery | **Polling + optional webhook** | 202 + `Operation-Location`; per-request `callbackUrl` |
| Upload methods | **Inline body or presigned S3 upload** | SDK picks automatically. No URL sources: the client provides the bytes |
| Large documents | **One worker per document** | No fan-out; long-running containers |
| Worker compute | **ECS Fargate** (on-demand only), autoscaled on SQS depth | CPU only; no Fargate Spot |
| Worker termination safety | **Hard requirement: a worker is never stopped mid-job** by scale-in or deployments | ECS task scale-in protection + visibility heartbeat + graceful shutdown (§5.3) |
| Limits | 100 MB inline, 500 MB presigned, 2,000 analyzed pages, 150 min per job | Enforced per request (§6.4) |
| Password-protected PDFs | **Fail fast** with `EncryptedPdf` | Not retried. Owner-password-only PDFs are parsed |
| API hosting | **ALB + FastAPI on ECS Fargate** | No API Gateway payload cap |
| Queue | **Amazon SQS** + DLQ | |
| State store | **DynamoDB** | Operations + tenants/keys; native TTL |
| Blob storage | **Amazon S3** | Inputs + results; lifecycle expiry |
| Auth | **API keys per tenant** | Hashed at rest; two active keys for rotation |
| Tenant mgmt | **Admin CLI** | Writes directly to DynamoDB |
| Rate limits / quotas | **None in MVP** | No rate limiting, no concurrency caps, no monthly quotas |
| Webhooks | **Per-request URL, HMAC-signed, retried** | Small payload (no result body); delayed retries by a Lambda function on an SQS queue (§9) |
| Failures | **Retry N times with backoff, then fail** | Permanent errors fail fast. The DLQ is for alerting and inspection only (no redrive); clients resubmit (§12) |
| Retention | **24h, logically enforced by the API** | S3 lifecycle deletes document data; DynamoDB TTL cleans up metadata (§13) |
| Observability | Structured JSON logs, CloudWatch EMF metrics, OpenTelemetry traces | §16 |
| Result model | **Simpler custom schema** | Per-page blocks; no span linking |
| API versioning | **Path-based `/v1`** | |
| Idempotency/dedup | **None in MVP** | |
| Repo / SDK | **Monorepo, handwritten SDK** | `uv` workspace; contract tests against OpenAPI |
| IaC | **Terraform** | |
| Local dev | **docker-compose + LocalStack** | In-memory fakes used as unit-test doubles only |

---

## 3. Architecture

### 3.1 High-level view

```
                    ┌───────────────────────────────────────────────────────────┐
                    │                         AWS                               │
                    │                                                           │
 ┌──────────┐ HTTPS │            ┌─────┐   ┌──────────────────┐                 │
 │  Client  │───────┼───────────▶│ ALB │──▶│ API (FastAPI)    │                 │
 │  (SDK)   │       │            └─────┘   │ ECS Fargate      │                 │
 └──────────┘       │                      └──┬──────┬──────┬─┘                 │
      ▲  │          │                         │      │      │ send job          │
      │  │ presigned│                         │      │      ▼                   │
      │  │ PUT      │                         │      │   ┌──────┐   ┌─────┐     │
      │  └──────────┼────────────┐            │      │   │ SQS  │──▶│ DLQ │     │
      │             │            ▼            │      │   └──┬───┘   └─────┘     │
      │             │   ┌────────────────┐    │      │      │ receive           │
      │             │   │ S3             │◀───┘      │      ▼                   │
      │             │   │ inputs/results │◀──────────┼──┌──────────────────┐    │
      │             │   │ (24h lifecycle)│           │  │ Worker           │    │
      │             │   └────────────────┘           │  │ ECS Fargate (CPU)│    │
      │             │                                │  │ parse (PyMuPDF)  │    │
      │             │   ┌────────────────┐           │  │ → store result   │    │
      │             │   │ DynamoDB       │◀──────────┴──┤                  │    │
      │             │   │ operations,    │              └────────┬─────────┘    │
      │             │   │ tenants (TTL)  │                       │              │
      │             │   └────────────────┘                       │              │
      │             └────────────────────────────────────────────┼──────────────┘
      │                     webhook (HMAC-signed, retried)       │
      └──────────────────────────────────────────────────────────┘
```

### 3.2 Request flow (happy path)

1. The client calls `begin_analyze(...)` in the SDK.
2. The SDK sends the document:
   - **Small file (≤ inline threshold):** `POST /v1/documents:analyze` with an `application/pdf` body.
   - **Large file:** `POST /v1/uploads` returns a presigned S3 PUT URL and an `uploadId`. The SDK PUTs the bytes to S3, then calls `POST /v1/documents:analyze` with `{ "uploadId": ... }`.
3. The API authenticates the API key, validates the request, stores the PDF in S3 (inline case), creates an `Operation` in DynamoDB with status `notStarted`, sends a job message to SQS, and returns **`202 Accepted`** with an `Operation-Location: /v1/operations/{operationId}` header.
4. A worker receives the message, immediately turns on ECS task scale-in protection, sets the operation to `running`, and starts a visibility-timeout heartbeat (§5.3).
5. The worker downloads the PDF from S3 to `/tmp` and hands the whole PDF to PyMuPDF in one call, in a child process (§5.1). PyMuPDF runs OCR only where a page lacks usable text. The worker builds the result, including Markdown if requested, and writes the result JSON to S3.
6. The worker sets the operation to `succeeded` with a pointer to the result.
7. If a `callbackUrl` was provided, the worker sends a signed webhook, or queues it for retry (§9).
8. Only then does the worker delete the SQS message and turn off scale-in protection. If it crashes before this point, the message is delivered again and the next worker only redoes the webhook step (§12).
9. The SDK poller calls `GET /v1/operations/{id}` until the status is terminal and returns the typed `AnalyzeResult`.

### 3.3 Operation lifecycle

```
            submit
              │
              ▼
        ┌────────────┐  worker picks up  ┌──────────┐   success   ┌───────────┐
        │ notStarted │──────────────────▶│ running  │────────────▶│ succeeded │
        └────────────┘                   └────┬─────┘             └───────────┘
              ▲                               │
              │  transient error,             │ permanent error, or
              │  attempts < N                 │ attempts exhausted
              └───────────────────────────────┤
                                              ▼
                                         ┌──────────┐
                                         │  failed  │
                                         └──────────┘

   Any state ──(expiresAt passed)──▶ treated as not found (404)
```

- `succeeded` and `failed` are terminal.
- On a transient error the operation goes back to `notStarted` until SQS redelivers the message. `attempt` follows the SQS receive count (§12).
- A `running` operation can also be taken over by a new delivery after its worker crashed (§12).
- Status transitions use DynamoDB **conditional writes** so a duplicate SQS delivery can't move a terminal operation back to an earlier state.

---

## 4. Hexagonal Design

### 4.1 Layers

```
            Driving adapters                        Driven adapters
   ┌─────────────────────────────┐        ┌──────────────────────────────────┐
   │ FastAPI HTTP routes         │        │ S3DocumentStorage                │
   │ SQS worker loop             │        │ SqsJobQueue                      │
   │ Admin CLI, sweeper          │        │ SqsWebhookRetryQueue             │
   │ Webhook-retry Lambda        │        │ DynamoOperationRepository        │
   └──────────────┬──────────────┘        │ DynamoTenantRepository           │
                  │                       │ PyMuPdfDocumentParser            │
                  ▼                       │ HttpxWebhookNotifier             │
   ┌─────────────────────────────┐        │ EcsScaleInProtection             │
   │ Application (use cases)     │───────▶│ SystemClock / UlidGenerator      │
   │  AnalyzeDocument            │ ports  └──────────────────────────────────┘
   │  CreateUpload               │
   │  GetOperation               │
   │  ProcessJob                 │
   │  DeliverWebhook             │
   │  ManageTenants              │
   │  SweepStuckOperations       │
   ├─────────────────────────────┤
   │ Domain                      │
   │  Operation, Tenant, ApiKey, │
   │  Document, AnalyzeResult,   │
   │  errors                     │
   └─────────────────────────────┘
```

**Dependency rule:** domain ← application ← adapters. The domain and application packages never import `boto3`, `fastapi`, `httpx`, `pymupdf` or OCR libraries.

All parsing sits behind a single `DocumentParser` port. Swapping the engine (e.g. Textract) means writing one new adapter.

### 4.2 Driving ports (use cases)

| Use case | Called by | Responsibility |
|---|---|---|
| `AnalyzeDocument` | API | Validate request, persist input (if inline) or check the `uploadId` exists for this tenant and is within the size limit, create operation, enqueue job |
| `CreateUpload` | API | Generate `uploadId` and presigned PUT URL |
| `GetOperation` | API | Return operation status + result (enforces tenant ownership and expiry) |
| `ProcessJob` | Worker | Run the full parse pipeline for one job |
| `DeliverWebhook` | Webhook-retry Lambda | Send one queued webhook (§9); a failure tells the Lambda to retry it later |
| `SweepStuckOperations` | Sweeper (scheduled) | Fail operations stuck in `running` and queue their `operation.failed` webhook (§12) |
| `ManageTenants` | Admin CLI | Create tenant, issue/rotate/revoke keys, set webhook secret, override file-size limits, lower the page limit (§6.4) |

### 4.3 Driven ports (sketch)

```python
from typing import BinaryIO, Protocol
from datetime import datetime, timedelta
from pathlib import Path

class DocumentStorage(Protocol):
    def put_input(self, operation_id: str, body: BinaryIO) -> str: ...       # streamed, never fully buffered (R9); returns key
    def download_input(self, key: str, dest: Path) -> None: ...              # streamed straight to a file in /tmp
    def presign_upload(self, tenant_id: str, upload_id: str, content_length: int, expires: timedelta) -> str: ...
    def upload_size(self, tenant_id: str, upload_id: str) -> int | None: ... # HeadObject on uploads/{tenantId}/{uploadId}; None if missing
    def put_result(self, operation_id: str, result_json: bytes) -> str: ...
    def get_result(self, key: str) -> bytes: ...

class JobQueue(Protocol):
    def send(self, job: "Job") -> None: ...
    def receive(self, wait_seconds: int) -> "ReceivedJob | None": ...
    def ack(self, receipt: str) -> None: ...                                  # delete message
    def extend_visibility(self, receipt: str, seconds: int) -> None: ...      # heartbeat
    def release(self, receipt: str, delay_seconds: int) -> None: ...          # retry w/ backoff

class OperationRepository(Protocol):
    def create(self, op: "Operation") -> None: ...
    def get(self, operation_id: str) -> "Operation | None": ...
    def transition(self, operation_id: str, expected: set["Status"], new: "Status", **fields) -> bool: ...  # conditional write

class TenantRepository(Protocol):
    def find_by_key_hash(self, key_hash: str) -> "Tenant | None": ...
    def save(self, tenant: "Tenant") -> None: ...

class DocumentParser(Protocol):
    def parse(self, pdf: Path, options: "ParseOptions", timeout: timedelta) -> "AnalyzeResult": ...
    # One call for the whole document (or the requested `pages` subset). Never called per page.
    # Opens, validates and parses in one child process (§5.1). Raises InvalidPdf, EncryptedPdf,
    # PageLimitExceeded (carries document_page_count), ParseTimeout or DocumentTooComplex;
    # any other failure of the child raises ParserError.
    # ParseOptions: features, include_markdown, pages, max_pages.
    # AnalyzeResult: document_page_count, per-page blocks and tables, document Markdown

class WebhookNotifier(Protocol):
    def notify(self, url: str, secret: str, event: "WebhookEvent") -> None: ...

class WebhookRetryQueue(Protocol):
    def enqueue(self, delivery: "WebhookDelivery", delay_seconds: int) -> None: ...  # SQS `webhooks`, consumed by the retry Lambda (§9)
    # WebhookDelivery: tenant_id, callback_url, event. Never the secret, which is looked up at send time

class ScaleInProtection(Protocol):
    def protect(self, expires_in_minutes: int) -> None: ...   # also used to renew
    def release(self) -> None: ...

class Clock(Protocol):
    def now(self) -> datetime: ...

class IdGenerator(Protocol):
    def new_id(self) -> str: ...      # ULID: sortable, URL-safe
```

### 4.4 Domain model

```python
class Status(StrEnum):
    NOT_STARTED = "notStarted"
    RUNNING     = "running"
    SUCCEEDED   = "succeeded"
    FAILED      = "failed"

class Feature(StrEnum):           # optional extras; text + layout blocks are always returned
    TABLES = "tables"

@dataclass
class Operation:
    operation_id: str
    tenant_id: str
    status: Status
    input_key: str                # S3 key of the PDF (inline body or presigned upload)
    features: set[Feature]
    include_markdown: bool
    pages: list[tuple[int, int]] | None  # validated `pages` ranges (§6.2), e.g. [(1, 3), (7, 7)]; never expanded. None = all
    callback_url: str | None
    attempt: int
    created_at: datetime
    updated_at: datetime
    running_since: datetime | None  # set only while running; sparse GSI for the sweeper (§14.1)
    expires_at: datetime          # created_at + 24h
    result_key: str | None
    error: "OperationError | None"
    document_page_count: int | None   # total pages in the PDF, reported by the parse (also on PageLimitExceeded)

@dataclass
class Tenant:
    tenant_id: str
    name: str
    api_keys: list["ApiKey"]      # up to 2 active (rotation); stored as one item per key in the `api_keys` table (§14.1)
    webhook_secret: str           # encrypted at rest (KMS)
    limits: "TenantLimits"        # max_inline_bytes, max_upload_bytes, max_pages (≤ 2,000; defaults in §6.4)
    active: bool

@dataclass
class ApiKey:
    key_id: str                   # public prefix, e.g. "pk_live_ab12"
    key_hash: str                 # SHA-256 of the full secret
    created_at: datetime
    revoked_at: datetime | None
```

### 4.5 Error taxonomy

| Code | Retryable | Example |
|---|---|---|
| `InvalidRequest` | — (400) | Missing body or `uploadId`, unknown `uploadId` (or another tenant's), unknown feature |
| `Unauthorized` | — (401) | Missing or invalid API key |
| `NotFound` | — (404) | Unknown or expired operation, wrong tenant |
| `PayloadTooLarge` | — (413) | File exceeds limit |
| `InvalidPdf` | No | Corrupt / not a PDF |
| `EncryptedPdf` | No (fail fast) | The PDF needs a password to open. Detected right after opening, before any parsing. Owner-password-only PDFs (open without a password) are parsed normally |
| `PageLimitExceeded` | No | Pages to analyze > tenant max (all pages, or the existing pages selected by `pages`) |
| `ParseTimeout` | No | Parsing exceeded the job time limit (§6.4). Not retried: the same PDF would time out again |
| `DocumentTooComplex` | No | The parse child ran out of memory: it hit its memory cap (§6.4) or the kernel OOM-killed it. Not retried: the same PDF would run out again. A smaller `pages` selection may succeed |
| `ParserError` | Yes | Unexpected exception or crash of the parse child process |
| `InternalError` | Yes | Storage/DB transient failure |

---

## 5. Parsing Pipeline (Worker)

### 5.1 Steps

```
download PDF from S3 to /tmp
   ─▶ DocumentParser.parse: one call, in a child process, on the whole PDF
         open + validate, failing fast with no retry:
            not a PDF / cannot open       → InvalidPdf
            needs a password to open      → EncryptedPdf
            pages to analyze > limit      → PageLimitExceeded
              (all pages, or the `pages` selection minus pages past the end)
         parse: layout, OCR where text is missing, tables and Markdown if requested
   ─▶ store result JSON in S3 ─▶ mark succeeded ─▶ webhook step ─▶ ack the job message
```

The **whole PDF goes to PyMuPDF in a single call**. The worker doesn't split the document into pages or call the engine page by page. PyMuPDF sees the full document, which it needs for document-wide decisions such as heading levels, and it returns Markdown for the whole document. The API's `pages` parameter is passed through to PyMuPDF as a page selection within that same call. Requested page numbers past the end of the document are **ignored** (dropped once the page count is known, with a `PagesOutOfRange` warning in the result). API page numbers are 1-based and PyMuPDF's are 0-based, so the adapter converts them. If none of the requested pages exist, PyMuPDF isn't called at all (an empty selection must never be passed through, because it could be read as "all pages"), and the operation succeeds with no pages.

Opening, validation and parsing all run in one **child process**, because all of them touch the untrusted PDF. The child is started with `spawn`, not `fork`, since the worker already runs threads (heartbeat) and forking a threaded process can deadlock. This lets the worker enforce the job time limit (§6.4), stop the parse on `SIGTERM` (§5.3), and contain crashes and exploits (§10.5). The worker's main process never opens the PDF and stays free for the heartbeat and scale-in protection renewal. The child writes its output (per-page JSON plus Markdown) to a file in `/tmp`; the worker maps it to the §7 schema and uploads it. The full result is held in memory once, which is acceptable for the 2,000-analyzed-page limit on a 16 GB task (checked in the spike, §18).

### 5.2 Engine

There is **no classification step**. The worker hands the whole document to PyMuPDF, and PyMuPDF itself OCRs only what lacks usable text (whole scanned pages, or just the affected regions of a page). The adapter sets `use_ocr=True` (default) and `ocr_language` from configuration. `force_ocr` is not exposed in the MVP.

| Concern | How |
|---|---|
| Text + layout | PyMuPDF layout analysis (reading order, multi-column, headings, headers/footers, lists, captions). CPU only, no GPU |
| OCR | PyMuPDF built-in hybrid OCR with **RapidOCR** (ONNX Runtime). Tesseract can be plugged in through `ocr_function` if a language needs it |
| Tables | PyMuPDF table detection, for digital and OCR'd pages alike |
| Markdown | PyMuPDF `to_markdown` on the whole document (tables as GFM tables), used as-is |
| JSON / blocks | PyMuPDF JSON output (layout boxes with class and bbox), mapped to the §7 schema by the adapter |

This removes the page classifier, the rasterizing step, PaddleOCR, PP-Structure, pdfplumber and the custom Markdown renderer. The worker has one parsing dependency (plus RapidOCR/ONNX Runtime).

**OCR and layout model files are baked into the worker image** at build time, so a new task never downloads models on first use or needs outbound internet access for them. The worker fails at startup if they're missing.

### 5.3 Worker termination safety (hard requirement)

**Requirement:** ECS must never stop a worker in the middle of a job because of autoscaling scale-in or a service deployment. One worker handles a whole document, and a fully scanned 500-page PDF can take 10–30 minutes on CPU. Losing that work would waste the compute and add the same delay again.

**Mechanisms:**

1. **ECS task scale-in protection** (`UpdateTaskProtection` through the ECS agent endpoint, `ECS_AGENT_URI`):
   - Turned **on right after a message is received**, before any processing starts.
   - Set with `expiresInMinutes` (e.g. 30 min) and **renewed** from the heartbeat thread, so a long job never outlives its protection. If the worker dies, protection lapses on its own.
   - Turned **off as soon as the job finishes** (success, failure or release), so idle tasks can be scaled in normally.
   - ECS respects protection for **both Service Auto Scaling scale-in and rolling deployments**. A deployment waits for protected tasks to finish their current job before replacing them.
2. **One job per worker task.** Concurrency comes from the number of tasks, not from threads inside a task. A protected task is therefore busy, and an unprotected one is idle. That's what makes protection meaningful, and it also keeps memory predictable.
3. **No Fargate Spot for workers.** Spot interruptions ignore task protection (2-minute warning). Workers run on on-demand Fargate only.
4. **Visibility heartbeat:** a background thread calls `extend_visibility` every ~60s, keeping the message invisible for another ~3 minutes. The visibility timeout stays short so that a worker that actually crashes gets its job picked up again quickly. The heartbeat (visibility and protection renewal) stops once the job has run longer than the job time limit plus a margin, and the worker exits, so a hung worker can't hold a message and a protected task forever.
5. **Graceful shutdown** (for stops that protection can't prevent, e.g. Fargate platform maintenance, a manual stop, or a SIGTERM in the short gap between receiving a message and enabling protection):
   - On `SIGTERM` the worker stops receiving new messages. The parse runs as one call in a child process, so it can't stop partway and resume. If it must give up, it kills the child.
   - Fargate gives at most 120s (`stopTimeout` = 120). If the current job can't finish within ~100s, the worker **releases the message right away** (visibility timeout → 0) and marks the operation `notStarted`, so another worker retries it without waiting.
6. **Deployment settings:** worker service `minimumHealthyPercent` = 100 and `maximumPercent` = 200. New tasks start alongside protected old tasks. Deployments can take up to the longest running job, which is accepted.

**Result:** in normal operation (scale-in, deploys) jobs are never interrupted. Rare forced stops lose at most one attempt, and the job is retried immediately.

---

## 6. HTTP API (`/v1`)

All endpoints require the `Api-Key: <secret>` header (the SDK sends it). Responses are JSON with camelCase fields.

### 6.1 `POST /v1/uploads`

Creates a presigned upload slot for large files.

**Request**
```json
{ "contentLength": 104857600 }
```

**Response `201`**
```json
{
  "uploadId": "01JBX9...",
  "uploadUrl": "https://bucket.s3.amazonaws.com/uploads/...?X-Amz-...",
  "method": "PUT",
  "headers": { "Content-Type": "application/pdf" },
  "expiresAt": "2026-11-02T12:15:00Z"
}
```

The presigned URL is limited to `Content-Type: application/pdf` and the declared length, and expires after 15 minutes. `analyze` re-checks the stored object's size (§6.4).

### 6.2 `POST /v1/documents:analyze`

**Query parameters (all variants)**

| Param | Type | Default | Description |
|---|---|---|---|
| `features` | csv of `tables` | none | Optional extras. Text and layout blocks are always returned; `tables` adds table cell structure (§7) |
| `includeMarkdown` | `true` \| `false` | `false` | Adds a Markdown rendering of the whole document (§7) |
| `pages` | e.g. `1-3,7` | all | Page subset (1-based). At most 100 ranges and page numbers up to 100,000, otherwise `InvalidRequest`; stored as ranges, never expanded. Pages past the end of the document are ignored (§5.1) |
| `callbackUrl` | HTTPS URL | — | Webhook target. Query parameter for inline bodies, JSON body field for Variant B; setting both is `InvalidRequest` |

**Variant A — inline body**
```
POST /v1/documents:analyze?features=tables&includeMarkdown=true
Content-Type: application/pdf
<binary>
```

**Variant B — presigned upload**
```json
POST /v1/documents:analyze
Content-Type: application/json
{ "uploadId": "01JBX9...", "callbackUrl": "https://client.example.com/hooks/pdf" }
```

There is no URL-source variant. The service doesn't fetch documents; clients that have a PDF at a URL download it themselves and upload the bytes.

**Response `202 Accepted`**
```
Operation-Location: https://api.example.com/v1/operations/01JBXA...
Retry-After: 2
```
```json
{ "operationId": "01JBXA...", "status": "notStarted" }
```

### 6.3 `GET /v1/operations/{operationId}`

**Response `200`, in progress**
```json
{
  "operationId": "01JBXA...",
  "status": "running",
  "createdAt": "2026-11-02T12:00:00Z",
  "lastUpdatedAt": "2026-11-02T12:00:05Z",
  "expiresAt": "2026-11-03T12:00:00Z"
}
```
Includes `Retry-After: <seconds>` (adaptive: short at first, longer for big documents).

**Response `200`, succeeded:** same fields plus `"result": { ...AnalyzeResult... }`.

**Response `200`, failed**
```json
{
  "operationId": "01JBXA...",
  "status": "failed",
  "error": { "code": "EncryptedPdf", "message": "The document is password-protected." }
}
```

**Response `404`:** unknown operation, operation owned by another tenant, or `expiresAt` has passed. The API checks expiry itself and doesn't rely on DynamoDB TTL timing.

> Results can be large (2,000 analyzed pages → tens of MB of JSON). If a result is over a size threshold (e.g. 5 MB), the response carries a short-lived presigned `resultUrl` instead of the inline `result`. The SDK handles both transparently.

### 6.4 Limits

Defaults apply to every tenant. File-size limits can be changed per tenant through the admin CLI (`TenantLimits`). The page limit can only be lowered per tenant: the job time limit is sized for 2,000 fully scanned pages, so the admin CLI rejects a value above 2,000. The job time limit and the parser memory cap are global.

| Limit | Value | Enforced where |
|---|---|---|
| Inline body max | **100 MB** | API (streaming byte count; `413 PayloadTooLarge`) |
| Presigned upload max | **500 MB** | `POST /v1/uploads` rejects a larger `contentLength` (`413`) and signs the PUT for the declared length. `analyze` re-checks the stored object's size with `HeadObject`; that is the authoritative check, since a PUT URL can't enforce a maximum on its own |
| Max pages | **2,000** pages analyzed per job | Worker validation (`PageLimitExceeded`, fail fast). Counts the pages that will be parsed, not the pages in the file: a 3,000-page PDF with `pages=1-5` is accepted, the same PDF without `pages` is rejected |
| Password-protected PDFs | **Rejected** | Worker validation (`EncryptedPdf`, fail fast) |
| Job time limit | **150 min** | Worker (parse child process killed; `ParseTimeout`, fail fast). Covers 2,000 fully scanned pages at ~4 s/page |
| Parser memory cap | Below the task's 16 GB (e.g. 12 GB, tuned in the spike, §18) | Worker (`RLIMIT_AS` on the parse child; `DocumentTooComplex`, fail fast) |
| SDK inline → presigned switch | **20 MB** | SDK (configurable `inline_threshold`) |

There is **no rate limiting** in the MVP (§11).

---

## 7. Result Schema (custom, simplified)

Organized per page. There is no span linking into a global content string (unlike Azure's `AnalyzeResult`).

```json
{
  "modelVersion": "2026-11-01",
  "documentPageCount": 12,
  "analyzedPageCount": 3,
  "pages": [
    {
      "pageNumber": 1,
      "width": 612.0,
      "height": 792.0,
      "unit": "point",
      "blocks": [
        {
          "type": "heading",
          "text": "Quarterly Report",
          "bbox": [72.0, 64.0, 540.0, 92.0],
          "confidence": null
        },
        {
          "type": "paragraph",
          "text": "Revenue grew 12% ...",
          "bbox": [72.0, 110.0, 540.0, 180.0],
          "lines": [
            { "text": "Revenue grew 12% ...", "bbox": [72.0, 110.0, 540.0, 124.0] }
          ],
          "confidence": null
        }
      ],
      "tables": [
        {
          "rowCount": 3,
          "columnCount": 2,
          "bbox": [72.0, 200.0, 540.0, 300.0],
          "cells": [
            { "row": 0, "column": 0, "rowSpan": 1, "columnSpan": 1, "text": "Region", "isHeader": true },
            { "row": 0, "column": 1, "rowSpan": 1, "columnSpan": 1, "text": "Revenue", "isHeader": true }
          ]
        }
      ]
    }
  ],
  "markdown": "# Quarterly Report\n\nRevenue grew 12% ...\n\n| Region | Revenue |\n|---|---|\n...",
  "warnings": [
    { "code": "PagesOutOfRange", "message": "Requested pages 13-15 don't exist; the document has 12 pages" },
    { "code": "LowOcrConfidence", "pageNumber": 3, "message": "Mean OCR confidence 0.42" }
  ]
}
```

Notes:
- `bbox` is `[x0, y0, x1, y1]` in points for every page, with the origin at the top-left corner and y growing downward (PyMuPDF's convention). PyMuPDF places OCR text at page coordinates, so no rescaling is needed.
- Block `type` values map from PyMuPDF's layout classes: `heading` (title, section header), `paragraph` (text), `listItem`, `caption`, `footnote`, `formula`, `table`, `picture`, `pageHeader`, `pageFooter`. A `table` block carries the table's text; its cell structure is in `tables` when requested.
- `confidence` is `null` for digital text. For OCR text it is `0..1` only if PyMuPDF exposes engine confidence; otherwise it is `null` and `LowOcrConfidence` is not emitted (§18).
- `documentPageCount` is the number of pages in the PDF. `analyzedPageCount` is the number of entries in the result's `pages` array, i.e. the pages actually parsed (fewer than `documentPageCount` when `pages` was used).
- Every page always has `blocks` (text and layout). `tables` appears only with `features=tables`, and `markdown` only with `includeMarkdown=true`.
- `modelVersion` identifies the version of the parsing pipeline, not the API version.

---

## 8. Client SDK (`pdfparser` Python package)

### 8.1 Design principles

Modeled on `azure-ai-documentintelligence`:
- A client object that takes an **endpoint** and a **credential**.
- `begin_analyze(...)` returns an **LRO poller**, and `.result()` blocks until a terminal state is reached.
- **Sync and async** flavors with the same API surface (`pdfparser.PdfParserClient`, `pdfparser.aio.PdfParserClient`).
- Typed result models (dataclasses or Pydantic) that mirror §7.
- Built on `httpx` (one dependency for both sync and async).

### 8.2 Usage

```python
from pdfparser import PdfParserClient, ApiKeyCredential, Feature

client = PdfParserClient(
    endpoint="https://api.example.com",
    credential=ApiKeyCredential("pk_live_..."),
)

# From a local file — SDK chooses inline vs presigned upload by size
with open("report.pdf", "rb") as f:
    poller = client.begin_analyze(
        document=f,
        features=[Feature.TABLES],
        include_markdown=True,
    )
result = poller.result()          # blocks; honors Retry-After
print(result.markdown)

# Resume a poller later (e.g. in another process)
token = poller.continuation_token()
poller = client.begin_analyze(continuation_token=token)

# Webhook-driven: submit and don't poll
with open("report.pdf", "rb") as f:
    poller = client.begin_analyze(document=f, callback_url="https://me.example.com/hook")
op_id = poller.operation_id
op = client.get_operation(op_id)
```

```python
# Async
from pdfparser.aio import PdfParserClient

async with PdfParserClient(endpoint=..., credential=...) as client:
    poller = await client.begin_analyze(document=data)
    result = await poller.result()
```

### 8.3 SDK responsibilities

| Concern | Behavior |
|---|---|
| Upload strategy | Uses an inline body when size ≤ `inline_threshold` (default 20 MB, configurable), otherwise presigned upload |
| Polling | Follows `Operation-Location`, respects `Retry-After`, `poller.status()`, `poller.done()`, `poller.wait(timeout)` |
| Continuation | `continuation_token()` / resuming from one |
| Retries | Exponential backoff with jitter on 502/503/504 and network errors (not on 4xx). Exception: `POST /v1/documents:analyze` is only retried on connection errors before the request was sent, because a retry after a 502/504 can create a duplicate job (R10) |
| Errors | `HttpResponseError` hierarchy for HTTP errors (`InvalidRequest`, `Unauthorized`, `NotFound`, `PayloadTooLarge`); `OperationFailedError` from `.result()` carrying the operation's error code (`EncryptedPdf`, `ParseTimeout`, ...) |
| Large results | Follows `resultUrl` transparently |
| Webhook helper | `pdfparser.webhooks.verify_signature(payload, headers, secret)` |
| Config | Timeouts, retry policy, user-agent, custom `httpx` transport (for testing) |

---

## 9. Webhooks

- Set **per request** with `callbackUrl` (HTTPS only).
- Sent when the operation reaches a terminal state: `operation.succeeded` or `operation.failed`, including operations failed by the sweeper (§12).
- **The payload is small and never includes the result.** The client fetches the result through the API, so no document data goes to a URL the service hasn't authenticated.

```json
{
  "eventId": "01JBXA...-succeeded",
  "type": "operation.succeeded",
  "operationId": "01JBXA...",
  "status": "succeeded",
  "occurredAt": "2026-11-02T12:04:10Z"
}
```

- **Signature headers:**
  - `Webhook-Id: <eventId>`
  - `Webhook-Timestamp: <unix seconds>`
  - `Webhook-Signature: v1=<hex HMAC-SHA256(secret, "{id}.{timestamp}.{body}")>`

  The secret is per tenant. Receivers should reject timestamps older than 5 minutes (replay protection). The timestamp and signature are computed for each delivery attempt, so a retry sent 10 minutes later still passes that check.
- **At-least-once delivery.** A crash or redelivery can send the same event twice. `eventId` is derived from the operation and the event type, so it stays the same across retries and redeliveries; receivers deduplicate on `Webhook-Id`.
- **Retries:** exponential backoff, 6 attempts over ~13 minutes: the first attempt and retries after 1s and 5s from the worker, then retries after 30s, 2m and 10m from the `webhooks` queue. A webhook that fails never changes the operation status, because the result is still available by polling.
- **Delivery mechanism (MVP):** the worker sends the webhook inline, with the short retry, after it marks the operation terminal and before it acks the job message, so a crash in between leads to a redelivery that redoes this step (§12). If those attempts fail, it puts the event on the `webhooks` SQS queue with a 30s delay and acks the job. The queued message holds the tenant ID, callback URL and event, never the secret.
- **Webhook-retry Lambda:** a small Lambda function consumes the `webhooks` queue (SQS event source). It loads the tenant's secret, signs and sends the event, and on failure sets the message's visibility timeout to the next backoff delay and leaves it on the queue. After the last attempt SQS moves the message to `webhooks-dlq` (`maxReceiveCount` = 3). It doesn't depend on worker tasks, so retries continue while workers are scaled to 0, and slow client endpoints never hold up parse workers. It runs in the private subnets and uses the same SSRF-safe notifier as the worker (§10.4).
- **SSRF protection:** see §10.4.

---

## 10. Security

### 10.1 Authentication

- API keys look like `pk_live_<keyId>_<secret>`. Only the **SHA-256 hash** is stored, in DynamoDB. Each key is its own item in the `api_keys` table, whose partition key is `keyHash`, so the API finds a key and its tenant with a direct lookup. (A GSI on the tenant item can't do this: GSI keys must be top-level attributes, not entries of a list.)
- Each tenant can have **two active keys** for zero-downtime rotation (issue the new key, switch clients, revoke the old one).
- The API caches tenant lookups in memory for a short time (e.g. 60s) to keep DynamoDB reads down. Revocation therefore takes effect within the cache TTL.

### 10.2 Authorization

- Every operation stores its `tenant_id`. `GetOperation` returns 404 (not 403) when the tenants don't match, so the API doesn't reveal which operation IDs exist.
- Presigned uploads are stored under `uploads/{tenantId}/{uploadId}`. `POST /v1/documents:analyze` only accepts an `uploadId` created by the same tenant; another tenant's `uploadId` is treated as unknown (`InvalidRequest`).

### 10.3 Data protection

- S3 and DynamoDB are encrypted at rest (SSE-KMS). TLS is used everywhere (ALB with an ACM certificate).
- S3 bucket: blocks all public access, enforces TLS in its bucket policy, allows access only through the task roles.
- Webhook secrets are encrypted with a dedicated KMS key (`webhook-secrets`) that only the worker and the webhook-retry Lambda can decrypt with. The API role uses the separate `data` key for S3, so it can't read them.
- Logs never contain document content or API key secrets.

### 10.4 SSRF (webhooks)

Webhook delivery is the only place the service sends requests to client-chosen URLs. It sends a small JSON event and never reads data back into the system. The worker and the webhook-retry Lambda use the same notifier adapter, so these rules apply to both:
- HTTPS only.
- Resolve DNS and **block private, loopback, link-local and metadata IP ranges**: RFC 1918, `127.0.0.0/8`, `169.254.0.0/16` (EC2 metadata and the ECS credentials endpoint `169.254.170.2`), `::1`, `fe80::/10`, `fc00::/7`, and the IPv4-mapped IPv6 forms of all of these. The connection goes to the IP that was checked, so DNS rebinding can't swap it afterwards.
- Redirects are not followed; a `3xx` counts as a failed delivery.
- Short timeouts; the response body is ignored.
- Workers and the webhook-retry Lambda (in the private subnets) send outbound traffic through a NAT gateway. A separate egress security group or proxy is optional hardening.

### 10.5 PDF safety

- PDFs are untrusted input. The parser runs inside worker containers that run as a non-root user with a read-only root filesystem (writable `/tmp` only), no instance credentials beyond the task role, and memory limits.
- A total job time limit (§6.4) protects against pathological PDFs (decompression bombs, huge images). The parse runs in a child process that is killed when the limit is reached. There is no per-page limit because the engine gets the whole document in one call.
- **Exploit containment.** MuPDF is a C library, and a PDF that exploits a memory-safety bug runs code in the parse child. Without containment, that code could take the worker's AWS credentials and read every tenant's documents. So the child starts with a scrubbed environment (no AWS variables), installs a seccomp filter that blocks opening network sockets before it opens the PDF (allowed unprivileged with `no_new_privs`), and runs under a memory cap (`RLIMIT_AS`, tuned in the spike) with `oom_score_adj` = 1000, so the kernel kills the child rather than the worker. A child that runs out of memory either way fails the job with `DocumentTooComplex` (§4.5). The worker process marks itself non-dumpable, so the child can't read its memory or environment through `/proc`.
- The worker wipes its `/tmp` job directory at the start and end of every job, so a later job (possibly another tenant's) never sees earlier files.

---

## 11. Rate Limiting & Quotas

**Out of scope for the MVP.** There is no rate limiting, no per-tenant concurrency cap and no usage quota, so there is no WAF, no rate-limiter port and no `429` responses.

The only limits are the per-request file-size, page-count and job-time limits in §6.4. All tenants share one FIFO-ish queue, and a single tenant can fill it. This is accepted for the MVP.

---

## 12. Failure Handling

| Situation | Behavior |
|---|---|
| Permanent error (`InvalidPdf`, `EncryptedPdf`, `PageLimitExceeded`, `ParseTimeout`, `DocumentTooComplex`) | **Fail fast**: mark `failed` right away (no retry), do the webhook step, then ack the message |
| Transient error (`ParserError`, `InternalError`) | Status back to `notStarted`, release the message with backoff delay (`ChangeMessageVisibility`) |
| Storage/DB error after the parse finished | Retried in-process with backoff for a few minutes before falling back to a release, because a release re-runs the whole parse |
| Parse child runs out of memory | The child reports an out-of-memory error, or exits from a `SIGKILL` the worker didn't send (the kernel OOM killer). The worker survives and fails the job with `DocumentTooComplex` (permanent) |
| Parse child crashes for any other reason | The worker survives and treats it as `ParserError` (transient, retried) |
| Worker crash / OOM | The heartbeat stops, the message becomes visible again, and the next delivery takes the job over |
| Picking up a job | The worker moves the operation to `running` with a conditional write (`expected={notStarted, running}`, only if the stored `attempt` is lower than the delivery's SQS `ApproximateReceiveCount`) and sets `attempt` to that count. `running` is allowed so a crashed worker's job can be taken over. Every later write by that worker (status, result pointer) is conditioned on `attempt` still being its own, so a superseded worker can't overwrite a newer run |
| Already terminal on pickup (duplicate delivery, or a crash after marking terminal but before acking) | Redo the webhook step (receivers deduplicate on `eventId`), then ack |
| Operation missing (deleted by TTL) or expired on pickup | Ack and skip, since nobody can fetch the result |
| Last attempt (`ApproximateReceiveCount` = N, default 3) | A transient error marks the operation `failed` (webhook step, then ack) instead of releasing |
| Delivery N + 1 | Only happens when a worker crashed or was stopped on attempt N, or crashed after marking the operation terminal. The queue's `maxReceiveCount` is N + 1, and this delivery never parses: it marks a non-terminal operation `failed`, does the webhook step and acks |
| DLQ | Gets a message only if delivery N + 1 fails too. CloudWatch alarm when depth > 0. For alerting and inspection only, with no redrive: the sweeper marks these operations `failed`, which is terminal, so a redriven message would only redo the webhook step. Clients resubmit the document |
| Stuck in `running` (backstop) | A periodic sweeper (scheduled ECS task or EventBridge + Lambda) marks operations `failed` with `InternalError` if they have been `running` (since `running_since`) longer than the job time limit (§6.4) plus a margin (e.g. 15 min), and queues their `operation.failed` webhook. A healthy worker can't exceed that, because the parse is killed at the limit and the heartbeat stops after it. Typical cause: delivery N + 1 failed too, so the message went to the DLQ |

---

## 13. Data Retention

**Target:** an operation, its input and its result are available for **24 hours** after submission.

### 13.1 Why S3 TTL alone isn't enough

Setting the TTL only in S3 (lifecycle expiration) is the right way to delete **document data**, but on its own it has two gaps:

1. **S3 lifecycle isn't exact either.** The shortest expiration is 1 day, rounded up to the next midnight UTC, and deletion runs asynchronously afterwards. An object can stay readable for roughly **24–48h**, and possibly more. Moving the TTL to S3 doesn't make expiry exact.
2. **S3 doesn't hold the operation record.** Status, tenant, error and the result pointer live in DynamoDB. If only S3 expires, operation records pile up forever, and `GET /v1/operations/{id}` would still report `succeeded` for a result that no longer exists.

### 13.2 Design: three layers, each with one job

| Layer | Mechanism | Purpose |
|---|---|---|
| **Logical expiry (exact)** | Every operation stores `expiresAt = createdAt + 24h`. The API returns **404 once `expiresAt < now`**, and presigned `resultUrl`s are never issued past `expiresAt` | What clients see: exactly 24h |
| **Document data deletion** | **S3 lifecycle rule**: expire `uploads/`, `inputs/`, `results/` after **1 day** | Removes the PDFs and results (the sensitive data), within ~48h |
| **Metadata cleanup** | **DynamoDB TTL** on `expiresAt` | Housekeeping, so the operations table doesn't grow forever. One attribute, no extra cost |

No sweeper or explicit delete job is needed for retention. If exact physical deletion becomes a compliance requirement later, the worker or a scheduled job can delete objects explicitly at `expiresAt`.

Two bucket settings keep the lifecycle rule effective: **versioning stays disabled** (with versioning, expired objects survive as noncurrent versions), and the rule also **aborts incomplete multipart uploads after 1 day** (streamed inline uploads use multipart, and abandoned parts would otherwise keep document data indefinitely).

The stated data policy is: "Results are available for 24 hours, and data is permanently deleted within 48 hours."

---

## 14. AWS Infrastructure (Terraform)

### 14.1 Resources

| Component | AWS resource |
|---|---|
| Edge | Route 53, ACM certificate (no WAF in MVP) |
| Load balancer | **ALB** (HTTPS listener, idle timeout ≥ 300s for big inline uploads) |
| API | **ECS Fargate service** (FastAPI + Uvicorn), autoscaling on CPU / request count |
| Workers | **ECS Fargate service**, on-demand only, `runtimePlatform` **ARM64** (CPU, e.g. 4 vCPU / 16 GB), autoscaling on jobs per task (§14.2), min 0–1, max e.g. 50, `stopTimeout` 120s, `/tmp` on a task volume (the root filesystem is read-only, §10.5) |
| Queues | SQS `jobs` (+ `jobs-dlq`, `maxReceiveCount` = N + 1, §12; message retention 24h), SQS `webhooks` (+ `webhooks-dlq`, `maxReceiveCount` = 3, §9). The DLQs keep messages for 4 days, longer than their source queues, because SQS keeps a message's original enqueue time when it moves it to a DLQ |
| State | DynamoDB `operations` (PK `operationId`, TTL `expiresAt`; sparse GSI on `runningSince`, which only `running` operations have, so the sweeper scans a small index), `tenants` (PK `tenantId`), `api_keys` (PK `keyHash`, one item per key with its `tenantId`) |
| Storage | S3 bucket (`uploads/`, `inputs/`, `results/`) with lifecycle + SSE-KMS |
| Secrets/keys | KMS CMKs: `data` (S3 and DynamoDB encryption) and `webhook-secrets` (only the worker and the webhook-retry Lambda can decrypt, §10.3); Secrets Manager (if needed) |
| Network | VPC, private subnets for tasks, NAT gateway, VPC endpoints for S3/DynamoDB/SQS (cost + security) |
| Images | ECR repositories (`api`, `worker`) |
| Observability | CloudWatch Logs, metrics, alarms, dashboards; X-Ray / ADOT collector sidecar |
| Sweeper | EventBridge schedule → Lambda (or scheduled ECS task) |
| Webhook retries | **Lambda** (arm64, Python 3.13) with an SQS event source on `webhooks`, in the private subnets (§9) |

### 14.2 Worker autoscaling

- Target-tracking metric: **jobs per task** = (`ApproximateNumberOfMessagesVisible` + `ApproximateNumberOfMessagesNotVisible`) / running tasks, target 1. In-flight messages must count, because each task holds its job for the whole run: counting only visible messages would leave as many jobs waiting as running.
- **Scale-out from zero:** with 0 running tasks the ratio is undefined, so a separate step-scaling rule starts the first task when visible messages > 0 and running tasks = 0.
- **Maximum task count** (e.g. 50) caps cost and stays below the account's Fargate vCPU quota (§17, R13).
- Scale-out cooldown is short; scale-in cooldown is longer. **Task scale-in protection** (§5.3) means scale-in only ever removes idle tasks.
- **No Fargate Spot**, because Spot interruptions bypass task protection (§5.3).

### 14.3 Terraform layout

```
infra/terraform/
  modules/
    network/        # VPC, subnets, NAT, endpoints
    storage/        # S3, KMS
    data/           # DynamoDB tables
    messaging/      # SQS queues + DLQs
    api_service/    # ALB, ECS service, autoscaling
    worker_service/ # ECS service, autoscaling, IAM
    functions/      # webhook-retry Lambda, sweeper schedule, IAM
    observability/  # dashboards, alarms
  envs/
    dev/
    staging/
    prod/
```
State lives in an S3 backend with locking (S3 native lockfile or a DynamoDB lock table).

### 14.4 IAM (least privilege)

- **API task role:** `s3:PutObject` on `inputs/*` and `uploads/*` (the latter for presigning); `s3:GetObject` on `uploads/*` (`HeadObject` for `upload_size`) and `results/*` (inline results and the presigned `resultUrl`); `kms:GenerateDataKey` and `kms:Decrypt` on the `data` key (required for SSE-KMS reads and writes, including presigned uploads); `sqs:SendMessage` on `jobs`; DynamoDB read/write on `operations`, read on `tenants` and `api_keys`.
- **Worker task role:** `s3:GetObject` on `inputs/*` and `uploads/*`; `s3:PutObject` on `results/*`; `kms:Decrypt` and `kms:GenerateDataKey` on the `data` key; `kms:Decrypt` on the `webhook-secrets` key; SQS receive/delete/change-visibility on `jobs`, send on `webhooks`; DynamoDB read/update on `operations`, read on `tenants`; `ecs:UpdateTaskProtection`.
- **Sweeper role:** DynamoDB read/update on `operations`, including its GSI (marking stuck jobs `failed`); `sqs:SendMessage` on `webhooks` (their `operation.failed` event).
- **Webhook-retry Lambda role:** SQS receive/delete/change-visibility on `webhooks`; DynamoDB read on `tenants`; `kms:Decrypt` on the `webhook-secrets` key; network interfaces in the VPC (`AWSLambdaVPCAccessExecutionRole`).
- **Admin CLI (operator role):** write on `tenants` and `api_keys`; `kms:Encrypt` on the `webhook-secrets` key; receive/delete on `jobs-dlq` and `webhooks-dlq` (inspection only).

---

## 15. Repository Layout & Tooling

```
pdf_parser/
├── pyproject.toml                 # uv workspace root
├── packages/
│   ├── core/                      # domain + application (ports, use cases). No infra deps.
│   │   └── src/pdfparser_core/
│   │       ├── domain/
│   │       ├── ports/
│   │       └── usecases/
│   ├── contracts/                 # Shared API DTOs (Pydantic) + OpenAPI snapshot
│   ├── adapters-aws/              # S3, SQS, DynamoDB, ECS protection
│   ├── adapters-parsing/          # PyMuPdfDocumentParser (PyMuPDF + RapidOCR)
│   └── adapters-http/             # httpx webhook notifier (SSRF-safe)
├── services/
│   ├── api/                       # FastAPI app = driving adapter + composition root
│   ├── worker/                    # SQS loop = driving adapter + composition root
│   ├── sweeper/                   # Scheduled sweep of stuck operations (§12)
│   ├── webhook-retry/             # Lambda handler for the `webhooks` queue (§9)
│   └── admin-cli/                 # Tenant & key management, DLQ inspection
├── sdk/
│   └── pdfparser/                 # Public client SDK (sync + aio). Depends only on httpx (+ pydantic).
├── infra/
│   └── terraform/
├── docker/
│   ├── api.Dockerfile
│   └── worker.Dockerfile          # linux/arm64, python:3.13-slim, OCR/layout models baked in
├── docker-compose.yml             # api + worker + LocalStack (S3, SQS, DynamoDB)
├── tests/
│   ├── unit/                      # core with in-memory fakes
│   ├── integration/               # adapters against LocalStack
│   ├── contract/                  # SDK ↔ OpenAPI contract tests
│   ├── e2e/                       # SDK → docker-compose stack, real PDFs
│   └── fixtures/pdfs/             # digital, scanned, mixed, tables, encrypted, corrupt, oversized
└── docs/
    ├── DESIGN.md
    └── adr/
```

**Tooling:**

| Tool | Use |
|---|---|
| **Python 3.13** | Every package, service and the SDK (`requires-python = ">=3.13,<3.14"` for services; SDK supports `>=3.11` for client reach). PyMuPDF ships `cp311-abi3` wheels, so it doesn't pin the Python version |
| **uv** | Workspace, dependency locking (`uv.lock`), running tools |
| **ruff** | Linting + formatting |
| **mypy** | `--strict` on `core`, `contracts` and `sdk`; standard mode on adapters |
| **pytest** | Unit, integration (LocalStack), contract and e2e suites |
| **import-linter** | Enforces the hexagonal dependency rule: `core` must not import `boto3`, `fastapi`, `httpx`, `pymupdf`, `rapidocr` or `onnxruntime` |

**SDK independence:** the SDK doesn't depend on `core` or the service packages. It's handwritten for a good developer experience. Contract tests check that its models and requests match the service's OpenAPI spec (generated by FastAPI and committed to `packages/contracts`).

### 15.1 Local development

- `docker-compose up` starts the **API**, the **worker** and **LocalStack** (S3, SQS, DynamoDB). An init script creates the bucket, queues and tables.
- The same AWS adapters run against LocalStack using `AWS_ENDPOINT_URL`.
- ECS scale-in protection has a no-op adapter for local runs.
- The webhook-retry handler is fed by a small SQS polling loop instead of a Lambda event source, and the sweeper runs on a timer.
- **Apple Silicon:** images are `linux/arm64`, the same as on AWS, so they run natively on M-series Macs without emulation. The parsing adapter also runs natively on macOS outside Docker for quick experiments. CI builds images on ARM64 runners (or with `docker buildx` for `linux/arm64`).
- In-memory fakes of every driven port live in `tests/` and are used for fast unit tests of the core. They aren't a supported runtime.

---

## 16. Observability

- **Logs:** structured JSON with `operationId`, `tenantId`, `attempt`, `documentPageCount`, `analyzedPageCount`, `durationMs`. Never document content.
- **Metrics (CloudWatch EMF):**
  - API: request count/latency/status by route, 401 rate.
  - Queue: `ApproximateAgeOfOldestMessage`, depth, DLQ depth.
  - Worker: jobs succeeded/failed by error code, job duration, pages/sec, parse duration, memory high-water mark, **jobs interrupted by SIGTERM** (should be ~0; checks §5.3).
  - Webhooks: delivery success/failure, attempts.
- **Tracing:** OpenTelemetry. Trace context passes from the API to the worker in SQS message attributes, so one trace covers submit → parse → webhook.
- **Alarms:** DLQ depth > 0 (`jobs-dlq` and `webhooks-dlq`), oldest message age > threshold (e.g. 15 min), worker failure rate, 5xx rate, sweeper finding stuck jobs, interrupted jobs > 0, worker task count at its maximum.
- **Dashboards:** one CloudWatch dashboard covering API health, queue depth and age, worker throughput, and failures by error code.

---

## 17. Risks & Mitigations

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | **CPU-only OCR is slow** (~1–4 s/page with RapidOCR; 500 scanned pages ≈ 10–30 min). Hybrid OCR roughly halves this on mixed pages | High latency for scanned docs | Accept for MVP; tune vCPU/threads; GPU worker pool or Textract adapter later. This is an infra/adapter change only |
| R2 | **Worker stopped mid-job** by scale-in or deployments | Lost work, repeated latency | **Hard requirement**, handled by task scale-in protection, on-demand Fargate only, heartbeat, graceful release (§5.3) |
| R3 | **Physical deletion lags the 24h TTL** (S3 lifecycle and DynamoDB TTL are both async, up to ~48h) | Data kept longer than 24h | API enforces `expiresAt` exactly; data policy says "deleted within 48h" (§13) |
| R4 | **SSRF via `callbackUrl`** | Requests to internal network / metadata | Strict URL validation, IP blocklist on the resolved IP, connection pinned to that IP, no redirects, response ignored (§10.4) |
| R5 | **Malicious / pathological PDFs** | Worker OOM/hang | Per-job timeout (child process killed) and memory cap on the child, both failing fast (`ParseTimeout`, `DocumentTooComplex`); sandboxed non-root containers; retries capped |
| R6 | **Single engine with built-in OCR.** PyMuPDF 2.0 is new (released 2026-10-19), and its OCR decisions and output (e.g. confidence) are less under our control | Behavior changes between versions; fields we can't fill | Pin the exact `pymupdf` version; golden-file tests on the fixture corpus run on every upgrade; spike before implementation (§18) |
| R7 | **Table quality on scanned pages** (borderless or complex tables) | Imperfect `tables` on OCR pages | Calibrate on fixture corpus; Textract adapter later behind `DocumentParser` if needed |
| R8 | **Large result payloads** | Slow polling responses | Offload to presigned `resultUrl` above a size threshold (§6.3) |
| R9 | **Large inline uploads through ALB/FastAPI** | API memory pressure | Stream request body to S3 (no full buffering); SDK prefers presigned above 20 MB |
| R10 | **No idempotency** | Client retries on `POST analyze` may create duplicate jobs | SDK doesn't automatically retry `POST analyze` after the request may have reached the server (only on connection errors before sending); add `Idempotency-Key` later |
| R11 | **PyMuPDF licensing.** PyMuPDF is AGPL-3.0 or Artifex commercial license. AGPL also covers software offered over a network | Running it in a closed-source service obliges us to publish our source to users, or to buy a license | Decide before production: open-source the service under AGPL, or buy an Artifex commercial license. Confirm with Artifex / legal |
| R12 | **Parser exploit.** A malicious PDF exploiting a MuPDF memory-safety bug runs code in the worker task | With the worker's AWS role: every tenant's documents and the webhook secrets | Child process with no network, scrubbed environment and memory cap; non-dumpable worker; `/tmp` wiped per job (§10.5). Pin PyMuPDF and patch on MuPDF security releases |
| R13 | **Runaway cost or capacity.** No rate limits or quotas; one tenant can queue thousands of large scanned PDFs | Large Fargate bill; hitting the account's Fargate vCPU quota slows every tenant | Worker maximum task count (§14.2), AWS Budgets alarm, task-count alarm (§16); raise the Fargate vCPU quota before launch if needed |

---

## 18. Open Questions

1. **OCR languages:** which languages are needed? This decides whether RapidOCR alone is enough or Tesseract (with language packs) is needed through `ocr_function`.
2. **`pages` parameter:** keep it in the MVP or defer? (Behavior is defined either way: out-of-range pages are ignored, §5.1.)
3. **PyMuPDF spike** on the fixture corpus (digital, scanned, mixed, tables) before implementation. It should confirm:
   - whether OCR confidence is exposed (if not, `confidence` stays `null` for OCR text);
   - quality of tables on scanned pages;
   - which model files must be baked into the image, and that every worker dependency installs on `linux/arm64`;
   - total time and peak memory of one whole-document call on a 500-page scanned PDF and a 2,000-page digital PDF, and on a PDF far above 2,000 pages with a small `pages` selection (only the selected pages should cost time and memory);
   - which PyMuPDF output gives per-page JSON and whole-document Markdown in a single pass, so OCR doesn't run twice;
   - whether that JSON has everything §7 needs: block classes, lines, and table cells with spans and header flags;
   - child start-up time per job (loading the OCR and layout models), which adds latency to every small document;
   - that the child sandbox (seccomp filter, memory cap, §10.5) works on Fargate, and how an out-of-memory child shows up (MuPDF error, `MemoryError` or kill), so it maps reliably to `DocumentTooComplex`.
4. **Data residency:** single region? Which one?
5. **Result-size threshold** for switching to `resultUrl`: 5 MB proposed.
6. **SDK distribution:** public PyPI or a private index (CodeArtifact)?

Resolved in v1: limits (§6.4), encrypted PDFs (fail fast), observability (§16), Python version and tooling (§15).
Resolved in v2: parsing and OCR engine (PyMuPDF, §5), whole-document parsing (§5.1), CPU architecture (ARM64), no URL sources (§6.2), request options (`features`, `includeMarkdown`, §6.2), page limit only lowered per tenant (§6.4), out-of-memory as a permanent error (§4.5), webhook retries in a Lambda function (§9), DLQ without redrive (§12).

---

## 19. Future Work

- `Idempotency-Key` header and content-hash result caching.
- GPU worker pool (ECS on EC2 GPU capacity provider) or Textract adapter for OCR.
- Expose `force_ocr` as a request option for PDFs with a bad text layer that PyMuPDF doesn't detect.
- Textract or LLM-based `DocumentParser` adapter for difficult (e.g. scanned, borderless) tables; schema-driven extraction.
- Page-range fan-out for very large documents.
- Batch submission (parent operation with child jobs).
- Per-tenant default webhook endpoints.
- `s3://` document sources from allow-listed buckets (server-side copy, no arbitrary URL fetching).
- Admin API and self-service portal.
- Additional cloud implementation (e.g. Azure/GCP adapters) to prove out the port boundaries.

---

## 20. Proposed ADRs

| ADR | Title |
|---|---|
| 0001 | Use ports & adapters (hexagonal) architecture with AWS as first adapter set |
| 0002 | One document per operation (no batches) |
| 0003 | PyMuPDF as the single parsing engine, with its built-in hybrid OCR (RapidOCR) |
| 0004 | One worker per document on on-demand ECS Fargate (no fan-out); never stopped mid-job |
| 0005 | ALB + FastAPI on Fargate instead of API Gateway |
| 0006 | Custom simplified result schema (not Azure AnalyzeResult-compatible); no key-values |
| 0007 | API keys per tenant; no rate limiting or quotas in MVP |
| 0008 | Handwritten Python SDK in monorepo, contract-tested against OpenAPI |
| 0009 | Terraform for infrastructure; LocalStack for local development |
| 0010 | 24h retention: API-enforced logical expiry + S3 lifecycle + DynamoDB TTL |
| 0011 | Python 3.13 on ARM64 (Graviton) with uv, ruff, mypy, pytest, import-linter |
| 0012 | PyMuPDF licensing: AGPL-3.0 compliance or Artifex commercial license |
| 0013 | No URL sources: clients upload document bytes (inline or presigned) |
