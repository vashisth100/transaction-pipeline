# AI-Powered Transaction Processing Pipeline

A FastAPI + Celery + PostgreSQL + Redis + Ollama backend that accepts a dirty CSV of financial transactions, processes them asynchronously, uses an LLM to classify and summarise, and exposes a polling API.

---

## Stack

| Component | Technology |
|-----------|-----------|
| API Framework | FastAPI |
| Database | PostgreSQL 16 |
| Job Queue | Celery + Redis |
| LLM | Ollama (`llama3.2:3b`) |
| Containerisation | Docker + Docker Compose |

---

## Quick Start

```bash
git clone <repo-url>
cd transaction-pipeline
docker compose up --build
```

> **First run note:** Ollama will download the `llama3.2:3b` model (~2 GB). This takes a few minutes. The API waits for Ollama's health check before accepting requests.

Once running, visit **http://localhost:8000** for the web UI, or use the curl examples below.

---

## API Endpoints

### POST /jobs/upload
Upload a CSV file and start processing.

```bash
curl -X POST http://localhost:8000/jobs/upload \
  -F "file=@transactions.csv"
# Response: {"job_id": 1, "status": "pending", "filename": "transactions.csv"}
```

### GET /jobs/{job_id}/status
Poll job status. Returns summary stats when completed.

```bash
curl http://localhost:8000/jobs/1/status
```

### GET /jobs/{job_id}/results
Get full results: cleaned transactions, anomalies, category breakdown, LLM narrative.

```bash
curl http://localhost:8000/jobs/1/results
```

### GET /jobs
List all jobs. Supports `?status=` filter.

```bash
curl http://localhost:8000/jobs
curl "http://localhost:8000/jobs?status=completed"
curl "http://localhost:8000/jobs?status=failed"
```

---

## Processing Pipeline

```
CSV Upload → Job Created (pending)
     ↓
Celery Worker picks up task
     ↓
1. Data Cleaning
   - Normalise dates to ISO 8601
   - Strip $ from amounts
   - Uppercase status/currency
   - Fill missing categories with 'Uncategorised'
   - Remove exact duplicate rows
     ↓
2. Anomaly Detection
   - Flag: amount > 3× account median
   - Flag: USD at domestic-only merchant (Swiggy, Ola, IRCTC…)
   - Flag: SUSPICIOUS notes
     ↓
3. LLM Classification (single batched call)
   - Transactions with missing category → Ollama assigns one of:
     Food / Shopping / Travel / Transport / Utilities /
     Cash Withdrawal / Entertainment / Other
   - Retry up to 3× with exponential backoff
   - On full failure: marked llm_failed=true, job continues
     ↓
4. LLM Narrative Summary (single call)
   - Produces: total spend, top merchants, anomaly count,
     2-3 sentence narrative, risk_level (low/medium/high)
     ↓
Job status → completed
```

---

## Data Model

**Job** — `id, filename, status, row_count_raw, row_count_clean, task_id, error_message, created_at, completed_at`

**Transaction** — `id, job_id, txn_id, date, merchant, amount, currency, status, category, account_id, is_anomaly, anomaly_reason, llm_category, llm_raw_response, llm_failed`

**JobSummary** — `id, job_id, total_spend_inr, total_spend_usd, top_merchants, anomaly_count, narrative, risk_level, category_breakdown`

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | postgres://... | PostgreSQL connection string |
| `REDIS_URL` | redis://redis:6379/0 | Redis broker URL |
| `OLLAMA_URL` | http://ollama:11434/api/chat | Ollama chat endpoint |
| `OLLAMA_MODEL` | llama3.2:3b | Model to use |

---

## Architecture Diagram

See the draw.io diagram linked in the submission.
https://app.diagrams.net/#G190VnAYEs3mrzH975JwNvCgwbw8snOeze#%7B%22pageId%22%3A%22architecture%22%7D
