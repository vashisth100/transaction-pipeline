import json
import os
import shutil
import uuid
from datetime import datetime

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse

from app.celery_worker import celery_app
from app.database import SessionLocal
from app.models.models import Job, JobSummary, Transaction
from app.services.processor import process_csv_task

router = APIRouter()

UPLOAD_DIR = "uploads"


@router.post("/jobs/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Accept a CSV upload, create a Job record, enqueue processing,
    return job_id immediately.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    unique_name = f"{uuid.uuid4()}_{file.filename}"
    file_path = os.path.join(UPLOAD_DIR, unique_name)

    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    db = SessionLocal()
    try:
        job = Job(
            filename=file.filename,
            status="pending",
            created_at=datetime.utcnow(),
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        task = process_csv_task.delay(job.id, file_path)

        job.task_id = task.id
        db.commit()

        return {"job_id": job.id, "status": "pending", "filename": file.filename}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/jobs/{job_id}/status")
def get_status(job_id: int):
    """
    Return current job status.
    If completed, also includes a high-level summary.
    """
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        response = {
            "job_id": job.id,
            "status": job.status,
            "filename": job.filename,
            "row_count_raw": job.row_count_raw,
            "row_count_clean": job.row_count_clean,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        }

        if job.status == "completed":
            summary = db.query(JobSummary).filter(JobSummary.job_id == job_id).first()
            if summary:
                response["summary"] = {
                    "total_spend_inr": summary.total_spend_inr,
                    "total_spend_usd": summary.total_spend_usd,
                    "anomaly_count": summary.anomaly_count,
                    "risk_level": summary.risk_level,
                }

        if job.status == "failed" and job.error_message:
            response["error_message"] = job.error_message

        return response

    finally:
        db.close()


@router.get("/jobs/{job_id}/results")
def get_results(job_id: int):
    """
    Return full structured output:
    - cleaned transactions list
    - flagged anomalies
    - per-category spend breakdown
    - LLM-generated narrative summary
    """
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status in ("pending", "processing"):
            return {"job_id": job_id, "status": job.status, "message": "Job not yet complete"}

        if job.status == "failed":
            return {"job_id": job_id, "status": "failed", "error": job.error_message}

        transactions = db.query(Transaction).filter(Transaction.job_id == job_id).all()
        summary = db.query(JobSummary).filter(JobSummary.job_id == job_id).first()

        cleaned_list = [
            {
                "txn_id": t.txn_id,
                "date": t.date,
                "merchant": t.merchant,
                "amount": t.amount,
                "currency": t.currency,
                "status": t.status,
                "category": t.category,
                "account_id": t.account_id,
                "is_anomaly": t.is_anomaly,
                "anomaly_reason": t.anomaly_reason,
                "llm_category": t.llm_category,
                "llm_failed": t.llm_failed,
            }
            for t in transactions
        ]

        flagged_anomalies = [t for t in cleaned_list if t["is_anomaly"]]

        category_breakdown = {}
        if summary and summary.category_breakdown:
            category_breakdown = json.loads(summary.category_breakdown)

        return {
            "job_id": job_id,
            "status": "completed",
            "transactions": cleaned_list,
            "flagged_anomalies": flagged_anomalies,
            "category_breakdown": category_breakdown,
            "summary": {
                "total_spend_inr": summary.total_spend_inr if summary else 0,
                "total_spend_usd": summary.total_spend_usd if summary else 0,
                "top_merchants": json.loads(summary.top_merchants) if summary else [],
                "anomaly_count": summary.anomaly_count if summary else 0,
                "narrative": summary.narrative if summary else "",
                "risk_level": summary.risk_level if summary else "unknown",
            },
        }

    finally:
        db.close()


@router.get("/jobs")
def list_jobs(status: str = Query(None, description="Filter by status: pending/processing/completed/failed")):
    """
    List all jobs with status, filename, row count, created_at.
    Supports ?status= query filter.
    """
    db = SessionLocal()
    try:
        query = db.query(Job)
        if status:
            query = query.filter(Job.status == status.lower())

        jobs = query.order_by(Job.created_at.desc()).all()

        return [
            {
                "job_id": j.id,
                "filename": j.filename,
                "status": j.status,
                "row_count_raw": j.row_count_raw,
                "row_count_clean": j.row_count_clean,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            }
            for j in jobs
        ]
    finally:
        db.close()
