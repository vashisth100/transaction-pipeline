import csv
import json
import logging
import re
import statistics
from collections import defaultdict
from datetime import datetime

from app.celery_worker import celery_app
from app.database import SessionLocal
from app.models.models import Job, JobSummary, Transaction
from app.services.llm_service import classify_transactions_batch, generate_narrative_summary

logger = logging.getLogger(__name__)

# Domestic-only merchants that should never have USD transactions
DOMESTIC_MERCHANTS = {
    "swiggy", "ola", "irctc", "zomato", "flipkart",
    "meesho", "nykaa", "bigbasket", "dunzo", "blinkit",
}


def _parse_date(raw: str) -> str:
    """Normalise various date formats to ISO 8601 (YYYY-MM-DD)."""
    raw = raw.strip()
    for fmt in ("%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw  # return as-is if unparseable


def _parse_amount(raw: str) -> float | None:
    """Strip currency symbols and parse to float."""
    try:
        cleaned = re.sub(r"[^\d.-]", "", raw)
        return float(cleaned)
    except (ValueError, TypeError):
        return None


@celery_app.task(bind=True, max_retries=0)
def process_csv_task(self, job_id: int, file_path: str):
    db = SessionLocal()
    job = None

    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.status = "processing"
        db.commit()

        # ──────────────────────────────────────────
        # STEP 1: Read + Clean CSV
        # ──────────────────────────────────────────
        raw_rows = []
        seen_row_hashes: set = set()
        row_count_raw = 0

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_count_raw += 1
                raw_rows.append(dict(row))

        job.row_count_raw = row_count_raw
        db.commit()

        cleaned_rows = []
        seen_txn_ids: set = set()

        for row in raw_rows:
            txn_id = (row.get("txn_id") or "").strip()

            # Fill missing txn_id
            if not txn_id:
                continue  # truly blank txn_id: skip

            # Deduplicate by txn_id
            if txn_id in seen_txn_ids:
                continue
            seen_txn_ids.add(txn_id)

            # Deduplicate exact rows (all fields identical)
            row_key = tuple(sorted(row.items()))
            if row_key in seen_row_hashes:
                continue
            seen_row_hashes.add(row_key)

            # Parse amount
            amount = _parse_amount(row.get("amount", ""))
            if amount is None:
                continue  # unparseable amount: skip

            # Normalise fields
            date = _parse_date(row.get("date", ""))
            merchant = (row.get("merchant") or "").strip()
            currency = (row.get("currency") or "INR").strip().upper()
            status = (row.get("status") or "").strip().upper()
            category = (row.get("category") or "").strip()
            account_id = (row.get("account_id") or "").strip()
            notes = (row.get("notes") or "").strip()

            # Fill missing category
            if not category:
                category = "Uncategorised"

            if not merchant:
                continue  # skip rows without merchant

            cleaned_rows.append({
                "txn_id": txn_id,
                "date": date,
                "merchant": merchant,
                "amount": amount,
                "currency": currency,
                "status": status,
                "category": category,
                "account_id": account_id,
                "notes": notes,
            })

        job.row_count_clean = len(cleaned_rows)
        db.commit()

        # ──────────────────────────────────────────
        # STEP 2: Anomaly Detection
        # ──────────────────────────────────────────
        # Group amounts by account for median calculation
        account_amounts: dict = defaultdict(list)
        for r in cleaned_rows:
            account_amounts[r["account_id"]].append(r["amount"])

        account_medians: dict = {}
        for acc, amounts in account_amounts.items():
            account_medians[acc] = statistics.median(amounts)

        for r in cleaned_rows:
            reasons = []
            acc_median = account_medians.get(r["account_id"], 0)

            # Rule 1: amount > 3x account median
            if acc_median > 0 and r["amount"] > 3 * acc_median:
                reasons.append(f"Amount {r['amount']:.2f} exceeds 3x account median {acc_median:.2f}")

            # Rule 2: USD transaction at domestic-only merchant
            if r["currency"] == "USD" and r["merchant"].lower() in DOMESTIC_MERCHANTS:
                reasons.append(f"USD transaction at domestic merchant '{r['merchant']}'")

            # Rule 3: notes flag
            if "SUSPICIOUS" in (r.get("notes") or "").upper():
                reasons.append("Flagged as SUSPICIOUS in notes")

            r["is_anomaly"] = len(reasons) > 0
            r["anomaly_reason"] = "; ".join(reasons) if reasons else None

        # ──────────────────────────────────────────
        # STEP 3: LLM Classification (batched)
        # ──────────────────────────────────────────
        to_classify = [r for r in cleaned_rows if r["category"] == "Uncategorised"]
        llm_categories: dict = {}
        llm_batch_failed = False

        if to_classify:
            logger.info(f"Sending {len(to_classify)} transactions to LLM for classification...")

            # Create temporary proxy objects for the service
            class _TxnProxy:
                def __init__(self, d):
                    self.txn_id = d["txn_id"]
                    self.merchant = d["merchant"]
                    self.amount = d["amount"]
                    self.currency = d["currency"]

            proxies = [_TxnProxy(r) for r in to_classify]
            llm_categories = classify_transactions_batch(proxies)

            if not llm_categories:
                llm_batch_failed = True
                logger.warning("LLM classification failed for this batch; marking as llm_failed")

        # ──────────────────────────────────────────
        # STEP 4: Save Transactions to DB
        # ──────────────────────────────────────────
        saved_transactions = []
        total_inr = 0.0
        total_usd = 0.0
        merchants: dict = defaultdict(float)
        categories: dict = defaultdict(float)
        anomaly_count = 0

        for r in cleaned_rows:
            llm_cat = llm_categories.get(r["txn_id"])
            final_category = llm_cat if llm_cat else r["category"]

            txn = Transaction(
                job_id=job_id,
                txn_id=r["txn_id"],
                date=r["date"],
                merchant=r["merchant"],
                amount=r["amount"],
                currency=r["currency"],
                status=r["status"],
                category=final_category,
                account_id=r["account_id"],
                is_anomaly=r["is_anomaly"],
                anomaly_reason=r["anomaly_reason"],
                llm_category=llm_cat,
                llm_failed=llm_batch_failed and r["category"] == "Uncategorised",
            )
            db.add(txn)
            saved_transactions.append(txn)

            if r["currency"] == "INR":
                total_inr += r["amount"]
            elif r["currency"] == "USD":
                total_usd += r["amount"]

            merchants[r["merchant"]] += r["amount"]
            categories[final_category] += r["amount"]

            if r["is_anomaly"]:
                anomaly_count += 1

        db.commit()

        # ──────────────────────────────────────────
        # STEP 5: Risk Level
        # ──────────────────────────────────────────
        n = len(saved_transactions)
        if n == 0:
            risk_level = "low"
        else:
            ratio = anomaly_count / n
            risk_level = "high" if ratio >= 0.30 else "medium" if ratio >= 0.10 else "low"

        # Top 3 merchants
        top_merchants = sorted(merchants.items(), key=lambda x: x[1], reverse=True)[:3]

        # ──────────────────────────────────────────
        # STEP 6: LLM Narrative Summary (single call)
        # ──────────────────────────────────────────
        summary_input = {
            "transactions": n,
            "total_inr": total_inr,
            "total_usd": total_usd,
            "anomalies": anomaly_count,
            "risk": risk_level,
            "top_merchants": top_merchants,
            "categories": dict(categories),
        }

        logger.info("Generating LLM narrative summary...")
        llm_summary = generate_narrative_summary(summary_input)

        narrative = llm_summary.get("narrative", "Summary unavailable.")

        # ──────────────────────────────────────────
        # STEP 7: Save JobSummary
        # ──────────────────────────────────────────
        job_summary = JobSummary(
            job_id=job_id,
            total_spend_inr=total_inr,
            total_spend_usd=total_usd,
            top_merchants=json.dumps(top_merchants),
            anomaly_count=anomaly_count,
            narrative=narrative,
            risk_level=risk_level,
            category_breakdown=json.dumps(dict(categories)),
        )
        db.add(job_summary)

        job.status = "completed"
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} completed. {n} transactions, {anomaly_count} anomalies.")
        return {"status": "completed", "job_id": job_id, "transactions": n}

    except Exception as e:
        logger.exception(f"Job {job_id} failed: {e}")
        if job:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
        raise

    finally:
        db.close()
