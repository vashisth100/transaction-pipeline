import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")

VALID_CATEGORIES = [
    "Food", "Shopping", "Travel", "Transport",
    "Utilities", "Cash Withdrawal", "Entertainment", "Other",
]

MAX_RETRIES = 3
BACKOFF_BASE = 2  # seconds


def _call_ollama(prompt: str, attempt: int = 0) -> str | None:
    """Single LLM call with exponential backoff retry."""
    for i in range(MAX_RETRIES):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                },
                timeout=120,
            )
            response.raise_for_status()
            return response.json()["message"]["content"]

        except Exception as e:
            wait = BACKOFF_BASE ** i
            logger.warning(f"LLM call failed (attempt {i+1}/{MAX_RETRIES}): {e}. Retrying in {wait}s...")
            if i < MAX_RETRIES - 1:
                time.sleep(wait)

    logger.error("All LLM retries exhausted.")
    return None


def classify_transactions_batch(transactions: list) -> dict:
    """
    Given a list of Transaction ORM objects without a category,
    returns a dict mapping txn_id -> category string.
    Batches all transactions into one LLM call.
    """
    if not transactions:
        return {}

    input_data = [
        {
            "txn_id": t.txn_id,
            "merchant": t.merchant,
            "amount": t.amount,
            "currency": t.currency,
        }
        for t in transactions
    ]

    prompt = f"""You are a financial transaction classifier.

Classify each transaction into exactly one of these categories:
Food, Shopping, Travel, Transport, Utilities, Cash Withdrawal, Entertainment, Other

Return ONLY a valid JSON object mapping txn_id to category. No extra text, no markdown fences.

Example output:
{{"TXN001": "Food", "TXN002": "Shopping"}}

Transactions to classify:
{json.dumps(input_data)}
"""

    raw = _call_ollama(prompt)
    if raw is None:
        return {}

    try:
        # Strip markdown fences if present
        clean = raw.strip().strip("```json").strip("```").strip()
        result = json.loads(clean)
        # Validate categories
        validated = {}
        for txn_id, cat in result.items():
            if cat in VALID_CATEGORIES:
                validated[txn_id] = cat
            else:
                validated[txn_id] = "Other"
        return validated
    except Exception as e:
        logger.error(f"Failed to parse LLM classification response: {e}\nRaw: {raw}")
        return {}


def generate_narrative_summary(summary_data: dict) -> dict:
    """
    Makes a single LLM call to produce a JSON narrative summary.
    Returns a dict with: total_spend_by_currency, top_merchants,
    anomaly_count, narrative (2-3 sentences), risk_level.
    """
    prompt = f"""You are a senior financial risk analyst.

Analyze these transaction statistics and return ONLY a valid JSON object.
No markdown, no explanation, just JSON.

Required JSON format:
{{
  "total_spend_inr": <number>,
  "total_spend_usd": <number>,
  "top_merchants": [<name1>, <name2>, <name3>],
  "anomaly_count": <number>,
  "narrative": "<2-3 sentence spending summary>",
  "risk_level": "<low|medium|high>"
}}

Transaction statistics:
{json.dumps(summary_data, indent=2)}
"""

    raw = _call_ollama(prompt)

    if raw is None:
        return {
            "total_spend_inr": summary_data.get("total_inr", 0),
            "total_spend_usd": summary_data.get("total_usd", 0),
            "top_merchants": [m[0] for m in summary_data.get("top_merchants", [])],
            "anomaly_count": summary_data.get("anomalies", 0),
            "narrative": (
                f"Processed {summary_data.get('transactions', 0)} transactions totalling "
                f"INR {summary_data.get('total_inr', 0):.2f} and USD {summary_data.get('total_usd', 0):.2f}. "
                f"Detected {summary_data.get('anomalies', 0)} anomalies with risk level {summary_data.get('risk', 'unknown')}."
            ),
            "risk_level": summary_data.get("risk", "low"),
            "llm_failed": True,
        }

    try:
        clean = raw.strip().strip("```json").strip("```").strip()
        return json.loads(clean)
    except Exception as e:
        logger.error(f"Failed to parse LLM narrative response: {e}\nRaw: {raw}")
        return {
            "total_spend_inr": summary_data.get("total_inr", 0),
            "total_spend_usd": summary_data.get("total_usd", 0),
            "top_merchants": [m[0] for m in summary_data.get("top_merchants", [])],
            "anomaly_count": summary_data.get("anomalies", 0),
            "narrative": raw[:500] if raw else "Summary unavailable.",
            "risk_level": summary_data.get("risk", "low"),
        }
