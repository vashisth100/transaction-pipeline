from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON
from app.database import Base


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=True)
    status = Column(String, default="pending")          # pending / processing / completed / failed
    task_id = Column(String, nullable=True)
    row_count_raw = Column(Integer, default=0)
    row_count_clean = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, index=True)
    txn_id = Column(String)
    date = Column(String)             # stored as ISO 8601 string
    merchant = Column(String)
    amount = Column(Float)
    currency = Column(String)
    status = Column(String)
    category = Column(String)
    account_id = Column(String, nullable=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_reason = Column(String, nullable=True)
    llm_category = Column(String, nullable=True)
    llm_raw_response = Column(Text, nullable=True)
    llm_failed = Column(Boolean, default=False)


class JobSummary(Base):
    __tablename__ = "job_summaries"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, index=True)
    total_spend_inr = Column(Float, default=0.0)
    total_spend_usd = Column(Float, default=0.0)
    top_merchants = Column(Text)        # JSON string
    anomaly_count = Column(Integer, default=0)
    narrative = Column(Text)
    risk_level = Column(String)
    category_breakdown = Column(Text)   # JSON string
