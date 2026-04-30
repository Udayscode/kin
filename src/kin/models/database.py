from sqlalchemy import Column, String, DateTime, JSON, Numeric, ForeignKey, Text
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
import uuid

Base = declarative_base()


class WorkflowRecord(Base):
    __tablename__ = "workflows"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    task_input = Column(Text, nullable=False)
    dag_spec = Column(JSON)
    status = Column(String, default="PENDING")
    result = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())
    total_cost_usd = Column(Numeric(10, 6), default=0)


class WorkflowNodeRecord(Base):
    __tablename__ = "workflow_nodes"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_id = Column(String, ForeignKey("workflows.id"))
    node_id = Column(String, nullable=False)
    agent_type = Column(String, nullable=False)
    status = Column(String, default="PENDING")
    result = Column(JSON)
    cost_usd = Column(Numeric(10, 6), default=0)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
