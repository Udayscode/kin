from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum
from uuid import UUID, uuid4


class NodeState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskNode(BaseModel):
    id: str
    agent_type: str
    task_description: str
    input_from: List[str] = []
    dependencies: List[str] = []
    input_from: List[str] = []
    dependencies: list[str] = []


class DAGSpec(BaseModel):
    workflow_id: UUID = Field(default_factory=uuid4)
    nodes: List[TaskNode]
    entry_nodes: List[str] = []
    exit_nodes: List[str] = []


class TaskMessage(BaseModel):
    msg_id: str
    workflow_id: str
    node_id: str
    agent_type: str
    task_description: str
    dep_outputs: Dict[str, Any] = {}


class TaskResult(BaseModel):
    node_id: str
    status: NodeState
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
