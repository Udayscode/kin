# Kin — Multiagent AI Orchestration Platform

Kin is a durable, parallel multiagent workflow engine. You submit a natural language task. A planner decomposes it into a DAG of specialized agents. The agents run — in parallel where possible — and produce a final synthesized report. If the orchestrator crashes mid-run, execution resumes exactly where it left off.

This is not a wrapper around LangChain. Every layer is explicit: message bus, DAG executor, agent runtime, result aggregation.

---

## What It Does

```
You: "Research silicon price spikes and write a risk report for a solid-state startup"

Planner (Groq/LLaMA):
  n1 → researcher  "silicon price history"          ─┐
  n2 → researcher  "battery manufacturing delays"   ─┼→ n4 writer
  n3 → researcher  "solid-state supply chain 2027"  ─┘

n1, n2, n3 run in parallel.
n4 waits for all three, receives analysis, and writes the final markdown report.
```

---

## Architecture

```
CLI / API Client
      │
      ▼
FastAPI Gateway          ← auth, rate limiting, workflow submission
      │
      ▼
Planner (Groq LLaMA)     ← decomposes task → DAGSpec (JSON)
      │
      ▼
Temporal Workflow        ← durable DAG executor (survives crashes)
      │
      ├─ dispatch_task activity ──► Redis Stream: tasks:{agent_type}
      │                                    │
      │                             Agent consumes task
      │                             (researcher / writer)
      │                                    │
      └─ poll results ◄────────── Redis Stream: results:{workflow_id}
```

**Why Temporal?** Workflow state is checkpointed after every activity. Kill the orchestrator mid-run — restart it — execution continues from the last completed node. No custom retry logic, no state machines.

**Why Redis Streams?** Agents are decoupled from the orchestrator. The orchestrator doesn't call agent code directly — it publishes to a stream and waits. This means agent pools scale independently (add replicas without touching orchestrator code). Consumer groups give at-least-once delivery with PEL tracking.

**Why separate agent processes?** One slow agent doesn't block others. Each agent type is an independent process with its own concurrency semaphore. The coder agent can run in a gVisor sandbox without affecting the researcher.

---

## Project Structure

```
infra/
└── docker/
    └── docker-compose.yaml  # Redis, Temporal, and Postgres services
src/kin/
├── gateway/
│   └── main.py              # FastAPI app — /v1/workflows POST + GET
├── orchestrator/
│   ├── planner.py           # LLM → DAGSpec with cycle validation
│   ├── main.py              # Orchestrator entrypoint
│   └── executor/
│       ├── dag_workflow.py  # Temporal @workflow.defn — parallel DAG runner
│       ├── activities.py    # Temporal @activity.defn — dispatch + poll
│       └── worker.py        # Temporal worker process
├── agents/
│   ├── base/
│   │   └── agent.py         # BaseAgent — stream consumer, semaphore, ACK
│   ├── researcher/
│   │   └── main.py          # DDG search → Groq synthesis
│   └── writer/
│       └── main.py          # Final markdown report generation
├── bus/
│   └── streams.py           # KinBus — Redis Streams wrapper (send/receive)
├── models/
│   ├── database.py          # Database configuration
│   └── schemas.py           # Pydantic: DAGSpec, TaskNode, TaskMessage, TaskResult
├── observability/
│   └── logging.py           # Structured logging setup
├── frontend/                # Next.js 14 control plane (Phase 3)
├── tools/                   # Agent tools and utilities
├── context/                 # Scoped context store
├── cli.py                   # typer CLI — submit + poll
├── runner.py                # Base agent runner script
├── agent_runner.py          # Researcher runner script
└── writer_runner.py         # Writer runner script
```

---

## Data Flow — Detailed

### 1. Submit

```
POST /v1/workflows  {"prompt": "..."}
  → Planner.plan(prompt)
      → Groq LLaMA decomposes into DAGSpec
      → cycle detection (DFS)
      → entry_nodes / exit_nodes computed
  → Temporal.start_workflow(KinDAGWorkflow, dag)
  → returns {workflow_id, nodes[]}
```

### 2. DAG Execution (inside Temporal)

```python
# KinDAGWorkflow runs this loop:
while not all COMPLETED:
    ready = nodes where all input_from deps are COMPLETED
    for each ready node (in parallel):
        inject dep outputs into task_description
        dispatch_task(node) → publish to Redis Stream
        wait for result on results:{workflow_id}
        mark node COMPLETED or FAILED
```

### 3. Agent Runtime (each agent is a separate process)

```python
# BaseAgent loop:
xreadgroup(tasks:{agent_type}, ">")   # blocking read, consumer group
→ process(task)                        # agent-specific logic
→ send_result(TaskResult) to Redis     # results:{workflow_id}
→ xack(msg_id)                         # remove from PEL
```

### 4. Result Stream

```
GET /v1/workflows/{id}
  → Temporal describe (overall status)
  → Redis xrange(results:{workflow_id})  (all completed node results)
  → returns {status, results: {node_id: {agent_type, data, error}}}
```

---

## Models

```python
class TaskNode(BaseModel):
    id: str                    # "n1", "n2" ...
    agent_type: str            # "researcher" | "writer"
    task_description: str
    input_from: List[str]      # dep node IDs — empty = runs first
    dependencies: list[str]    # additional dependency references
    timeout_sec: int = 600
    max_retries: int = 2

class DAGSpec(BaseModel):
    workflow_id: UUID
    nodes: List[TaskNode]
    entry_nodes: List[str]     # no deps — run immediately in parallel
    exit_nodes: List[str]      # no dependents — their output = final result

class TaskMessage(BaseModel):
    msg_id: str
    workflow_id: str
    node_id: str
    agent_type: str
    task_description: str
    dep_outputs: Dict[str, Any]   # outputs from input_from nodes

class TaskResult(BaseModel):
    node_id: str
    status: NodeState              # COMPLETED | FAILED
    output: Optional[Dict]
    error: Optional[str]
```

---

## Setup

**Prerequisites:** Python 3.12+, Node 18+, Redis, Temporal server

```bash
# 1. Start infrastructure (Redis, Temporal, Postgres)
cd infra/docker
docker compose up -d

# 2. Install Python deps
cd ~/projects/kin
uv sync   # or: pip install -e .

# 3. Environment
cp .env.example .env
# set GROQ_API_KEY in .env

# 4. Start the Temporal worker
python -m kin.orchestrator.executor.worker

# 5. Start agents (each in a separate terminal)
python -m kin.agents.researcher.main
python -m kin.agents.writer.main

# 6. Start the gateway
uvicorn kin.gateway.main:app --host 0.0.0.0 --port 8000

# 7. Submit a workflow
kin submit "Research the impact of TSMC's Arizona fabs on US semiconductor sovereignty"
```

---

## Key Design Decisions

**LLM only at planning time, not routing time.** The planner calls Groq once to produce a DAG. After that, all execution is deterministic Python. No LLM decides which agent runs next — the DAG does. This keeps execution fast, predictable, and auditable.

**Agents are stateless.** No agent holds state between tasks. State lives in the context injected into `task_description` (short-term) and Redis (workflow-scoped). This makes agents independently restartable and horizontally scalable.

**Idempotent dispatch.** Before publishing a task to Redis, the dispatcher checks a `sent:{workflow_id}:{node_id}` key. Temporal retries are safe — the task won't be double-dispatched to the agent pool.

**Consumer groups for competing consumers.** Multiple instances of the same agent type all read from `tasks:{agent_type}` via the same consumer group. Redis delivers each message to exactly one consumer. Scale researcher agents from 1 to 10 with no code changes.

---

## WIP...
