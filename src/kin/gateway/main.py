import os
from contextlib import asynccontextmanager

import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from kin.models.schemas import TaskResult
from kin.observability.logging import get_logger, setup_logging
from kin.orchestrator.executor.dag_workflow import KinDAGWorkflow
from kin.orchestrator.planner import Planner

load_dotenv()
setup_logging()
log = get_logger("kin.gateway")

RESULTS_TTL_SEC = 3600


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Gateway starting up...")
    app.state.temporal = await Client.connect(
        "localhost:7233", data_converter=pydantic_data_converter
    )
    app.state.redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
    log.info("Connected to Temporal and Redis")
    yield
    await app.state.redis.aclose()
    log.info("Gateway shut down cleanly")


app = FastAPI(title="Kin AI Gateway", lifespan=lifespan)


class WorkflowRequest(BaseModel):
    prompt: str


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/healthz", tags=["ops"])
async def health():
    """Liveness probe — returns 200 when gateway is up and Redis is reachable."""
    try:
        await app.state.redis.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"status": "ok", "redis": redis_ok}


# ---------------------------------------------------------------------------
# Workflow endpoints
# ---------------------------------------------------------------------------
@app.post("/v1/workflows")
async def start_workflow(request: WorkflowRequest):
    try:
        planner = Planner(api_key=os.getenv("GROQ_API_KEY"))
        dag = planner.plan(request.prompt)
        dag_id = str(dag.workflow_id)

        log.info("Starting workflow dag_id=%s nodes=%d", dag_id, len(dag.nodes))

        await app.state.temporal.start_workflow(
            KinDAGWorkflow.run,
            dag,
            id=dag_id,
            task_queue="kin-task-queue",
        )

        return {
            "workflow_id": dag_id,
            "nodes": [n.id for n in dag.nodes],
        }

    except Exception as e:
        log.error("Failed to start workflow: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/workflows/{workflow_id}")
async def get_status(workflow_id: str):
    try:
        handle = app.state.temporal.get_workflow_handle(workflow_id)
        desc = await handle.describe()

        stream_key = f"results:{workflow_id}"
        raw_entries = await app.state.redis.xrange(stream_key)

        final_results: dict = {}
        for _, entry in raw_entries:
            result = TaskResult.model_validate_json(entry["data"])
            final_results[result.node_id] = {
                "status": result.status,
                "agent_type": (
                    result.output.get("agent_type") if result.output else None
                ),
                "data": result.output,
                "error": result.error,
            }

        overall_status = desc.status.name

        # Set TTL on result stream once workflow is terminal
        if overall_status in ("COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"):
            await app.state.redis.expire(stream_key, RESULTS_TTL_SEC)

        return {
            "workflow_id": workflow_id,
            "status": overall_status,
            "results": final_results,
        }

    except Exception as e:
        log.warning("get_status error for %s: %s", workflow_id, e)
        raise HTTPException(status_code=404, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("kin.gateway.main:app", host="0.0.0.0", port=8000, reload=True)
