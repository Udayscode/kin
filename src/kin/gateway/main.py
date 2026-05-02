import os
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from kin.orchestrator.executor.dag_workflow import KinDAGWorkflow
from kin.models.schemas import DAGSpec, TaskNode, TaskResult
from kin.orchestrator.planner import Planner

load_dotenv()

app = FastAPI(title="Kin AI Gateway")


class WorkflowRequest(BaseModel):
    prompt: str


@app.post("/v1/workflows")
async def start_workflow(request: WorkflowRequest):
    try:
        client = await Client.connect(
            "localhost:7233", data_converter=pydantic_data_converter
        )

        # NEW: use planner
        planner = Planner(api_key=os.getenv("GROQ_API_KEY"))
        dag = planner.plan(request.prompt)

        dag_id = str(dag.workflow_id)

        await client.start_workflow(
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
        print(f"Error starting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Initialize Redis client (ideally outside the function or in app state)
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


@app.get("/v1/workflows/{workflow_id}")
async def get_status(workflow_id: str):
    try:
        client = await Client.connect("localhost:7233")
        handle = client.get_workflow_handle(workflow_id)
        desc = await handle.describe()

        stream_key = f"results:{workflow_id}"
        raw_entries = await redis_client.xrange(stream_key)

        final_results = {}

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

        return {
            "workflow_id": workflow_id,
            "status": desc.status.name,
            "results": final_results,
        }

    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    # Use the string "kin.gateway.main:app" for hot-reloading support
    uvicorn.run(app, host="0.0.0.0", port=8000)
