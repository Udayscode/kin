import os
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client
from kin.orchestrator.executor.dag_workflow import KinSequentialWorkflow
from kin.models.schemas import DAGSpec, TaskNode, TaskResult

app = FastAPI(title="Kin AI Gateway")


class WorkflowRequest(BaseModel):
    prompt: str


@app.post("/v1/workflows")
async def start_workflow(request: WorkflowRequest):
    try:
        # 1. CONNECT to the client
        client = await Client.connect("localhost:7233")

        research_node = TaskNode(
            id="research_01", agent_type="researcher", task_description=request.prompt
        )

        writer_node = TaskNode(
            id="writer_01",
            agent_type="writer",
            task_description="Summarize the research into markdown",
        )

        dag = DAGSpec(nodes=[research_node, writer_node])

        # 2. Use the CONNECTED client instance
        handle = await client.start_workflow(
            KinSequentialWorkflow.run,
            dag,
            id=f"kin-wf-{os.urandom(4).hex()}",
            task_queue="kin-task-queue",
        )
        return {"workflow_id": handle.id}

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

        # 1. Fetch results from Redis
        stream_key = f"results:{workflow_id}"
        raw_entries = await redis_client.xrange(stream_key)

        results_map = {}
        for _, entry in raw_entries:
            result = TaskResult.model_validate_json(entry["data"])
            results_map[result.node_id] = {
                "agent_type": (
                    "researcher" if "research" in result.node_id else "writer"
                ),
                "data": result.output,
                "status": "COMPLETED",
            }

        # 2. Add PENDING nodes so the table isn't empty
        # In Phase 1, we know the nodes are research_01 and writer_01
        expected_nodes = [("research_01", "researcher"), ("writer_01", "writer")]

        final_results = {}
        for node_id, agent_type in expected_nodes:
            if node_id in results_map:
                final_results[node_id] = results_map[node_id]
            else:
                final_results[node_id] = {
                    "agent_type": agent_type,
                    "data": None,
                    "status": "RUNNING" if desc.status.name == "RUNNING" else "PENDING",
                }

        return {
            "workflow_id": workflow_id,
            "status": desc.status.name,
            "results": final_results,
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
