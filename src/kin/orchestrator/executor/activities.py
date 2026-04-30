from temporalio import activity
from bus.streams import KinBus
from kin.models.schemas import TaskNode, TaskMessage, TaskResult

import uuid
import time
import asyncio


class AgentActivities:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.bus = KinBus(host=host, port=port)

    # ---------------------------------------------------------
    # Research Activity
    # ---------------------------------------------------------
    @activity.defn(name="research_activity")
    async def research_activity(self, task: TaskMessage) -> dict:
        node = TaskNode(
            id=task.node_id,
            agent_type="researcher",
            task_description=task.task_description,
        )

        return await self.dispatch_task(node, task.workflow_id)

    # ---------------------------------------------------------
    # Writer Activity
    # ---------------------------------------------------------
    @activity.defn(name="writer_activity")
    async def writer_activity(self, task: TaskMessage) -> dict:
        node = TaskNode(
            id=task.node_id, agent_type="writer", task_description=task.task_description
        )

        return await self.dispatch_task(node, task.workflow_id)

    # ---------------------------------------------------------
    # Core Dispatcher
    # ---------------------------------------------------------
    @activity.defn
    async def dispatch_task(self, node: TaskNode, workflow_id: str) -> dict:
        clean_wf_id = workflow_id.strip()

        stream_key = f"results:{clean_wf_id}"
        sent_key = f"sent:{clean_wf_id}:{node.id}"

        # -------------------------------------------------
        # Idempotent Send (avoid duplicate dispatch on retry)
        # -------------------------------------------------
        already_sent = await self.bus.client.get(sent_key)

        if not already_sent:
            msg = TaskMessage(
                msg_id=str(uuid.uuid4()),
                workflow_id=clean_wf_id,
                node_id=node.id,
                agent_type=node.agent_type,
                task_description=node.task_description,
            )

            await self.bus.send_task(msg)

            # mark sent for 1 hour
            await self.bus.client.set(sent_key, "1", ex=3600)

        # -------------------------------------------------
        # Wait for Result
        # -------------------------------------------------
        last_id = "0"  # only listen for new messages
        deadline = time.time() + 120  # 2 minute timeout

        while time.time() < deadline:
            try:
                activity.heartbeat(f"Waiting for node {node.id}")

                entries = await self.bus.client.xread(
                    {stream_key: last_id}, count=1, block=3000
                )

                if entries:
                    for _, messages in entries:
                        for msg_id, data in messages:
                            last_id = msg_id

                            result = TaskResult.model_validate_json(data["data"])

                            if result.node_id == node.id:
                                if result.status == "FAILED":
                                    raise Exception(result.error)

                                return {
                                    "node_id": node.id,
                                    "agent_type": node.agent_type,
                                    "data": result.output,
                                }

            except asyncio.CancelledError:
                activity.logger.warning(
                    f"Activity cancelled while waiting for {node.id}"
                )
                raise

            except Exception as e:
                activity.logger.warning(
                    f"Transient polling error for {node.id}: {str(e)}"
                )
                await asyncio.sleep(1)

        raise TimeoutError(f"Node {node.id} timed out after 120 seconds")
