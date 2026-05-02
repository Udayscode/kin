import asyncio
import os
import json
import redis.asyncio as redis
from kin.agents.writer.main import WriterAgent
from kin.models.schemas import TaskMessage, TaskResult
from dotenv import load_dotenv

load_dotenv()


async def run_agent():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    agent = WriterAgent(api_key=os.getenv("GROQ_API_KEY"))

    print(
        f"[Agent Runner] Starting agent and listening for '{agent.agent_type}' tasks..."
    )

    last_id = "$"

    while True:
        # read from pool
        streams = await r.xread({"agent_pool": last_id}, count=1, block=0)

        for _, messages in streams:
            for msg_id, data in messages:
                last_id = msg_id

                task_data = TaskMessage.model_validate_json(data["data"])

                if task_data.agent_type == agent.agent_type:
                    print(f"Processing task: {task_data.msg_id}")

                    # execute agent
                    result_data = await agent.process(task_data)

                    # construct result
                    res = TaskResult(
                        node_id=task_data.node_id,
                        status="COMPLETED",
                        output=result_data,
                    )

                    # publish result to workflow stream
                    await r.xadd(
                        f"results:{task_data.workflow_id}",
                        {"data": res.model_dump_json()},
                    )

                    # acknowledge/cleanup the processed message
                    await r.xdel("agent_pool", msg_id)
                    print(f"Task {task_data.msg_id} completed and result published.")


if __name__ == "__main__":
    asyncio.run(run_agent())
