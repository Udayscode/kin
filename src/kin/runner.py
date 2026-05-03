"""
Generic agent runner — replaces agent_runner.py and writer_runner.py.

Usage:
    uv run python -m kin.runner researcher
    uv run python -m kin.runner writer
"""

import asyncio
import os
import sys
import uuid

import redis.asyncio as redis
from dotenv import load_dotenv

from kin.models.schemas import TaskMessage, TaskResult
from kin.observability.logging import get_logger, setup_logging

load_dotenv()
setup_logging()
log = get_logger("kin.runner")

STREAM = "agent_pool"
RESULTS_TTL_SEC = 3600  # clean up result streams after 1 hour


def _build_agent(agent_type: str):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")

    if agent_type == "researcher":
        from kin.agents.researcher.main import ResearcherAgent

        return ResearcherAgent(api_key=api_key)
    elif agent_type == "writer":
        from kin.agents.writer.main import WriterAgent

        return WriterAgent(api_key=api_key)
    else:
        raise ValueError(f"Unknown agent type: {agent_type!r}")


async def _publish_result(r: redis.Redis, workflow_id: str, result: TaskResult) -> None:
    stream_key = f"results:{workflow_id}"
    await r.xadd(stream_key, {"data": result.model_dump_json()})
    # Refresh TTL on every write so active streams stay alive
    await r.expire(stream_key, RESULTS_TTL_SEC)


async def _ensure_group(r: redis.Redis, group: str) -> None:
    try:
        await r.xgroup_create(STREAM, group, id="0", mkstream=True)
        log.info("Consumer group %r created on stream %r", group, STREAM)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def run(agent_type: str) -> None:
    agent = _build_agent(agent_type)
    group = f"kin-workers-{agent_type}"  # per-type group: each gets all messages
    consumer_id = f"{agent_type}-{uuid.uuid4().hex[:8]}"

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    await _ensure_group(r, group)
    log.info("Runner started | agent=%s consumer=%s", agent_type, consumer_id)

    while True:
        try:
            entries = await r.xreadgroup(
                group,
                consumer_id,
                {STREAM: ">"},
                count=1,
                block=2000,
            )
        except (redis.ConnectionError, redis.TimeoutError) as e:
            log.warning("Redis connection error: %s — retrying in 2s", e)
            await asyncio.sleep(2)
            continue

        if not entries:
            continue

        for _, messages in entries:
            for msg_id, data in messages:
                try:
                    task_data = TaskMessage.model_validate_json(data["data"])
                except Exception as e:
                    log.error("Bad message %s — skipping: %s", msg_id, e)
                    await r.xack(STREAM, group, msg_id)
                    continue

                if task_data.agent_type != agent_type:
                    # Not ours — ACK so it doesn't sit in PEL, but don't process
                    await r.xack(STREAM, group, msg_id)
                    continue

                log.info(
                    "Processing | node=%s workflow=%s",
                    task_data.node_id,
                    task_data.workflow_id,
                )

                # Publish RUNNING immediately so CLI table updates
                await _publish_result(
                    r,
                    task_data.workflow_id,
                    TaskResult(
                        node_id=task_data.node_id,
                        status="RUNNING",
                        output={"agent_type": agent_type},
                    ),
                )

                try:
                    result_data = await agent.process(task_data)
                    result = TaskResult(
                        node_id=task_data.node_id,
                        status="COMPLETED",
                        output=result_data,
                    )
                    log.info(
                        "Completed  | node=%s workflow=%s",
                        task_data.node_id,
                        task_data.workflow_id,
                    )
                except Exception as e:
                    log.error(
                        "Failed     | node=%s error=%s",
                        task_data.node_id,
                        e,
                        exc_info=True,
                    )
                    result = TaskResult(
                        node_id=task_data.node_id,
                        status="FAILED",
                        error=str(e),
                        output={"agent_type": agent_type},
                    )

                await _publish_result(r, task_data.workflow_id, result)
                # ACK only after result is published — at-least-once delivery
                await r.xack(STREAM, group, msg_id)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m kin.runner <researcher|writer>")
        sys.exit(1)
    asyncio.run(run(sys.argv[1]))
