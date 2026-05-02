import asyncio
import logging
from abc import ABC, abstractmethod
from kin.models.schemas import TaskMessage, TaskResult, NodeState
from kin.bus.streams import KinBus

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    def __init__(
        self,
        agent_type: str,
        host: str = "localhost",
        port: int = 6379,
        max_tasks: int = 5,
    ):
        self.agent_type = agent_type
        self.bus = KinBus(host=host, port=port)
        self.group = "workers"
        self.consumer_id = f"{agent_type}-instance-{id(self)}"
        self.semaphore = asyncio.Semaphore(max_tasks)

    @abstractmethod
    async def process(self, task: TaskMessage) -> dict:
        """Specific logic for the agent implementation."""
        pass

    async def _setup_stream(self):
        try:
            await self.bus.client.xgroup_create(
                f"tasks:{self.agent_type}", self.group, id="0", mkstream=True
            )
        except Exception:
            pass

    async def start(self):
        await self._setup_stream()

        while True:
            # block for 5 secs if no messages; fetch new messages (">")
            streams = await self.bus.client.xreadgroup(
                self.group,
                self.consumer_id,
                {f"tasks:{self.agent_type}": ">"},
                count=1,
                block=5000,
            )

            if not streams:
                continue

            for _, msg in streams:
                for msg_id, data in msg:
                    asyncio.create_task(self._handle_wrapper(msg_id, data))

    async def _handle_wrapper(self, msg_id: str, data: dict):
        async with self.semaphore:
            try:
                task = TaskMessage.model_validate_json(data["data"])
                output = await self.process(task)

                result = TaskResult(
                    node_id=task.node_id, status=NodeState.COMPLETED, output=output
                )
            except Exception as e:
                logger.error(f"Execution failed for node {data.get('node_id')}: {e}")
                result = TaskResult(
                    node_id=data.get("node_id", "unknown"),
                    status=NodeState.FAILED,
                    error=str(e),
                )

            # Sequence: 1. Send Result -> 2. ACK message
            await self.bus.send_result(result, result.node_id)
            await self.bus.client.xack(f"tasks:{self.agent_type}", self.group, msg_id)
