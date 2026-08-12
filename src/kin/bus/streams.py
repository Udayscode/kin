import redis.asyncio as redis
import json


class KinBus:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
        # Create the actual Redis client needed by the activities for XREAD
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        print(f"[KinBus] Initialized with {self.host}:{self.port}")

    async def send_task(self, msg):
        """
        Publishes the TaskMessage to the agent_pool stream.
        Matches the call in activities.py
        """
        payload = {"data": msg.model_dump_json()}
        # Add to the appropriate agent stream based on agent_type
        stream_name = f"tasks:{msg.agent_type}"
        await self.client.xadd(stream_name, payload)
        print(
            f"[KinBus] Task {msg.msg_id} (Type: {msg.agent_type}) sent to {stream_name}"
        )
        return True

    async def send_result(self, result, workflow_id: str):
        """Publishes TaskResult back to orchestrator stream."""
        stream_name = f"results:{workflow_id}"
        payload = {"data": result.model_dump_json()}
        await self.client.xadd(stream_name, payload)
        print(f"[KinBus] Result for node {result.node_id} sent to {stream_name}")
        return True

    async def publish(self, stream_name, data):
        """Generic publish method for other events."""
        payload = {"data": json.dumps(data) if isinstance(data, dict) else data}
        await self.client.xadd(stream_name, payload)
        print(f"[KinBus] Publishing to {stream_name}")
        return True
