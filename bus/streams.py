import redis.asyncio as redis
import json

class KinBus:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
        # Create the actual Redis client needed by the activities for XREAD
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        print(f"[Stub Bus] Initialized with {self.host}:{self.port}")
    
    async def send_task(self, msg):
        """
        Publishes the TaskMessage to the agent_pool stream.
        Matches the call in activities.py
        """
        payload = {"data": msg.model_dump_json()}
        # Add to the 'agent_pool' stream
        await self.client.xadd("agent_pool", payload)
        print(f"[Stub Bus] Task {msg.msg_id} (Type: {msg.agent_type}) sent to agent_pool")
        return True

    async def publish(self, stream_name, data):
        """Generic publish method for other events."""
        payload = {"data": json.dumps(data) if isinstance(data, dict) else data}
        await self.client.xadd(stream_name, payload)
        print(f"[Stub Bus] Publishing to {stream_name}")
        return True