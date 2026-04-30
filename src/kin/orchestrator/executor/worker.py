import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.pydantic import pydantic_data_converter

from kin.orchestrator.executor.dag_workflow import KinSequentialWorkflow
from kin.orchestrator.executor.activities import AgentActivities


async def main():
    client = await Client.connect(
        "localhost:7233",
        data_converter=pydantic_data_converter
    )

    activities = AgentActivities()

    worker = Worker(
        client,
        task_queue="kin-task-queue",
        workflows=[KinSequentialWorkflow],
        activities=[
            activities.dispatch_task,
            activities.research_activity,
            activities.writer_activity,
        ],
    )

    print("Worker started...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())