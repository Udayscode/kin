import asyncio

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from kin.observability.logging import get_logger, setup_logging
from kin.orchestrator.executor.activities import AgentActivities
from kin.orchestrator.executor.dag_workflow import KinDAGWorkflow

load_dotenv()
setup_logging()
log = get_logger("kin.worker")


async def main():
    client = await Client.connect(
        "localhost:7233", data_converter=pydantic_data_converter
    )

    activities = AgentActivities()

    worker = Worker(
        client,
        task_queue="kin-task-queue",
        workflows=[KinDAGWorkflow],
        activities=[
            activities.dispatch_task,
            activities.research_activity,
            activities.writer_activity,
        ],
    )

    log.info("Temporal worker started on queue 'kin-task-queue'")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
