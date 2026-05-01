import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from kin.orchestrator.executor.dag_workflow import KinWorkflow
from kin.orchestrator.executor.activities import KinActivities


async def main():
    client = await Client.connect("localhost:7233")

    acts = KinActivities()

    worker = Worker(
        client,
        task_queue="kin-tasks",
        workflows=[KinWorkflow],
        activities=[acts.dispatch_task],
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
