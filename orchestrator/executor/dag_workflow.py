from datetime import timedelta
from temporalio import workflow
from models.schemas import DAGSpec, TaskResult

@workflow.defn
class KinSequentialWorkflow:
    @workflow.run
    async def run(self, dag: DAGSpec) -> dict:
        results = {}

        for i, node in enumerate(dag.nodes):
            # 1. RATE LIMIT PREVENTION (The "Wait")
            # Gemini Free Tier allows very few Requests Per Minute (RPM).
            # If this isn't the first node, wait 35 seconds to let the quota reset.
            if i > 0:
                await workflow.sleep(35) 

            # 2. CONTEXT INJECTION
            if results:
                context_summary = "\n".join([
                    f"--- Data from {node_id} ---\n{str(data)}" 
                    for node_id, data in results.items()
                ])
                node.task_description += f"\n\nUSE THIS PREVIOUS CONTEXT:\n{context_summary}"

            # 3. ROBUST EXECUTION
            result_dict = await workflow.execute_activity(
                "dispatch_task",
                args=[node, workflow.info().workflow_id],
                start_to_close_timeout=timedelta(
                    seconds=getattr(node, 'timeout_sec', 120)
                ),
            )

            # 4. STATE PERSISTENCE
            results[node.id] = result_dict.get("data")

        return results