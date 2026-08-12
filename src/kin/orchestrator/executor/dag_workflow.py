import asyncio
from datetime import timedelta
from temporalio import workflow
import temporalio.exceptions
from temporalio.common import RetryPolicy
from kin.models.schemas import DAGSpec, NodeState


@workflow.defn
class KinDAGWorkflow:
    @workflow.run
    async def run(self, dag: DAGSpec) -> dict:
        node_map = {n.id: n for n in dag.nodes}
        state = {n.id: NodeState.PENDING for n in dag.nodes}
        results = {}

        while True:
            # nodes whose deps are all done and are still pending
            ready = [
                nid
                for nid, s in state.items()
                if s == NodeState.PENDING
                and all(
                    state.get(dep) == NodeState.COMPLETED
                    for dep in node_map[nid].input_from
                )
            ]

            if not ready:
                if all(s == NodeState.COMPLETED for s in state.values()):
                    break
                if any(s == NodeState.FAILED for s in state.values()):
                    failed = [nid for nid, s in state.items() if s == NodeState.FAILED]
                    raise temporalio.exceptions.ApplicationError(f"Nodes failed: {failed}")
                # nothing ready but not done — shouldn't happen if DAG is valid
                break

            # mark ready nodes as running
            for nid in ready:
                state[nid] = NodeState.RUNNING

            # inject context from completed deps into task description
            for nid in ready:
                node = node_map[nid]
                if node.input_from:
                    parts = []
                    for dep in node.input_from:
                        dep_result = results.get(dep, {})
                        if isinstance(dep_result, dict):
                            content = (
                                dep_result.get("data", {}).get("research_content")
                                or dep_result.get("markdown")
                                or dep_result.get("research_content")
                                or str(dep_result)
                            )
                        else:
                            content = str(dep_result)
                        parts.append("--- Research from " + dep + " ---\n" + content)
                    node.task_description += (
                        "\n\nCONTEXT FROM PREVIOUS RESEARCH:\n" + "\n\n".join(parts)
                    )

            # dispatch ready nodes in parallel
            tasks = [
                workflow.execute_activity(
                    "dispatch_task",
                    args=[node_map[nid], str(dag.workflow_id)],
                    start_to_close_timeout=timedelta(seconds=node_map[nid].timeout_sec),
                    retry_policy=RetryPolicy(
                        maximum_attempts=3,
                        initial_interval=timedelta(seconds=30),
                        maximum_interval=timedelta(minutes=3),
                    ),
                )
                for nid in ready
            ]

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for nid, result in zip(ready, batch_results):
                if isinstance(result, Exception):
                    state[nid] = NodeState.FAILED
                else:
                    results[nid] = result.get("data")
                    state[nid] = NodeState.COMPLETED

        return results
