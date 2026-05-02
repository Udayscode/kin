import asyncio
from datetime import timedelta
from temporalio import workflow
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
                    raise Exception(f"Nodes failed: {failed}")
                # nothing ready but not done — shouldn't happen if DAG is valid
                break

            # mark ready nodes as running
            for nid in ready:
                state[nid] = NodeState.RUNNING

            # inject context from completed deps into task description
            for nid in ready:
                node = node_map[nid]
                if node.input_from:
                    context = "\n".join(
                        f"--- Output from {dep} ---\n{str(results.get(dep, ''))}"
                        for dep in node.input_from
                    )
                    node.task_description += (
                        f"\n\nCONTEXT FROM PREVIOUS STEPS:\n{context}"
                    )

            # dispatch ready nodes in parallel
            tasks = [
                workflow.execute_activity(
                    "dispatch_task",
                    args=[node_map[nid], str(dag.workflow_id)],
                    start_to_close_timeout=timedelta(seconds=node_map[nid].timeout_sec),
                    retry_policy=RetryPolicy(maximum_attempts=1),
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
