from datetime import timedelta
import asyncio

from temporalio import workflow
from temporalio.common import RetryPolicy

from kin.models.schemas import DAGSpec, NodeState, TaskMessage


@workflow.defn
class KinDAGWorkflow:

    @workflow.run
    async def run(self, dag: DAGSpec) -> dict:
        node_map = {n.id: n for n in dag.nodes}
        state = {n.id: NodeState.PENDING for n in dag.nodes}
        results = {}

        workflow_id = str(dag.workflow_id)

        while True:

            # Runnable nodes
            ready = [
                nid
                for nid, st in state.items()
                if st == NodeState.PENDING
                and all(
                    state[d] == NodeState.COMPLETED
                    for d in node_map[nid].dependencies
                )
            ]

            # Exit conditions
            if not ready:

                if all(s == NodeState.COMPLETED for s in state.values()):
                    break

                failed = [
                    nid for nid, st in state.items()
                    if st == NodeState.FAILED
                ]

                if failed:
                    raise Exception(f"Nodes failed: {failed}")

                raise Exception("Deadlock detected in DAG")

            # Mark RUNNING
            for nid in ready:
                state[nid] = NodeState.RUNNING

            # Run all ready nodes in parallel
            batch_tasks = [
                self._execute_node(
                    workflow_id,
                    node_map[nid],
                    results
                )
                for nid in ready
            ]

            batch_results = await asyncio.gather(
                *batch_tasks,
                return_exceptions=True
            )

            # Update states
            for nid, output in zip(ready, batch_results):

                if isinstance(output, Exception):
                    state[nid] = NodeState.FAILED
                    results[nid] = {"error": str(output)}
                else:
                    state[nid] = NodeState.COMPLETED
                    results[nid] = output

        return {
            "workflow_id": workflow_id,
            "results": results,
            "final_outputs": {
                nid: results.get(nid)
                for nid in dag.exit_nodes
            },
        }

    async def _execute_node(
        self,
        workflow_id: str,
        node,
        prior_results: dict
    ):
        context = self._build_context(
            node.dependencies,
            prior_results
        )

        msg = TaskMessage(
            msg_id="",
            workflow_id=workflow_id,
            node_id=node.id,
            agent_type=node.agent_type,
            task_description=node.task_description + context,
            dep_outputs={},
        )

        activity_name = self._activity_for(node.agent_type)

        return await workflow.execute_activity(
            activity_name,
            msg,
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3
            ),
        )

    def _build_context(
        self,
        deps: list[str],
        results: dict
    ) -> str:

        if not deps:
            return ""

        parts = ["\n\nCONTEXT FROM PREVIOUS TASKS:\n"]

        for dep in deps:
            parts.append(
                f"\n--- {dep} ---\n{results.get(dep)}"
            )

        return "".join(parts)

    def _activity_for(self, agent_type: str) -> str:

        mapping = {
            "researcher": "research_activity",
            "writer": "writer_activity",
            "planner": "planner_activity",
            "critic": "critic_activity",
            "coder": "coder_activity",
            "reviewer": "reviewer_activity",
        }

        if agent_type not in mapping:
            raise ValueError(
                f"Unknown agent type: {agent_type}"
            )

        return mapping[agent_type]