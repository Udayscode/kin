import json
from groq import Groq
from kin.models.schemas import DAGSpec, TaskNode
import os


SYSTEM_PROMPT = """
You are a workflow planner for an AI agent platform called Kin.
Available agent types: "researcher", "writer"

Given a user task, decompose it into a DAG of sub-tasks.
Rules:
- researcher: fetches and synthesizes information
- writer: formats/structures content into final output
- writer must always depend on researcher output
- input_from: list of node IDs whose output this node needs
- dependencies: same values as input_from
- entry_nodes: node IDs with no dependencies
- exit_nodes: node IDs with no dependents

Return ONLY valid JSON. No prose. No markdown fences.

Example output:
{
  "nodes": [
    {
      "id": "research_01",
      "agent_type": "researcher",
      "task_description": "...",
      "input_from": [],
      "dependencies": [],
      "timeout_sec": 120,
      "max_retries": 2
    },
    {
      "id": "writer_01",
      "agent_type": "writer",
      "task_description": "...",
      "input_from": ["research_01"],
      "dependencies": ["research_01"],
      "timeout_sec": 120,
      "max_retries": 2
    }
  ],
  "entry_nodes": ["research_01"],
  "exit_nodes": ["writer_01"]
}
"""

class Planner:
    def __init__(self):
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    def plan(self, task: str) -> DAGSpec:
        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Task: {task}"}
            ],
            temperature=0.1,   # low temp — we want deterministic structure
            max_tokens=1024,
        )

        raw = response.choices[0].message.content.strip()

        # Strip markdown fences if model disobeys
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        data = json.loads(raw)
        dag = DAGSpec(**data)
        self._validate(dag)
        return dag

    def _validate(self, dag: DAGSpec):
        node_ids = {n.id for n in dag.nodes}

        # All dependency references must point to real nodes
        for node in dag.nodes:
            for dep in node.dependencies:
                if dep not in node_ids:
                    raise ValueError(f"Node {node.id} has unknown dependency: {dep}")

        # Cycle detection (DFS)
        adj = {n.id: n.dependencies for n in dag.nodes}
        visited, rec = set(), set()

        def dfs(nid):
            visited.add(nid)
            rec.add(nid)
            for dep in adj.get(nid, []):
                if dep not in visited and dfs(dep):
                    return True
                if dep in rec:
                    return True
            rec.discard(nid)
            return False

        for nid in node_ids:
            if nid not in visited:
                if dfs(nid):
                    raise ValueError(f"Cycle detected in DAG")