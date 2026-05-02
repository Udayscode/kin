import json
from groq import Groq
from kin.models.schemas import DAGSpec, TaskNode
import os

SYSTEM_PROMPT = """
You are a workflow planner for an AI agent platform called Kin.
Available agent types: "researcher", "writer"

Decompose the user task into a DAG of nodes.
Rules:
- Use "researcher" to gather information
- Use "writer" to produce final output from research
- input_from: list of node IDs whose output this node needs
- Nodes with empty input_from run first (in parallel if multiple)
- Output ONLY valid JSON, no prose, no markdown fences

JSON format:
{
  "nodes": [
    {
        "id": "n1",
        "agent_type": "researcher",
        "task_description": "...",
        "input_from": []
    },
    {
        "id": "n2",
        "agent_type": "writer",
        "task_description": "...",
        "input_from": ["n1"]
    }
  ]
}
"""


class Planner:
    def __init__(self, api_key: str):
        self.client = Groq(api_key=api_key)

    def plan(self, task: str) -> DAGSpec:
        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Task: {task}"},
            ],
            temperature=0.2,  # low temp = more deterministic JSON
            max_tokens=1024,
        )

        raw = response.choices[0].message.content.strip()

        # strip markdown fences if model ignores instructions
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        data = json.loads(raw)
        nodes = [TaskNode(**n) for n in data["nodes"]]

        # auto-compute entry/exit nodes
        all_deps = {dep for n in nodes for dep in n.input_from}

        dag = DAGSpec(
            nodes=nodes,
            entry_nodes=[n.id for n in nodes if not n.input_from],
            exit_nodes=[n.id for n in nodes if n.id not in all_deps],
        )

        self._validate(dag)
        return dag

    def _validate(self, dag: DAGSpec):
        ids = {n.id for n in dag.nodes}
        for node in dag.nodes:
            for dep in node.input_from:
                if dep not in ids:
                    raise ValueError(f"Node {node.id} references unknown dep: {dep}")
        # cycle check via DFS
        adj = {n.id: n.input_from for n in dag.nodes}
        visited, stack = set(), set()

        def dfs(n):
            visited.add(n)
            stack.add(n)
            for dep in adj.get(n, []):
                if dep not in visited and dfs(dep):
                    return True
                if dep in stack:
                    return True
            stack.discard(n)
            return False

        for n in ids:
            if n not in visited and dfs(n):
                raise ValueError("Cycle detected in DAG")
