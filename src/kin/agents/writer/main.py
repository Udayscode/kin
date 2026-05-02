import asyncio
import os
import google.generativeai as genai
from groq import Groq
from kin.agents.base.agent import BaseAgent
from kin.models.schemas import TaskMessage
from dotenv import load_dotenv


class WriterAgent(BaseAgent):
    def __init__(self, api_key: str, **kwargs):
        super().__init__(agent_type="writer", **kwargs)
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"

    async def process(self, task: TaskMessage) -> dict:
        """
        Takes research data and formats it into professional Markdown.
        """
        # The 'task_description' here will actually be the research blob
        # passed from the ResearcherAgent via the Orchestrator.

        system_prompt = (
            "You are a professional technical writer. Your goal is to take raw research "
            "data and organize it into a structured, clean, and engaging Markdown report. "
            "Use headers, bullet points, and bold text for readability."
        )

        user_content = f"Please transform this research into a Markdown report:\n\n{task.task_description}"

        try:
            # Switched to generate_content_async
            response = await asyncio.to_thread(
                self.client.chat.completions.create,
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
            )

            return {
                "agent_type": "writer",
                "output_file": "report.md",
                "markdown": response.choices[0].message.content,
                "status": "success",
            }
        except Exception as e:
            print(f"Writer Gemini Error: {e}")
            return {
                "agent_type": "writer",
                "markdown": f"Error: {str(e)}",
                "status": "failed",
            }


if __name__ == "__main__":
    load_dotenv()
    # Testing logic
    agent = WriterAgent(api_key=os.getenv("GROQ_API_KEY"))
    mock_task = TaskMessage(
        workflow_id="test",
        node_id="writer-node",
        agent_type="writer",
        task_description="Research: AI is cool. Source: internet. Content: it helps code.",
    )
    asyncio.run(agent.start())
