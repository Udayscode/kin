import asyncio
import os
import google.generativeai as genai
from groq import Groq
from ddgs import DDGS
from kin.agents.base.agent import BaseAgent
from kin.models.schemas import TaskMessage
from dotenv import load_dotenv


def web_search(query: str, max_results: int = 3):
    """Performs a real-time web search to feed fresh data to Gemini."""
    results = []
    try:
        with DDGS() as ddgs:
            # We use the task description as the search query
            ddgs_gen = ddgs.text(query, max_results=max_results)
            for r in ddgs_gen:
                results.append(
                    f"Title: {r.get('title')}\nSnippet: {r.get('body')}\nURL: {r.get('href')}"
                )
    except Exception as e:
        print(f"Search Tool Error: {e}")
    return "\n\n".join(results)


class ResearcherAgent(BaseAgent):
    def __init__(self, api_key: str, **kwargs):
        super().__init__(agent_type="researcher", **kwargs)
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"

    async def process(self, task: TaskMessage) -> dict:
        print(f"[*] RESEARCHER: Processing task -> {task.task_description[:50]}...")

        # Use task description as the search query (first 120 chars as a focused query)
        search_query = task.task_description[:120].strip()
        search_data = web_search(search_query, max_results=5)
        print(f"[DEBUG] DDG returned {len(search_data)} chars")

        system_prompt = (
            "You are a Senior Research Analyst. Use the provided SEARCH DATA to answer the user request. "
            "If no search data is available, use your internal knowledge but acknowledge the limitation."
        )

        try:
            response = await asyncio.to_thread(
                self.client.chat.completions.create,
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": (
                            f"TASK: {task.task_description}\n\n"
                            f"SEARCH DATA:\n{search_data or 'No search results available.'}"
                        ),
                    },
                ],
            )

            return {
                "agent_type": "researcher",
                "data": {
                    "research_content": response.choices[0].message.content,
                    "sources": ["DuckDuckGo Live Search", "Groq AI"],
                },
                "status": "success",
            }

        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                raise  # let Temporal retry after backoff
            # only fallback for non-quota errors
            return {
                "agent_type": "researcher",
                "data": {"research_content": search_data, "sources": ["DuckDuckGo"]},
                "status": "partial_success",
            }


if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY not found in environment")

    agent = ResearcherAgent(api_key=api_key)
    asyncio.run(agent.start())
