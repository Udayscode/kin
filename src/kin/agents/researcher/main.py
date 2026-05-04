import asyncio
import os
import re

import httpx
from ddgs import DDGS
from dotenv import load_dotenv
from groq import Groq

from kin.agents.base.agent import BaseAgent
from kin.models.schemas import TaskMessage
from kin.observability.logging import get_logger

log = get_logger("kin.researcher")

# Tags whose content we completely skip when extracting text
_SKIP_TAGS = re.compile(
    r"<(script|style|nav|header|footer|aside|form|button)[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)
_HTML_TAG = re.compile(r"<[^>]+>")
_WHITESPACE = re.compile(r"\s{2,}")


def _extract_text(html: str, max_chars: int = 1500) -> str:
    html = _SKIP_TAGS.sub(" ", html)
    text = _HTML_TAG.sub(" ", html)
    text = _WHITESPACE.sub(" ", text).strip()
    return text[:max_chars]


async def _fetch_url(client: httpx.AsyncClient, url: str) -> str:
    try:
        r = await client.get(url, timeout=6, follow_redirects=True)
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
            return _extract_text(r.text)
    except Exception as e:
        log.debug("Fetch failed for %s: %s", url, e)
    return ""


def _extract_queries(task_description: str) -> list[str]:
    """Extract 2-3 short, search-friendly queries from the task description."""
    clean = re.sub(r"\*+", "", task_description)
    sentences = re.split(r"[.\n—]+", clean)
    queries = []
    for s in sentences:
        s = s.strip()
        if len(s) > 15:
            queries.append(s[:80])
        if len(queries) == 3:
            break
    return queries or [task_description[:80].strip()]


def _ddgs_search(query: str, max_results: int = 5) -> list[dict]:
    """Return list of {title, snippet, url} dicts from DDGS."""
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                results.append({
                    "title": r.get("title", ""),
                    "snippet": r.get("body", ""),
                    "url": r.get("href", ""),
                })
    except Exception as e:
        log.debug("DDGS error for %r: %s", query[:40], e)
    return results


async def deep_search(queries: list[str]) -> tuple[str, list[str]]:
    """
    Run queries concurrently, collect URLs, fetch page content in parallel.
    Returns (combined_content, source_urls).
    """
    # 1. Run all searches concurrently
    raw = await asyncio.gather(*[asyncio.to_thread(_ddgs_search, q) for q in queries])

    # Deduplicate results by URL
    seen: set[str] = set()
    items: list[dict] = []
    for batch in raw:
        for item in batch:
            if item["url"] and item["url"] not in seen:
                seen.add(item["url"])
                items.append(item)

    if not items:
        return "", []

    source_urls = [i["url"] for i in items]

    # 2. Fetch top 4 pages concurrently for full content
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; KinBot/1.0)"},
        follow_redirects=True,
    ) as client:
        page_texts = await asyncio.gather(
            *[_fetch_url(client, item["url"]) for item in items[:2]]  # top 2 only
        )

    # 3. Build final content block: snippet always included, full page if fetched
    parts = []
    for item, full_text in zip(items[:2], page_texts):
        block = f"### {item['title']}\nSource: {item['url']}\nSnippet: {item['snippet']}"
        if full_text:
            block += f"\nFull Content:\n{full_text}"
        parts.append(block)

    # Append remaining snippet-only results (up to 3 more)
    for item in items[2:5]:
        parts.append(
            f"### {item['title']}\nSource: {item['url']}\nSnippet: {item['snippet']}"
        )

    combined = "\n\n---\n\n".join(parts)
    # Hard cap: keep total under ~6000 chars to control token usage
    return combined[:6000], source_urls


class ResearcherAgent(BaseAgent):
    def __init__(self, api_key: str, **kwargs):
        super().__init__(agent_type="researcher", **kwargs)
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"

    async def process(self, task: TaskMessage) -> dict:
        log.info("RESEARCHER task -> %s...", task.task_description[:60])

        queries = _extract_queries(task.task_description)
        log.debug("Queries: %s", queries)

        search_data, source_urls = await deep_search(queries)
        log.info("Search: %d chars from %d sources", len(search_data), len(source_urls))

        system_prompt = (
            "You are a Senior Research Analyst with access to live web content. "
            "Synthesize the SEARCH DATA into a detailed, structured research note that directly "
            "addresses the TASK. Cite specific facts, numbers, dates, and source URLs. "
            "Prioritize data from 'Full Content' sections over snippets. "
            "Be precise and comprehensive — this note will feed a final written report."
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
                            f"SEARCH DATA:\n{search_data or 'No search results — use internal knowledge.'}"
                        ),
                    },
                ],
            )

            return {
                "agent_type": "researcher",
                "data": {
                    "research_content": response.choices[0].message.content,
                    "sources": source_urls,
                },
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                } if getattr(response, "usage", None) else None,
                "status": "success",
            }

        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                raise  # let Temporal retry after backoff
            log.error("Researcher LLM error: %s", e)
            return {
                "agent_type": "researcher",
                "data": {"research_content": search_data, "sources": source_urls},
                "status": "partial_success",
            }


if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY not found in environment")
    agent = ResearcherAgent(api_key=api_key)
    asyncio.run(agent.start())
