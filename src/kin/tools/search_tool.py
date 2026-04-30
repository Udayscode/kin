from ddgs import DDGS

def web_search(query: str, max_results: int = 5):
    """
    Performs a free web search.
    """
    results = []
    try:
        with DDGS() as ddgs:
            ddgs_gen = ddgs.text(query, max_results=max_results)
            for r in ddgs_gen:
                results.append({
                    "title": r.get('title'),
                    "content": r.get('body'),
                    "url": r.get('href')
                })
    except Exception as e:
        print(f"Search error: {e}")
    return results