from google import genai

REWRITE_PROMPT = """
Given conversation history and a follow-up question, rewrite the follow-up
as a complete standalone question. Return ONLY the rewritten question.

History:
{history}

Follow-up: {question}
Rewritten:"""

async def rewrite_query(session_id: str, current_query: str) -> str:
    from core.models import Message
    msgs = await Message.objects.filter(
        session_id=session_id).order_by('-created_at').values_list('role', 'content')[:5]
    if not msgs:
        return current_query
    history = "\n".join(f"{role.capitalize()}: {content}" for role, content in reversed(msgs))
    client = genai.Client()
    resp = await client.aio.models.generate_content(
        model='gemini-2.5-flash-lite',
        contents=REWRITE_PROMPT.format(history=history, question=current_query)
    )
    return resp.text.strip()

def classify_query(query: str) -> str:
    q = query.lower()
    if any(kw in q for kw in ["how many", "count", "total", "most", "least"]): return "AGGREGATION"
    if any(kw in q for kw in ["compare", "vs", "versus", "difference"]): return "COMPARATIVE"
    if any(kw in q for kw in ["similar", "precedent", "find all", "list all"]): return "PRECEDENT"
    return "FACT_LOOKUP"

async def extract_query_entities(query: str) -> dict:
    """Fast entity extraction — extracts year, event, driver, car_number, doc_type."""
    import re
    entities = {}

    # Year: 4-digit number 2018-2026
    year_m = re.search(r'\b(201[89]|202[0-6])\b', query)
    if year_m:
        entities["year"] = int(year_m.group(1))

    # Car number
    car_m = re.search(r'\bcar\s+(\d{1,2})\b', query, re.IGNORECASE)
    if car_m:
        entities["car_number"] = car_m.group(1)

    # Doc type keywords
    if any(w in query.lower() for w in ["decision", "penalty", "penalised"]):
        entities["doc_type"] = "decision"
    elif any(w in query.lower() for w in ["summons", "summoned"]):
        entities["doc_type"] = "summons"
    elif "offence" in query.lower():
        entities["doc_type"] = "offence"

    # Known event names (partial match)
    known_events = [
        "bahrain", "saudi", "australia", "japan", "china", "miami",
        "monaco", "canada", "spain", "austria", "silverstone", "hungary",
        "belgium", "netherlands", "monza", "azerbaijan", "singapore",
        "austin", "mexico", "brazil", "las vegas", "qatar", "abu dhabi",
    ]
    for ev in known_events:
        if ev in query.lower():
            entities["event"] = ev
            break

    return entities
