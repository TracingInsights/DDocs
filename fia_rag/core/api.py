from django.http import HttpRequest
from ninja import NinjaAPI
from ninja.security import HttpBearer
from jose import jwt, JWTError
from django.conf import settings
from django.utils import timezone
from core.schema import ChatMessageIn, ChatMessageOut, StreamChunk
from core.services.router import rewrite_query, classify_query, extract_query_entities
from core.services.retrieval import (sql_first_search, fetch_docs_by_ids,
    build_context_from_chunks, vector_index, bm25_index, reciprocal_rank_fusion)
from core.models import Message, ChatSession, User
from google import genai
from google.genai import types
import asyncio

class GlobalAuth(HttpBearer):
    def authenticate(self, request, token):
        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("sub")
            if user_id:
                request.user = User.objects.get(id=user_id)
                return request.user
        except (JWTError, Exception):
            return None

api = NinjaAPI(auth=GlobalAuth())

class QuotaExceeded(Exception):
    pass

@api.exception_handler(QuotaExceeded)
def quota_exceeded_handler(request, exc):
    return api.create_response(request, {"detail": "Daily query quota exceeded"}, status=429)

def enforce_quota(request):
    user = request.user
    if user.quota_reset_date < timezone.now().date():
        user.queries_today = 0
        user.quota_reset_date = timezone.now().date()
        user.save(update_fields=["queries_today", "quota_reset_date"])
    if user.queries_today >= user.daily_quota:
        raise QuotaExceeded()
    user.queries_today += 1
    user.save(update_fields=["queries_today"])

@api.post("/chat/stream")
async def chat_stream(request: HttpRequest, payload: ChatMessageIn):
    enforce_quota(request)

    # 1. Rewrite for multi-turn
    rewritten   = await rewrite_query(payload.session_id, payload.query)
    query_type  = classify_query(rewritten)
    entities    = await extract_query_entities(rewritten)

    # 2. Retrieve context
    context_docs = []
    if query_type == "AGGREGATION":
        # Generate and execute DuckDB SQL (Placeholder for actual generation logic)
        # sql_result = await generate_aggregation_sql(rewritten, entities)
        # context_docs = [str(sql_result)]
        context_docs = ["SQL execution placeholder"]
    else:
        # Try SQL-first (structured filter)
        doc_ids = sql_first_search(entities)
        if doc_ids:
            context_docs = fetch_docs_by_ids(doc_ids)
        else:
            # Hybrid BM25 + Vector with RRF
            bm25_ids = bm25_index.search(rewritten)
            vec_ids  = vector_index.search(rewritten)
            chunk_ids = reciprocal_rank_fusion(bm25_ids, vec_ids)
            context_docs = build_context_from_chunks(chunk_ids)

    # 3. Stream answer from Gemini
    context_str = "\\n\\n---\\n\\n".join(context_docs[:5])
    prompt = f"FIA Documents:\\n{context_str}\\n\\nQuestion: {rewritten}\\nAnswer:"
    client = genai.Client()

    async def event_gen():
        full = ""
        response = await client.aio.models.generate_content_stream(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=1500)
        )
        async for chunk in response:
            if chunk.text:
                full += chunk.text
                yield {"token": chunk.text, "done": False}

        # Persist to DB
        await Message.objects.acreate(session_id=payload.session_id, role="user",   content=payload.query)
        await Message.objects.acreate(session_id=payload.session_id, role="assistant", content=full)
        yield {"token": "", "done": True, "sources": []}

    return event_gen()
