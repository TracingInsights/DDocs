from google import genai
from google.genai import types

async def generate_answer_stream(rewritten_query: str, context_docs: list[str]):
    """Yields chunks of the generated answer from Gemini."""
    context_str = "\n\n---\n\n".join(context_docs[:5])
    prompt = f"FIA Documents:\n{context_str}\n\nQuestion: {rewritten_query}\nAnswer:"
    client = genai.Client()

    response = await client.aio.models.generate_content_stream(
        model='gemini-2.5-flash',
        contents=prompt,
        config=types.GenerateContentConfig(max_output_tokens=1500)
    )
    async for chunk in response:
        if chunk.text:
            yield chunk.text
