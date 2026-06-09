import asyncio
from google import genai
from google.genai import types

async def main():
    client = genai.Client()
    response = client.models.embed_content(
        model='gemini-embedding-exp-03-07',
        contents="Hello world",
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY")
    )
    print(response.embeddings[0].values[:5])
    
    response2 = await client.aio.models.generate_content(
        model='gemini-2.5-flash-lite',
        contents="Say hi",
        config=types.GenerateContentConfig(max_output_tokens=10)
    )
    print(response2.text)

asyncio.run(main())
