"""
Test if passing list[Content] objects returns multiple embeddings,
and check EmbeddingApiType values.
"""
from google import genai
from google.genai import types
import inspect

client = genai.Client(api_key='AIzaSyAUSNquY_UurJSu5fnM89wlhhNEvq8ZZd8')

test_texts = ["Hello world", "FIA regulations", "Formula One race"]

# Check EmbeddingApiType values
print("=== EmbeddingApiType values ===")
try:
    for name, val in vars(types.EmbeddingApiType).items():
        if not name.startswith('_'):
            print(f"  {name} = {val!r}")
except Exception as e:
    print(f"Error: {e}")

print()

# Try passing list[Content] objects (each Content = separate document)
print("=== Approach: list[Content] objects ===")
try:
    contents_list = [
        types.Content(parts=[types.Part(text=t)])
        for t in test_texts
    ]
    result = client.models._embed_content(
        model="gemini-embedding-2-preview",
        contents=contents_list,
        config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
    )
    print(f"Texts sent    : {len(test_texts)}")
    print(f"Embeddings got: {len(result.embeddings)}")
    if len(result.embeddings) == len(test_texts):
        print("SUCCESS: list[Content] returns one embedding per document!")
    else:
        print("Still only 1 embedding — API truly only embeds 1 doc per call.")
except Exception as e:
    print(f"Failed: {e}")

print()

# Try with embedding_api_type = GENAI_API
print("=== Approach: embedding_api_type variations ===")
for api_type_name in ['GENAI_API', 'VERTEX_AI', None]:
    try:
        api_type = getattr(types.EmbeddingApiType, api_type_name) if api_type_name else None
        result = client.models._embed_content(
            model="gemini-embedding-2-preview",
            contents=test_texts,
            embedding_api_type=api_type,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        print(f"embedding_api_type={api_type_name}: {len(result.embeddings)} embedding(s)")
    except Exception as e:
        print(f"embedding_api_type={api_type_name}: Error — {e}")
