from ninja import Schema

class ChatMessageIn(Schema):
    session_id: str
    query: str

class ChatMessageOut(Schema):
    answer: str
    query_type: str
    sources: list[str] = []

class StreamChunk(Schema):
    token: str
    done: bool = False
    sources: list[str] = []
