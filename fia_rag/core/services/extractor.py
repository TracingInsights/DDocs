import json
from google import genai
from google.genai import types
async def llm_extract_fields(md_text: str) -> dict:
    """Uses Gemini Flash Lite to extract structured fields from rich documents."""
    prompt = f"""You are a FIA Formula 1 document parser.
Extract the following fields from this FIA document as JSON.
Return ONLY valid JSON with these exact keys. Use null if not found.

Fields:
- driver_name (string): Full name of the driver involved
- car_number (string): Car number (e.g. "44")
- team (string): Constructor name
- charge_description (string): The alleged offence or charge
- charge_normalized (string): Short normalized category (e.g. "track limits", "unsafe release")
- regulation_articles (array of strings): FIA regulation articles cited
- heard (boolean): Was a hearing held?
- decision (string): "penalty" | "no further action" | "reprimand" | "disqualified" | null
- penalty_type (string): "time penalty" | "grid penalty" | "fine" | "drive-through" | null
- penalty_value (string): The numeric or textual value of the penalty
- penalty_unit (string): "seconds" | "positions" | "euros" | null
- stewards (array of strings): Names of the stewards

Document:
---
{md_text[:4000]}
---
JSON:"""

    client = genai.Client()
    response = await client.aio.models.generate_content(
        model='gemini-2.5-flash-lite',
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json", 
            max_output_tokens=512
        )
    )
    try:
        return json.loads(response.text)
    except Exception:
        return {}
