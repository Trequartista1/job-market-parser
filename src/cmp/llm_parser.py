import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def parse_with_llm(text: str):

    prompt = f"""
    Extract job search parameters from user query.

    Return JSON:
    {{
      "skills": [],
      "role": "",
      "remote": false
    }}

    Query: "{text}"
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content