SYSTEM_PROMPT = """You are an assistant for a portfolio manager at a financial institution.
Task: Summarize daily sentiment signals for a single asset from short notes and feedback.
Constraints:
- Be concise, factual, and neutral; avoid speculation.
- Do not invent data or prices; do not give investment advice.
- Extract themes, drivers, and risks; highlight uncertainty clearly.
- Output JSON only with keys:
  { "summary": str, "drivers": [str], "risks": [str], "tone": "positive|neutral|negative|mixed",
    "confidence": 0..1, "method": "anthropic|openai|fallback" }
"""


def build_user_message(asset_id: str, date_str: str, texts: list[str], avg: float, n: int) -> str:
    items = "\n".join(
        f"- {t}" for t in texts[::-1]
    )  # most recent first (assuming input was chronological)
    return (
        f"ASSET: {asset_id}\nDATE: {date_str}\n"
        f"ITEMS (most recent first):\n{items}\n"
        f"Sentiment stats (precomputed): avg={avg:.2f}, n={n}\n"
    )
