import json
import os
from typing import Any, Dict

import anthropic

from ..insights.prompt_templates import SYSTEM_PROMPT, build_user_message


class ClaudeAdapter:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model = os.getenv("SPECTORR_ANTHROPIC_MODEL", "claude-3-haiku-20240307")
        self.max_tokens = int(os.getenv("SPECTORR_MAX_TOKENS", "800"))
        self.temperature = float(os.getenv("SPECTORR_TEMPERATURE", "0.2"))

    def summarize(
        self, *, asset_id: str, date_str: str, texts: list[str], avg: float, n: int
    ) -> Dict[str, Any]:
        user = build_user_message(asset_id, date_str, texts, avg, n)
        msg = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user}],
        )
        raw = msg.content[0].text if msg.content else ""
        try:
            data = json.loads(raw)
        except Exception:
            data = {
                "summary": raw.strip()[:1000],
                "drivers": [],
                "risks": [],
                "tone": "mixed",
                "confidence": 0.4,
            }
        data["method"] = "anthropic"
        return {
            "asset_id": asset_id,
            "date": date_str,
            "avg_sentiment": round(avg, 3),
            "n": n,
            "insight": data,
        }
