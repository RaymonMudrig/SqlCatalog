from __future__ import annotations
import os, json, time
from typing import List, Dict, Any, Optional

# Environment (defaults target LM Studio)
API_BASE   = os.getenv("CHAT_API_BASE", "http://127.0.0.1:1234/v1")
API_KEY    = os.getenv("CHAT_API_KEY", "lm-studio")
CHAT_MODEL = os.getenv("CHAT_MODEL",   "qwen2.5-32b-instruct-mlx")
TIMEOUT_S  = float(os.getenv("CHAT_TIMEOUT", "120"))
TEMP       = float(os.getenv("CHAT_TEMPERATURE", "0.2"))
MAX_TOK    = int(os.getenv("CHAT_MAX_TOKENS", "800"))

def _post_chat(messages: List[Dict[str, str]], temperature: float = TEMP,
               max_tokens: int = MAX_TOK) -> Optional[str]:
    try:
        import requests
        r = requests.post(
            f"{API_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            data=json.dumps({
                "model": CHAT_MODEL,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "stream": False
            }),
            timeout=TIMEOUT_S,
        )
        r.raise_for_status()
        data = r.json()
        return (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    except Exception:
        return None

SYS_DEFAULT = (
    "You are an expert SQL catalog analyst. Answer concisely in Markdown. "
    "Prefer bullet points. If uncertain, say so plainly."
)

def chat(user_text: str, system: str = SYS_DEFAULT, temperature: float = TEMP, max_tokens: int = MAX_TOK) -> Optional[str]:
    messages = [{"role":"system","content":system},{"role":"user","content":user_text}]
    return _post_chat(messages, temperature=temperature, max_tokens=max_tokens)

def llm_answer(question: str, picked: List[Dict[str, Any]], system: str = SYS_DEFAULT) -> Optional[str]:
    """
    Summarize picked items into a helpful natural-language answer.
    """
    if not picked:
        return None
    lines = []
    for it in picked[:12]:
        kind = it.get("kind")
        schema = it.get("schema") or ""
        name = it.get("name") or it.get("safe_name")
        disp = f"{schema+'.' if schema else ''}{name}"
        status = it.get("status") or ""
        doc = (it.get("doc") or it.get("text") or "").strip().replace("\n", " ")
        cols = it.get("columns") or []
        refs = it.get("refs") or {}
        reads = refs.get("reads") or it.get("reads") or []
        writes= refs.get("writes")or it.get("writes")or []
        lines.append(f"- **{kind}** `{disp}` — status: {status}; cols: {', '.join([c.get('name') for c in cols[:6]])}{'…' if len(cols)>6 else ''}; reads: {', '.join(reads[:4])}{'…' if len(reads)>4 else ''}; writes: {', '.join(writes[:3])}{'…' if len(writes)>3 else ''}; doc: {doc[:280]}{'…' if len(doc)>280 else ''}")
    prompt = f"""Question:
{question}

Context (entities):
{chr(10).join(lines)}

Guidelines:
- If the question is about "which procedures access table X", list the procedures and how (READ/WRITE, via views if applicable).
- If it's about a column or table meaning, summarize purpose based on names/docs/relationships.
- Include concrete names; avoid raw JSON.
- If relevant, mention whether the table/column is unused or unreferenced.

Answer succinctly:"""
    return chat(prompt, system=system)
