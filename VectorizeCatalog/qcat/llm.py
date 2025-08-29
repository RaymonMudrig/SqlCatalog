import os, textwrap

CHAT_MODEL        = os.getenv("CHAT_MODEL")  # e.g., qwen2.5-32b-instruct-mlx
LMSTUDIO_BASE_URL = os.getenv("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
LMSTUDIO_API_KEY  = os.getenv("LMSTUDIO_API_KEY", "lm-studio")

def llm_answer(question: str, picked_items):
    if not CHAT_MODEL:
        return None
    try:
        import requests
    except Exception:
        return None

    ctx_lines = []
    for it in picked_items[:8]:
        ctx = (it.get("text") or "").strip().replace("\r", "")
        ctx = "\n".join(ctx.splitlines()[:40])
        ctx_lines.append(f"- {it.get('id')}: {ctx}")

    messages = [
        {"role": "system", "content": "You are a helpful SQL catalog assistant. Be concise and cite entity IDs you used."},
        {"role": "user", "content": textwrap.dedent(f"""
            Question: {question}

            Context (top results):
            {chr(10).join(ctx_lines)}

            Task: Answer the question directly. For relation questions, list entities and their roles (READ/WRITE),
            and include their IDs.
        """).strip()},
    ]
    url = f"{LMSTUDIO_BASE_URL.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}", "Content-Type": "application/json"}
    try:
        r = requests.post(url, headers=headers, json={"model": CHAT_MODEL, "messages": messages}, timeout=120)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return None
