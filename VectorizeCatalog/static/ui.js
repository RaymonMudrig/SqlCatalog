(() => {
  const input = document.getElementById("prompt");
  const btn = document.getElementById("askBtn");
  const status = document.getElementById("status");
  const answer = document.getElementById("answer");

  // Markdown renderer
  const md = window.markdownit({ html: false, linkify: true, breaks: true });

  function renderMarkdown(markdownText) {
    try {
      const dirtyHtml = md.render(markdownText || "");
      const cleanHtml = DOMPurify.sanitize(dirtyHtml, { USE_PROFILES: { html: true } });
      answer.innerHTML = cleanHtml;
      answer.querySelectorAll("a[href]").forEach(a => {
        a.setAttribute("target", "_blank");
        a.setAttribute("rel", "noopener noreferrer");
      });
      answer.querySelectorAll('pre code').forEach(block => {
        try { window.hljs.highlightElement(block); } catch {}
      });
    } catch {
      answer.textContent = markdownText || "";
    }
  }

  async function ask(prompt) {
    const q = (prompt || "").trim();
    if (!q) return;
    btn.disabled = true;
    status.textContent = "Thinking…";
    answer.innerHTML = "";

    try {
      const res = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: q })
      });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const data = await res.json();
      const mdText = data.answer || data.markdown || data.text || (typeof data === "string" ? data : JSON.stringify(data, null, 2));
      renderMarkdown(mdText);
      status.textContent = "";
    } catch (err) {
      status.textContent = "Error: " + (err.message || err);
      renderMarkdown("**Request failed.**\n\n```\n" + (err.stack || err) + "\n```");
    } finally {
      btn.disabled = false;
    }
  }

  // Button click
  btn.addEventListener("click", () => ask(input.value));

  // Keyboard:
  // - Enter inserts a newline (default)
  // - Ctrl/Cmd + Enter sends
  input.addEventListener("keydown", (e) => {
    const isSubmitCombo = (e.ctrlKey || e.metaKey) && e.key === "Enter";
    if (isSubmitCombo) {
      e.preventDefault();
      ask(input.value);
    }
  });
})();
