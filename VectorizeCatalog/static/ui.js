(() => {
  const input = document.getElementById("prompt");
  const btn = document.getElementById("askBtn");
  const status = document.getElementById("status");
  const answer = document.getElementById("answer");
  const diffBox = document.getElementById("diff");

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

  function renderDiff(unifiedDiff) {
    if (!unifiedDiff) {
      diffBox.hidden = true;
      diffBox.innerHTML = "";
      return;
    }
    try {
      const html = Diff2Html.html(unifiedDiff, {
        drawFileList: false,
        matching: 'lines',
        outputFormat: 'side-by-side',
      });
      const clean = DOMPurify.sanitize(html, { ADD_ATTR: ['class', 'style'] });
      diffBox.innerHTML = clean;
      diffBox.hidden = false;
    } catch (e) {
      console.error(e);
      diffBox.hidden = false;
      diffBox.innerHTML = "<pre>" + (unifiedDiff || "").replace(/[&<>]/g, s => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[s])) + "</pre>";
    }
  }

  async function ask(prompt) {
    const q = (prompt || "").trim();
    if (!q) return;
    btn.disabled = true;
    status.textContent = "Thinking…";
    answer.innerHTML = "";
    renderDiff(null);

    try {
      const res = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: q })
      });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const data = await res.json();
      const mdText =
        data.answer || data.markdown || data.text ||
        (typeof data === "string" ? data : JSON.stringify(data, null, 2));
      renderMarkdown(mdText);
      renderDiff(data.unified_diff || null);
      status.textContent = "";
    } catch (err) {
      status.textContent = "Error: " + (err.message || err);
      renderMarkdown("**Request failed.**\n\n```\n" + (err.stack || err) + "\n```");
      renderDiff(null);
    } finally {
      btn.disabled = false;
    }
  }

  btn.addEventListener("click", () => ask(input.value));
  input.addEventListener("keydown", (e) => {
    const isSubmitCombo = (e.ctrlKey || e.metaKey) && e.key === "Enter";
    if (isSubmitCombo) {
      e.preventDefault();
      ask(input.value);
    }
  });
})();
