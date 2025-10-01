const $ = sel => document.querySelector(sel);

function showAnswer(text) {
  $('#answer').textContent = text || '(no answer)';
}
function showParsed(obj) {
  $('#parsed').textContent = JSON.stringify(obj || {}, null, 2);
}
function showRaw(obj) {
  $('#raw').textContent = JSON.stringify(obj || {}, null, 2);
}

async function ask() {
  const prompt = $('#q').value || '';
  if (!prompt.trim()) return;

  showAnswer('Thinking...');
  showParsed({});
  showRaw({});

  try {
    const r = await fetch('/api/ask', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ prompt })
    });
    const txt = await r.text();
    let data;
    try { data = JSON.parse(txt); } catch { data = { answer: txt }; }
    showAnswer(data.answer);
    showParsed(data.parsed);
    showRaw(data.raw);
  } catch (err) {
    showAnswer(String(err));
  }
}

window.addEventListener('DOMContentLoaded', () => {
  $('#ask').addEventListener('click', ask);
  $('#q').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') ask();
  });
});
