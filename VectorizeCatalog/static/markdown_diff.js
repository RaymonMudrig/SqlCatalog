// ============================================================================
// MARKDOWN & DIFF RENDERING
// ============================================================================

// Get promptInput reference (will be set when DOM is loaded)
const promptInput = document.getElementById('semantic-prompt');

function renderMarkdown(text) {
  if (!text) return '(no answer)';

  try {
    // Use marked.js to render markdown
    return marked.parse(text);
  } catch (e) {
    console.error('Markdown rendering failed:', e);
    return text;
  }
}

function renderDiff(unifiedDiff) {
  if (!unifiedDiff) return '';

  try {
    const html = Diff2Html.html(unifiedDiff, {
      drawFileList: true,
      outputFormat: 'side-by-side',
      matching: 'words',
      highlight: true
    });
    return `<div class="diff-container">${html}</div>`;
  } catch (e) {
    console.error('Diff rendering failed:', e);
    return `<div class="diff-container"><pre>${unifiedDiff}</pre></div>`;
  }
}

function highlightSQL(element) {
  if (typeof hljs === 'undefined') return;

  const codeBlocks = element.querySelectorAll('pre code');
  codeBlocks.forEach(block => {
    const content = block.textContent.toLowerCase();
    const isSQL = block.classList.contains('language-sql') ||
                 content.includes('select') || content.includes('create') ||
                 content.includes('insert') || content.includes('update') ||
                 content.includes('delete') || content.includes('from') ||
                 content.includes('alter') || content.includes('drop') ||
                 content.includes('declare');

    if (isSQL) {
      block.classList.add('language-sql');
      delete block.dataset.highlighted;
      try {
        hljs.highlightElement(block);
      } catch (e) {
        console.error('Syntax highlighting failed:', e);
      }
    }
  });
}

function makeEntitiesClickable(element) {
  // Find all <code> elements (markdown backticks render as <code>)
  const codeElements = element.querySelectorAll('code');

  codeElements.forEach(codeEl => {
    const text = codeEl.textContent.trim();

    // Match entity patterns like: dbo.TableName, schema.ProcName, TableName.ColumnName, etc.
    // Allow spaces within names (e.g., "dbo.BO Client Cash")
    const entityPattern = /^([a-zA-Z_][\w\s]*\.)?[a-zA-Z_][\w\s]*(\.[a-zA-Z_][\w\s]*)?$/;

    if (entityPattern.test(text)) {
      // Create a clickable span
      const span = document.createElement('span');
      span.className = 'clickable-entity';
      span.textContent = text;
      span.title = `Click to add "${text}" to prompt`;

      span.addEventListener('click', (e) => {
        e.preventDefault();
        const currentValue = promptInput.value.trim();

        // Wrap entity in backticks to avoid ambiguity with spaces
        const wrappedText = '`' + text + '`';

        // Append entity to prompt with a space if needed
        if (currentValue) {
          promptInput.value = currentValue + ' ' + wrappedText;
        } else {
          promptInput.value = wrappedText;
        }

        // Focus the prompt input
        promptInput.focus();

        // Move cursor to end
        promptInput.setSelectionRange(promptInput.value.length, promptInput.value.length);
      });

      // Replace the code element's content with our clickable span
      codeEl.innerHTML = '';
      codeEl.appendChild(span);
    }
  });
}

// Make functions globally available
window.renderMarkdown = renderMarkdown;
window.renderDiff = renderDiff;
window.highlightSQL = highlightSQL;
window.makeEntitiesClickable = makeEntitiesClickable;
