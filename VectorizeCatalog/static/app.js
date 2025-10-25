// app.js - Unified SQL Catalog Frontend
const API_BASE = '';
let sessionId = null;
let clusterData = null;

// State tracking for navigation
let lastContentState = {
  entities: null,  // Will store last semantic search result HTML
  clusters: null   // Will store last cluster view HTML
};

let currentTab = 'entities';
let currentClusterId = null; // Track current cluster being viewed
let selectedEntity = null; // Track selected entity in diagram
let currentZoomLevel = 1.0; // Track zoom level

// ============================================================================
// DOM ELEMENTS
// ============================================================================

const promptInput = document.getElementById('semantic-prompt');
const promptSubmit = document.getElementById('prompt-submit');
const contentArea = document.getElementById('content-area');
const statusMessage = document.getElementById('status-message');
const notificationBar = document.getElementById('notification-bar');
const notificationMessage = document.querySelector('.notification-message');

// Tab elements
const tabButtons = document.querySelectorAll('.tab-btn');
const tabContents = document.querySelectorAll('.tab-content');

// Entity tab
const entitySearch = document.getElementById('entity-search');
const clearEntitiesBtn = document.getElementById('clear-entities');

// Cluster tab
const clusterList = document.getElementById('cluster-list');

// ============================================================================
// TAB SWITCHING
// ============================================================================

async function switchTab(tabName) {
  // Save current content state before switching
  if (currentTab && contentArea.innerHTML) {
    lastContentState[currentTab] = {
      html: contentArea.innerHTML,
      status: statusMessage.textContent,
      statusType: statusMessage.className
    };
  }

  currentTab = tabName;

  tabButtons.forEach(btn => {
    if (btn.dataset.tab === tabName) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });

  tabContents.forEach(content => {
    if (content.id === `tab-${tabName}`) {
      content.classList.add('active');
    } else {
      content.classList.remove('active');
    }
  });

  // Restore last content state if available
  if (lastContentState[tabName]) {
    contentArea.innerHTML = lastContentState[tabName].html;
    statusMessage.textContent = lastContentState[tabName].status;
    statusMessage.className = lastContentState[tabName].statusType;

    // Re-attach event handlers for cluster controls
    if (tabName === 'clusters') {
      attachClusterEventHandlers();
    }
  } else {
    // No previous state, show default content
    if (tabName === 'clusters') {
      await showClusterSummary();
    } else if (tabName === 'entities') {
      showWelcome();
    }
  }
}

tabButtons.forEach(btn => {
  btn.addEventListener('click', () => {
    switchTab(btn.dataset.tab);
  });
});

// ============================================================================
// STATUS UPDATES & NOTIFICATIONS
// ============================================================================

function setStatus(message, type = 'info') {
  statusMessage.textContent = message;
  statusMessage.className = type;
}

let notificationTimeout = null;

function showNotification(message, type = 'loading', autoHide = true) {
  // Clear any existing timeout
  if (notificationTimeout) {
    clearTimeout(notificationTimeout);
    notificationTimeout = null;
  }

  // Update notification content
  notificationMessage.textContent = message;

  // Set notification type
  notificationBar.className = 'notification-bar ' + type;

  // Auto-hide success notifications after 3 seconds
  if (autoHide && type === 'success') {
    notificationTimeout = setTimeout(() => {
      hideNotification();
    }, 3000);
  }
}

function hideNotification() {
  notificationBar.classList.add('hidden');
  if (notificationTimeout) {
    clearTimeout(notificationTimeout);
    notificationTimeout = null;
  }
}

// Make hideNotification globally accessible for onclick handler
window.hideNotification = hideNotification;

function showWelcome() {
  contentArea.innerHTML = `
    <div id="welcome">
      <h2>Welcome to SQL Catalog Analysis</h2>
      <p>Use the semantic prompt above to:</p>
      <ul>
        <li>Search for entities: "show me order tables"</li>
        <li>Compare SQL: "compare dbo.Order with dbo.Order_Archive"</li>
        <li>Manage clusters: "rename cluster C1 to Orders"</li>
        <li>View relationships: "which procedures access Order table"</li>
      </ul>
    </div>
  `;
  setStatus('Ready', 'success');
}

// ============================================================================
// ENTITY MEMORY
// ============================================================================

function updateEntityMemory(memory) {
  const sections = {
    'tables': document.querySelector('[data-kind="tables"] .entity-list'),
    'procedures': document.querySelector('[data-kind="procedures"] .entity-list'),
    'views': document.querySelector('[data-kind="views"] .entity-list'),
    'functions': document.querySelector('[data-kind="functions"] .entity-list')
  };

  for (const [kind, listEl] of Object.entries(sections)) {
    const entities = memory[kind] || [];
    const section = listEl.closest('.entity-section');
    const countEl = section.querySelector('.count');

    countEl.textContent = entities.length;

    if (entities.length === 0) {
      listEl.innerHTML = `<li class="empty">No ${kind}</li>`;
    } else {
      listEl.innerHTML = entities.map(name =>
        `<li data-entity="${name}">${name}</li>`
      ).join('');
    }
  }

  // Add click handlers
  document.querySelectorAll('.entity-list li:not(.empty)').forEach(item => {
    item.addEventListener('click', () => {
      const entity = item.dataset.entity;
      promptInput.value = (promptInput.value.trim() + ' `' + entity + '`').trim();
      promptInput.focus();
    });
  });
}

clearEntitiesBtn.addEventListener('click', async () => {
  if (!sessionId) return;

  try {
    const res = await fetch(`${API_BASE}/api/qcat/clear_memory`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId })
    });
    const data = await res.json();

    if (data.ok) {
      updateEntityMemory({ tables: [], procedures: [], views: [], functions: [] });
      setStatus('Entity memory cleared', 'success');
    }
  } catch (e) {
    console.error('Failed to clear memory:', e);
    setStatus('Failed to clear memory', 'error');
  }
});

// ============================================================================
// SEMANTIC SEARCH
// ============================================================================

async function executeCommand() {
  const command = promptInput.value.trim();
  if (!command) return;

  promptSubmit.disabled = true;
  showNotification('Processing query...', 'loading', false);
  setStatus('Processing...', 'info');

  try {
    const res = await fetch(`${API_BASE}/api/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command, session_id: sessionId })
    });

    // Check if response is JSON
    const contentType = res.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
      const text = await res.text();
      throw new Error(`Server returned non-JSON response: ${text.substring(0, 200)}`);
    }

    const data = await res.json();

    if (data.type === 'qcat') {
      // Semantic search result
      const result = data.result;
      sessionId = result.session_id;

      // Switch to Entities tab if not already there
      if (currentTab !== 'entities') {
        await switchTab('entities');
      }

      // Update entity memory
      if (result.memory) {
        updateEntityMemory(result.memory);
      }

      // Display answer
      let html = '';
      if (result.answer) {
        html += renderMarkdown(result.answer);
      }

      // Display diff if present
      if (result.unified_diff) {
        html += renderDiff(result.unified_diff);
      }

      contentArea.innerHTML = html;

      // Apply syntax highlighting
      highlightSQL(contentArea);

      // Make entities clickable
      makeEntitiesClickable(contentArea);

      showNotification('Query completed successfully', 'success', true);
      setStatus('Query completed', 'success');

      // Save state
      lastContentState.entities = {
        html: contentArea.innerHTML,
        status: statusMessage.textContent,
        statusType: statusMessage.className
      };
    } else if (data.type === 'cluster') {
      // Cluster command result
      const result = data.result;

      if (result.status === 'ok') {
        // Display the result answer if present
        if (result.answer) {
          contentArea.innerHTML = renderMarkdown(result.answer);
        }

        showNotification(result.message || 'Command executed successfully', 'success', true);
        setStatus(result.message || 'Command executed', 'success');

        // Reload cluster list
        await loadClusterList();

        // Reload trash (to show newly deleted items or reflect changes)
        if (window.loadTrash) {
          await window.loadTrash();
        }

        // Refresh the current diagram view
        if (currentTab === 'clusters') {
          if (currentClusterId) {
            // If viewing a specific cluster detail, reload it
            await window.showCluster(currentClusterId);
          } else {
            // If viewing summary, refresh summary
            await window.showClusterSummary();
          }
        }
      } else {
        showNotification(result.message || 'Command failed', 'error', false);
        setStatus(result.message || 'Command failed', 'error');
      }
    } else if (data.type === 'error') {
      // Error case (e.g., LLM timeout, needs_confirmation)
      const result = data.result;

      // Display error message or help text
      if (result.answer) {
        contentArea.innerHTML = renderMarkdown(result.answer);
      }

      showNotification(result.message || 'Could not process command', 'error', false);
      setStatus(result.message || 'Command needs clarification', 'error');
    } else {
      // Unknown response type
      showNotification('Unexpected response from server', 'error', false);
      setStatus('Unexpected response', 'error');
    }
  } catch (e) {
    console.error('Command failed:', e);
    showNotification('Command failed: ' + e.message, 'error', false);
    setStatus('Command failed: ' + e.message, 'error');
  } finally {
    promptSubmit.disabled = false;
  }
}

promptSubmit.addEventListener('click', executeCommand);
promptInput.addEventListener('keydown', (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
    executeCommand();
  }
});

// ============================================================================
// INITIALIZATION
// ============================================================================

async function init() {
  // Show welcome message
  showWelcome();

  // Load cluster list on startup
  await loadClusterList();
}

// Export init so it can be called after all modules load
window.initApp = init;

// ============================================================================
// MAKE FUNCTIONS AND VARIABLES GLOBALLY AVAILABLE FOR OTHER MODULES
// ============================================================================

// Functions
window.setStatus = setStatus;
window.showNotification = showNotification;
window.executeCommand = executeCommand;

// DOM elements
window.contentArea = contentArea;
window.statusMessage = statusMessage;
window.clusterList = clusterList;
window.promptInput = promptInput;

// Global state - use getters/setters to ensure modules can read/write shared state
Object.defineProperty(window, 'API_BASE', {
  get: () => API_BASE,
  set: (value) => { API_BASE = value; }
});

Object.defineProperty(window, 'currentClusterId', {
  get: () => currentClusterId,
  set: (value) => { currentClusterId = value; }
});

Object.defineProperty(window, 'selectedEntity', {
  get: () => selectedEntity,
  set: (value) => { selectedEntity = value; }
});

Object.defineProperty(window, 'currentZoomLevel', {
  get: () => currentZoomLevel,
  set: (value) => { currentZoomLevel = value; }
});

Object.defineProperty(window, 'currentTab', {
  get: () => currentTab,
  set: (value) => { currentTab = value; }
});

Object.defineProperty(window, 'lastContentState', {
  get: () => lastContentState,
  set: (value) => { lastContentState = value; }
});

Object.defineProperty(window, 'clusterData', {
  get: () => clusterData,
  set: (value) => { clusterData = value; }
});
