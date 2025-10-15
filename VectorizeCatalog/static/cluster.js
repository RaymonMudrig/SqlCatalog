// ============================================================================
// CLUSTER MANAGEMENT
// ============================================================================

async function showClusterSummary() {
  try {
    currentClusterId = null; // Clear current cluster
    deselectEntity(); // Clear any entity selection

    // Load cluster summary diagram
    const svgRes = await fetch(`${API_BASE}/api/cluster/svg/summary`);
    const svg = await svgRes.text();

    contentArea.innerHTML = `
      <div class="cluster-view">
        <div class="cluster-header">
          <div class="cluster-breadcrumb">Summary</div>
          <div class="cluster-controls">
            <div class="zoom-controls">
              <button onclick="zoomIn()" title="Zoom In">+</button>
              <div class="zoom-level">100%</div>
              <button onclick="zoomOut()" title="Zoom Out">−</button>
              <button onclick="zoomReset()" title="Reset Zoom">⊙</button>
            </div>
            <button onclick="refreshSummary()" class="secondary">Refresh Summary</button>
            <button onclick="reloadFromSnapshot()">Reload from Snapshot</button>
          </div>
        </div>
        <div class="diagram-container">
          ${svg}
        </div>
      </div>
    `;

    setStatus('Viewing cluster summary', 'info');

    // Clear active cluster selection
    clusterList.querySelectorAll('li').forEach(li => {
      li.classList.remove('active');
    });

    // Initialize diagram interactions
    const svgElement = contentArea.querySelector('.diagram-container svg');
    initializeDiagram(svgElement);

    // Save state
    lastContentState.clusters = {
      html: contentArea.innerHTML,
      status: statusMessage.textContent,
      statusType: statusMessage.className
    };
  } catch (e) {
    console.error('Failed to load cluster summary:', e);
    setStatus('Failed to load cluster summary', 'error');
    contentArea.innerHTML = '<p>Failed to load cluster summary diagram</p>';
  }
}

async function loadClusterList() {
  try {
    const res = await fetch(`${API_BASE}/api/cluster/summary`);
    clusterData = await res.json();

    clusterList.innerHTML = '';
    clusterData.clusters.forEach(cluster => {
      const li = document.createElement('li');
      const name = cluster.display_name || cluster.cluster_id;
      li.textContent = `${name} (${cluster.cluster_id})`;
      li.dataset.clusterId = cluster.cluster_id;
      li.addEventListener('click', () => showCluster(cluster.cluster_id));
      clusterList.appendChild(li);
    });
  } catch (e) {
    console.error('Failed to load clusters:', e);
  }
}

async function showCluster(clusterId) {
  try {
    currentClusterId = clusterId;
    deselectEntity(); // Clear any entity selection

    // Load cluster diagram
    const svgRes = await fetch(`${API_BASE}/api/cluster/svg/${clusterId}`);
    const svg = await svgRes.text();

    // Get cluster display name if available
    const cluster = clusterData?.clusters?.find(c => c.cluster_id === clusterId);
    const displayName = cluster?.display_name || clusterId;

    contentArea.innerHTML = `
      <div class="cluster-view">
        <div class="cluster-header">
          <div class="cluster-breadcrumb">
            <a onclick="showClusterSummary()" style="cursor: pointer;">Summary</a>
            <span class="separator">→</span>
            <span>${displayName}</span>
          </div>
          <div class="cluster-controls">
            <div class="zoom-controls">
              <button onclick="zoomIn()" title="Zoom In">+</button>
              <div class="zoom-level">100%</div>
              <button onclick="zoomOut()" title="Zoom Out">−</button>
              <button onclick="zoomReset()" title="Reset Zoom">⊙</button>
            </div>
            <div class="rename-control">
              <input type="text" id="rename-cluster-input-inline" placeholder="New name...">
              <button onclick="renameCurrentCluster()">Rename</button>
            </div>
          </div>
        </div>
        <div class="diagram-container">
          ${svg}
        </div>
      </div>
    `;

    // Set the input value after DOM creation to avoid HTML escaping issues
    const renameInput = document.getElementById('rename-cluster-input-inline');
    if (renameInput) {
      renameInput.value = displayName;
    }

    setStatus(`Viewing cluster ${displayName}`, 'info');

    // Mark active cluster
    clusterList.querySelectorAll('li').forEach(li => {
      if (li.dataset.clusterId === clusterId) {
        li.classList.add('active');
      } else {
        li.classList.remove('active');
      }
    });

    // Initialize diagram interactions
    const svgElement = contentArea.querySelector('.diagram-container svg');
    initializeDiagram(svgElement);

    // Save state
    lastContentState.clusters = {
      html: contentArea.innerHTML,
      status: statusMessage.textContent,
      statusType: statusMessage.className
    };
  } catch (e) {
    console.error('Failed to load cluster:', e);
    setStatus('Failed to load cluster', 'error');
    contentArea.innerHTML = `<p>Failed to load cluster diagram for ${clusterId}</p>`;
  }
}

function attachClusterEventHandlers() {
  // Re-attach global handlers after restoring state
  // Note: These functions are already attached to window, this just ensures they're available
  window.showClusterSummary = showClusterSummary;
  // refreshSummary, reloadFromSnapshot, and renameCurrentCluster are already on window

  // Reinitialize diagram interactions WITHOUT clearing highlighting
  // This preserves any user selections when restoring from saved state
  const svgElement = contentArea.querySelector('.diagram-container svg');
  if (svgElement) {
    // Clear the initialized flag to force re-attachment of event handlers
    delete svgElement.dataset.initialized;
    initializeDiagram(svgElement, false);
  }
}

// Make functions globally available
window.showClusterSummary = showClusterSummary;
window.showCluster = showCluster;
window.loadClusterList = loadClusterList;
window.attachClusterEventHandlers = attachClusterEventHandlers;

window.refreshSummary = async function() {
  showNotification('Refreshing summary...', 'loading', false);
  await showClusterSummary();
  await loadClusterList();
  showNotification('Summary refreshed', 'success', true);
};

window.reloadFromSnapshot = async function() {
  try {
    showNotification('Reloading from snapshot...', 'loading', false);
    const res = await fetch(`${API_BASE}/api/cluster/reload`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    const data = await res.json();

    if (data.ok) {
      showNotification('Reloaded from snapshot successfully', 'success', true);
      setStatus('Reloaded from snapshot', 'success');
      await loadClusterList();
      await showClusterSummary();
    } else {
      showNotification('Failed to reload snapshot', 'error', false);
      setStatus('Failed to reload snapshot', 'error');
    }
  } catch (e) {
    console.error('Failed to reload snapshot:', e);
    showNotification('Failed to reload snapshot', 'error', false);
    setStatus('Failed to reload snapshot', 'error');
  }
};

window.renameCurrentCluster = async function() {
  if (!currentClusterId) {
    showNotification('No cluster selected', 'error', false);
    return;
  }

  const input = document.getElementById('rename-cluster-input-inline');
  if (!input) {
    showNotification('Rename input field not found', 'error', false);
    console.error('rename-cluster-input-inline element not found in DOM');
    return;
  }

  console.log('Input element found:', input);
  console.log('Input raw value:', input.value);
  const newName = input.value?.trim();
  console.log('Rename cluster:', currentClusterId, 'to:', newName);
  console.log('New name length:', newName?.length);

  if (!newName) {
    showNotification('Please enter a new name', 'error', false);
    return;
  }

  try {
    showNotification('Renaming cluster...', 'loading', false);
    const commandText = `rename cluster ${currentClusterId} to ${newName}`;
    console.log('Sending command:', commandText);

    const res = await fetch(`${API_BASE}/api/cluster/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        command: commandText
      })
    });

    console.log('Response status:', res.status);
    const data = await res.json();
    console.log('Response data:', data);

    if (data.status === 'ok') {
      console.log('Rename successful, refreshing UI...');
      showNotification('Cluster renamed successfully', 'success', true);
      setStatus('Cluster renamed successfully', 'success');

      console.log('Reloading cluster list...');
      await loadClusterList();

      console.log('Reloading cluster view...');
      await showCluster(currentClusterId);

      console.log('Rename complete!');
    } else {
      console.log('Rename failed:', data);
      showNotification(`Failed to rename cluster: ${data.message || 'Unknown error'}`, 'error', false);
      setStatus('Failed to rename cluster', 'error');
    }
  } catch (e) {
    console.error('Failed to rename cluster:', e);
    showNotification(`Failed to rename cluster: ${e.message}`, 'error', false);
    setStatus('Failed to rename cluster', 'error');
  }
};

