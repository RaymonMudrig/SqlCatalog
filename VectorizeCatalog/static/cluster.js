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
            <button onclick="refreshSummary()" class="secondary">Refresh</button>
            <button onclick="resetClusters()" class="danger-btn" title="Rebuild clusters from catalog.json (DESTRUCTIVE)">Reset Clusters</button>
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

    // Sort clusters: non-empty first, then empty ones
    const sortedClusters = [...clusterData.clusters].sort((a, b) => {
      const aEmpty = (a.procedure_count === 0) ? 1 : 0;
      const bEmpty = (b.procedure_count === 0) ? 1 : 0;
      return aEmpty - bEmpty;
    });

    sortedClusters.forEach(cluster => {
      const li = document.createElement('li');
      const name = cluster.display_name || cluster.cluster_id;
      li.textContent = `${name} (${cluster.cluster_id})`;
      li.dataset.clusterId = cluster.cluster_id;

      // Add empty-cluster class if procedure_count is 0
      if (cluster.procedure_count === 0) {
        li.classList.add('empty-cluster');
      }

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
  // refreshSummary and renameCurrentCluster are already on window

  // Reinitialize diagram interactions WITHOUT clearing highlighting
  // This preserves any user selections when restoring from saved state
  const svgElement = contentArea.querySelector('.diagram-container svg');
  if (svgElement) {
    // Clear the initialized flag to force re-attachment of event handlers
    delete svgElement.dataset.initialized;
    initializeDiagram(svgElement, false);
  }
}

// ============================================================================
// TRASH MANAGEMENT
// ============================================================================

async function loadTrash() {
  try {
    const res = await fetch(`${API_BASE}/api/cluster/trash`);
    const trashData = await res.json();
    renderTrash(trashData);
  } catch (e) {
    console.error('Failed to load trash:', e);
  }
}

function renderTrash(trashData) {
  const trashContent = document.getElementById('trash-content');
  const trashActions = document.getElementById('trash-actions');
  if (!trashContent || !trashActions) return;

  const totalCount = trashData.total_count || 0;

  if (totalCount === 0) {
    trashContent.innerHTML = '<p class="trash-empty">No items in trash</p>';
    trashActions.style.display = 'none';
    return;
  }

  // Show empty trash button
  trashActions.style.display = 'block';

  let html = '';

  // Render procedures as simple list
  if (trashData.procedures && trashData.procedures.length > 0) {
    html += '<div class="trash-section-group">';
    html += `<h4>Procedures (${trashData.procedures.length})</h4>`;
    html += '<ul class="trash-list">';
    trashData.procedures.forEach(proc => {
      html += `<li class="trash-item" data-item-type="procedure" data-item-name="${escapeHtml(proc.procedure_name)}" data-original-cluster="${escapeHtml(proc.original_cluster || '')}" data-table-count="${proc.table_count || 0}" data-deleted-at="${proc.deleted_at || ''}">`;
      html += escapeHtml(proc.procedure_name);
      html += '</li>';
    });
    html += '</ul>';
    html += '</div>';
  }

  // Render tables as simple list
  if (trashData.tables && trashData.tables.length > 0) {
    html += '<div class="trash-section-group">';
    html += `<h4>Tables (${trashData.tables.length})</h4>`;
    html += '<ul class="trash-list">';
    trashData.tables.forEach(table => {
      html += `<li class="trash-item" data-item-type="table" data-item-name="${escapeHtml(table.item_id)}" data-trash-index="${table.index}" data-was-global="${table.data?.was_global || false}" data-was-orphaned="${table.data?.was_orphaned || false}" data-deleted-at="${table.deleted_at || ''}">`;
      html += escapeHtml(table.item_id);
      html += '</li>';
    });
    html += '</ul>';
    html += '</div>';
  }

  trashContent.innerHTML = html;

  // Attach click handlers to show context menu
  const trashItems = trashContent.querySelectorAll('.trash-item');
  trashItems.forEach(item => {
    item.addEventListener('click', (e) => {
      e.stopPropagation();
      showTrashItemMenu(item);
    });
  });
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function showTrashItemMenu(item) {
  const itemType = item.dataset.itemType;
  const itemName = item.dataset.itemName;

  // Clear previous selections
  document.querySelectorAll('.trash-item.selected').forEach(el => {
    el.classList.remove('selected');
  });

  // Mark this item as selected
  item.classList.add('selected');

  // Build info panel content
  let infoHtml = '<div class="trash-item-info">';
  infoHtml += `<h4>${itemName}</h4>`;
  infoHtml += '<div class="info-details">';

  if (itemType === 'procedure') {
    const originalCluster = item.dataset.originalCluster || 'Unknown';
    const tableCount = item.dataset.tableCount || '0';
    const deletedAt = item.dataset.deletedAt ? new Date(item.dataset.deletedAt).toLocaleString() : 'Unknown';

    infoHtml += `<p><strong>Type:</strong> Procedure</p>`;
    infoHtml += `<p><strong>Original cluster:</strong> ${escapeHtml(originalCluster)}</p>`;
    infoHtml += `<p><strong>Tables:</strong> ${tableCount}</p>`;
    infoHtml += `<p><strong>Deleted:</strong> ${deletedAt}</p>`;
  } else if (itemType === 'table') {
    const wasGlobal = item.dataset.wasGlobal === 'true' ? 'Yes' : 'No';
    const wasOrphaned = item.dataset.wasOrphaned === 'true' ? 'Yes' : 'No';
    const deletedAt = item.dataset.deletedAt ? new Date(item.dataset.deletedAt).toLocaleString() : 'Unknown';

    infoHtml += `<p><strong>Type:</strong> Table</p>`;
    infoHtml += `<p><strong>Was global:</strong> ${wasGlobal}</p>`;
    infoHtml += `<p><strong>Was orphaned:</strong> ${wasOrphaned}</p>`;
    infoHtml += `<p><strong>Deleted:</strong> ${deletedAt}</p>`;
  }

  infoHtml += '</div>';

  // Add restore button
  infoHtml += '<div class="info-actions">';
  if (itemType === 'procedure') {
    infoHtml += `<button class="restore-btn" onclick="restoreProcedure('${escapeHtml(itemName)}')">Restore to Cluster</button>`;
  } else if (itemType === 'table') {
    const trashIndex = item.dataset.trashIndex;
    infoHtml += `<button class="restore-btn" onclick="restoreTable(${trashIndex})">Restore</button>`;
  }
  infoHtml += '</div>';
  infoHtml += '</div>';

  // Show in entity action panel
  const actionPanel = document.getElementById('entity-action-panel');
  if (actionPanel) {
    actionPanel.innerHTML = infoHtml;
    actionPanel.style.display = 'block';
  }
}

async function restoreProcedure(procedureName) {
  const targetCluster = prompt(`Restore procedure '${procedureName}' to which cluster?`, currentClusterId || '');
  if (!targetCluster) return;

  try {
    showNotification('Restoring procedure...', 'loading', false);
    const res = await fetch(`${API_BASE}/api/cluster/trash/restore`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        item_type: 'procedure',
        procedure_name: procedureName,
        target_cluster_id: targetCluster,
        force_new_group: false,
      }),
    });

    const data = await res.json();

    if (data.status === 'ok') {
      showNotification(data.message || 'Procedure restored successfully', 'success', true);
      setStatus(data.message || 'Procedure restored', 'success');

      // Reload trash and clusters
      await loadTrash();
      await loadClusterList();

      // Refresh current view if in clusters tab
      if (currentTab === 'clusters') {
        if (currentClusterId) {
          await showCluster(currentClusterId);
        } else {
          await showClusterSummary();
        }
      }
    } else {
      showNotification(data.message || 'Failed to restore procedure', 'error', false);
      setStatus(data.message || 'Failed to restore procedure', 'error');
    }
  } catch (e) {
    console.error('Failed to restore procedure:', e);
    showNotification('Failed to restore procedure: ' + e.message, 'error', false);
    setStatus('Failed to restore procedure', 'error');
  }
}

async function restoreTable(trashIndex) {
  if (!confirm('Restore this table from trash?')) return;

  try {
    showNotification('Restoring table...', 'loading', false);
    const res = await fetch(`${API_BASE}/api/cluster/trash/restore`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        item_type: 'table',
        trash_index: trashIndex,
      }),
    });

    const data = await res.json();

    if (data.ok) {
      showNotification(data.message || 'Table restored successfully', 'success', true);
      setStatus(data.message || 'Table restored', 'success');

      // Reload trash and clusters
      await loadTrash();
      await loadClusterList();

      // Refresh current view if in clusters tab
      if (currentTab === 'clusters') {
        if (currentClusterId) {
          await showCluster(currentClusterId);
        } else {
          await showClusterSummary();
        }
      }
    } else {
      showNotification(data.message || 'Failed to restore table', 'error', false);
      setStatus(data.message || 'Failed to restore table', 'error');
    }
  } catch (e) {
    console.error('Failed to restore table:', e);
    showNotification('Failed to restore table: ' + e.message, 'error', false);
    setStatus('Failed to restore table', 'error');
  }
}

async function emptyTrash() {
  if (!confirm('Permanently delete all items in trash? This action cannot be undone.')) return;

  try {
    showNotification('Emptying trash...', 'loading', false);
    const res = await fetch(`${API_BASE}/api/cluster/trash/empty`, {
      method: 'POST',
    });

    const data = await res.json();

    if (data.ok) {
      showNotification(data.message || 'Trash emptied successfully', 'success', true);
      setStatus(data.message || 'Trash emptied', 'success');
      await loadTrash();
    } else {
      showNotification(data.message || 'Failed to empty trash', 'error', false);
      setStatus(data.message || 'Failed to empty trash', 'error');
    }
  } catch (e) {
    console.error('Failed to empty trash:', e);
    showNotification('Failed to empty trash: ' + e.message, 'error', false);
    setStatus('Failed to empty trash', 'error');
  }
}

// Wire up trash toggle and empty button
const trashToggleBtn = document.getElementById('trash-toggle-btn');
const trashContent = document.getElementById('trash-content');
const emptyTrashBtn = document.getElementById('empty-trash-btn');

if (trashToggleBtn && trashContent) {
  trashToggleBtn.addEventListener('click', () => {
    const isExpanded = trashContent.style.display !== 'none';
    if (isExpanded) {
      trashContent.style.display = 'none';
      trashToggleBtn.textContent = '▼';
      trashToggleBtn.classList.remove('expanded');
    } else {
      trashContent.style.display = 'block';
      trashToggleBtn.textContent = '▲';
      trashToggleBtn.classList.add('expanded');
      // Load trash when expanding
      loadTrash();
    }
  });
}

if (emptyTrashBtn) {
  emptyTrashBtn.addEventListener('click', emptyTrash);
}

// Reset clusters function (called from summary diagram)
window.resetClusters = async function() {
  // Double confirmation for destructive operation
  const confirmed = confirm(
    'WARNING: Reset Clusters will rebuild clusters.json from catalog.json.\n\n' +
    'This will PERMANENTLY DELETE:\n' +
    '- All current clusters and groups\n' +
    '- All trash items\n' +
    '- All custom names and reorganizations\n\n' +
    'This action CANNOT be undone!\n\n' +
    'Are you sure you want to continue?'
  );

  if (!confirmed) return;

  // Second confirmation
  const doubleConfirm = confirm(
    'FINAL WARNING!\n\n' +
    'This will completely reset all cluster data to freshly generated clusters.\n\n' +
    'Click OK to proceed with cluster reset, or Cancel to abort.'
  );

  if (!doubleConfirm) return;

  try {
    showNotification('Rebuilding clusters from catalog.json...', 'loading', false);
    setStatus('Rebuilding clusters from catalog.json...', 'info');

    const apiUrl = `${API_BASE}/api/cluster/rebuild`;
    console.log('Fetching URL:', apiUrl);
    console.log('API_BASE value:', API_BASE);

    const res = await fetch(apiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });

    console.log('Reset clusters response status:', res.status);

    if (!res.ok) {
      const errorText = await res.text();
      console.error('Reset clusters failed with status:', res.status, 'Error:', errorText);
      throw new Error(`Server returned ${res.status}: ${errorText}`);
    }

    const data = await res.json();
    console.log('Reset clusters response data:', data);

    if (data.status === 'ok') {
      const stats = data.statistics || {};
      const statsMsg = `Clusters reset successfully! Generated ${stats.clusters || 0} clusters, ${stats.procedure_groups || 0} groups.`;

      showNotification(statsMsg, 'success', true);
      setStatus(statsMsg, 'success');

      // Reload cluster list and summary
      await loadClusterList();
      await showClusterSummary();
    } else {
      console.error('Reset clusters failed: data.status is not ok:', data);
      showNotification(`Failed to reset clusters: ${data.message || 'Unknown error'}`, 'error', false);
      setStatus('Failed to reset clusters', 'error');
    }
  } catch (e) {
    console.error('Failed to reset clusters:', e);
    showNotification('Failed to reset clusters: ' + e.message, 'error', false);
    setStatus('Failed to reset clusters', 'error');
  }
};

// Make trash functions globally available
window.restoreProcedure = restoreProcedure;
window.restoreTable = restoreTable;
window.loadTrash = loadTrash;

// Make functions globally available
window.showClusterSummary = showClusterSummary;
window.showCluster = showCluster;
window.loadClusterList = loadClusterList;
window.attachClusterEventHandlers = attachClusterEventHandlers;

// Unified refresh function: file → memory → display
window.refreshSummary = async function() {
  try {
    showNotification('Refreshing...', 'loading', false);
    setStatus('Refreshing clusters...', 'info');

    // Reload from file (file → memory on backend)
    const res = await fetch(`${API_BASE}/api/cluster/reload`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    const data = await res.json();

    if (data.ok) {
      // Refresh display (memory → display on frontend)
      await loadClusterList();
      await showClusterSummary();
      showNotification('Refreshed successfully', 'success', true);
      setStatus('Clusters refreshed', 'success');
    } else {
      showNotification('Failed to refresh', 'error', false);
      setStatus('Failed to refresh', 'error');
    }
  } catch (e) {
    console.error('Failed to refresh:', e);
    showNotification('Failed to refresh: ' + e.message, 'error', false);
    setStatus('Failed to refresh', 'error');
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

