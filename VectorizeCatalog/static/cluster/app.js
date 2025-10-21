const API_BASE = '/api/cluster';

const summaryGraphWrapper = document.getElementById('summary-graph');
const summaryGraphEl = summaryGraphWrapper ? summaryGraphWrapper.querySelector('.graph-content') : null;
const clusterGraphWrapper = document.getElementById('cluster-graph');
const clusterGraphEl = clusterGraphWrapper ? clusterGraphWrapper.querySelector('.graph-content') : null;
const summaryViewEl = document.getElementById('summary-view');
const detailViewEl = document.getElementById('detail-view');
const clusterTitleEl = document.getElementById('cluster-title');
const clusterListEl = document.getElementById('cluster-list');
const statusEl = document.getElementById('status-message');
const timestampEl = document.getElementById('timestamp');

const renameClusterForm = document.getElementById('rename-cluster-form');
const renameClusterInput = document.getElementById('rename-cluster-input');
const renameClusterFormSidebar = document.getElementById('rename-cluster-form-sidebar');
const renameClusterSelect = document.getElementById('rename-cluster-select');
const renameClusterInputSidebar = document.getElementById('rename-cluster-input-sidebar');
const renameGroupForm = document.getElementById('rename-group-form');
const renameGroupSelect = document.getElementById('rename-group-select');
const renameGroupInput = document.getElementById('rename-group-input');
const moveGroupForm = document.getElementById('move-group-form');
const moveGroupSelect = document.getElementById('move-group-select');
const moveGroupTargetSelect = document.getElementById('move-group-target');
const moveProcedureForm = document.getElementById('move-procedure-form');
const moveProcedureSelect = document.getElementById('move-procedure-select');
const moveProcedureTargetSelect = document.getElementById('move-procedure-target');
const commandForm = document.getElementById('command-form');
const commandInput = document.getElementById('command-input');

const refreshSummaryBtn = document.getElementById('refresh-summary');
const reloadSnapshotBtn = document.getElementById('reload-snapshot');
const resetClustersBtn = document.getElementById('reset-clusters');
const backButton = document.getElementById('back-button');
const sidebarTabs = document.querySelectorAll('.sidebar-tab');
const tabPanels = document.querySelectorAll('.tab-panel');

const zoomState = {
  summary: 1,
  cluster: 1,
};

let summaryData = null;
let currentClusterId = null;
let currentClusterDetail = null;

function setRenameFormsEnabled(enabled) {
  const disable = !enabled;

  const toggle = (element) => {
    if (!element) return;
    if ('disabled' in element) element.disabled = disable;
  };

  toggle(renameClusterInput);
  if (renameClusterForm) toggle(renameClusterForm.querySelector('button'));

  toggle(renameGroupSelect);
  toggle(renameGroupInput);
  if (renameGroupForm) toggle(renameGroupForm.querySelector('button'));

  toggle(moveGroupSelect);
  toggle(moveGroupTargetSelect);
  if (moveGroupForm) toggle(moveGroupForm.querySelector('button'));

  toggle(moveProcedureSelect);
  toggle(moveProcedureTargetSelect);
  if (moveProcedureForm) toggle(moveProcedureForm.querySelector('button'));

  toggle(commandInput);
  if (commandForm) toggle(commandForm.querySelector('button'));
}


async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed (${response.status})`);
  }
  return response.json();
}

async function fetchText(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed (${response.status})`);
  }
  return response.text();
}

function setStatus(message, kind = 'info') {
  statusEl.textContent = message;
  statusEl.className = '';
  if (kind === 'error') statusEl.classList.add('status-error');
  else if (kind === 'success') statusEl.classList.add('status-success');
  else statusEl.classList.add('status-info');
}

function updateTimestamp() {
  if (!summaryData || !summaryData.last_updated) {
    timestampEl.textContent = '';
    return;
  }
  const dt = new Date(summaryData.last_updated);
  timestampEl.textContent = `Last updated: ${dt.toLocaleString()}`;
}

async function loadSummaryData() {
  summaryData = await fetchJson(`${API_BASE}/summary`);
  renderClusterList();
  updateTimestamp();
}

async function loadSummaryGraph() {
  const svg = await fetchText(`${API_BASE}/svg/summary`);
  if (summaryGraphEl) {
    summaryGraphEl.innerHTML = svg;
    applyZoom('summary');
    wireSummaryGraph();
  }
}

function renderClusterList() {
  if (!summaryData) {
    clusterListEl.innerHTML = '<li>No clusters loaded.</li>';
    return;
  }

  clusterListEl.innerHTML = '';
  summaryData.clusters.forEach((cluster) => {
    const li = document.createElement('li');
    li.dataset.clusterId = cluster.cluster_id;
    const name = cluster.display_name || cluster.cluster_id;
    li.textContent = `${name} (${cluster.cluster_id})`;
    if (cluster.cluster_id === currentClusterId) {
      li.classList.add('active');
    }
    li.addEventListener('click', () => showCluster(cluster.cluster_id));
    clusterListEl.appendChild(li);
  });
}

function populateClusterSelect(selectEl) {
  if (!summaryData) return;
  selectEl.innerHTML = '';
  summaryData.clusters.forEach((cluster) => {
    const option = document.createElement('option');
    option.value = cluster.cluster_id;
    const name = cluster.display_name || cluster.cluster_id;
    option.textContent = `${name} (${cluster.cluster_id})`;
    selectEl.appendChild(option);
  });
  if (currentClusterId) {
    selectEl.value = currentClusterId;
  }
}

function wireSummaryGraph() {
  if (!summaryGraphEl) return;
  const svgElement = summaryGraphEl.querySelector('svg');
  if (!svgElement) return;

  // Get all nodes (clusters and tables)
  const nodes = svgElement.querySelectorAll('g.node');

  nodes.forEach((node) => {
    const nodeId = node.getAttribute('id');
    if (!nodeId) return;

    // Check if this is a cluster
    const isCluster = !summaryData?.global_tables?.includes(nodeId);

    // Single click: highlight connected entities and show rename controls for clusters
    node.addEventListener('click', (event) => {
      event.stopPropagation();
      highlightConnectedEntities(svgElement, nodeId);

      if (isCluster) {
        handleClusterSelection(nodeId);
      }
    });

    // Double click on cluster nodes: drill down
    if (isCluster) {
      node.addEventListener('dblclick', (event) => {
        event.stopPropagation();
        clearHighlights(svgElement);
        showCluster(nodeId);
      });
      node.style.cursor = 'pointer';
    }
  });

  // Click on empty area: clear highlights and hide controls
  svgElement.addEventListener('click', (event) => {
    if (event.target === svgElement || event.target.closest('g.graph')) {
      clearHighlights(svgElement);
      hideSidebarControls();
    }
  });
}

function handleClusterSelection(clusterId) {
  // Hide all sections first
  document.getElementById('rename-cluster-section')?.classList.add('hidden');
  document.getElementById('rename-group-section')?.classList.add('hidden');
  document.getElementById('move-group-section')?.classList.add('hidden');
  document.getElementById('move-procedure-section')?.classList.add('hidden');
  document.getElementById('rename-tab-hint')?.classList.add('hidden');

  // Show rename cluster section
  const renameClusterSection = document.getElementById('rename-cluster-section');
  renameClusterSection?.classList.remove('hidden');

  // Populate cluster dropdown
  populateClusterSelectForRename();

  // Set the selected cluster
  if (renameClusterSelect) {
    renameClusterSelect.value = clusterId;
  }

  // Find cluster display name
  const cluster = summaryData?.clusters?.find(c => c.cluster_id === clusterId);
  if (cluster && renameClusterInputSidebar) {
    renameClusterInputSidebar.value = cluster.display_name || cluster.cluster_id;
  }

  // Switch to Rename tab
  const renameTab = document.querySelector('.sidebar-tab[data-tab="rename"]');
  if (renameTab) {
    renameTab.click();
  }

  setStatus(`Selected cluster: ${cluster?.display_name || clusterId}`, 'info');
}

function populateClusterSelectForRename() {
  if (!summaryData || !renameClusterSelect) return;
  renameClusterSelect.innerHTML = '';
  summaryData.clusters.forEach((cluster) => {
    const option = document.createElement('option');
    option.value = cluster.cluster_id;
    const name = cluster.display_name || cluster.cluster_id;
    option.textContent = `${name} (${cluster.cluster_id})`;
    renameClusterSelect.appendChild(option);
  });
}

function hideSidebarControls() {
  // Hide all rename tab sections
  document.getElementById('rename-cluster-section')?.classList.add('hidden');
  document.getElementById('rename-group-section')?.classList.add('hidden');
  document.getElementById('move-group-section')?.classList.add('hidden');
  document.getElementById('move-procedure-section')?.classList.add('hidden');
  document.getElementById('rename-tab-hint')?.classList.remove('hidden');

  // Switch back to Clusters tab
  const clustersTab = document.querySelector('.sidebar-tab[data-tab="clusters"]');
  if (clustersTab) {
    clustersTab.click();
  }
}

function setupSidebarTabs() {
  if (!sidebarTabs.length) return;
  sidebarTabs.forEach((tab) => {
    tab.addEventListener('click', () => {
      const target = tab.dataset.tab;
      sidebarTabs.forEach((btn) => btn.classList.toggle('active', btn === tab));
      tabPanels.forEach((panel) => {
        panel.classList.toggle('active', panel.id === `tab-${target}`);
      });

      // If switching to rename tab without a selection, show hint
      if (target === 'rename') {
        const renameGroupSection = document.getElementById('rename-group-section');
        const moveGroupSection = document.getElementById('move-group-section');
        const moveProcedureSection = document.getElementById('move-procedure-section');
        const renameTabHint = document.getElementById('rename-tab-hint');

        const hasVisibleSection =
          !renameGroupSection?.classList.contains('hidden') ||
          !moveGroupSection?.classList.contains('hidden') ||
          !moveProcedureSection?.classList.contains('hidden');

        if (!hasVisibleSection) {
          renameTabHint?.classList.remove('hidden');
        }
      }
    });
  });
}

async function showCluster(clusterId, { preserveView = false } = {}) {
  try {
    const [detail, svg] = await Promise.all([
      fetchJson(`${API_BASE}/${encodeURIComponent(clusterId)}`),
      fetchText(`${API_BASE}/svg/${encodeURIComponent(clusterId)}`),
    ]);

    currentClusterId = detail.cluster.cluster_id;
    currentClusterDetail = detail;

    if (!preserveView) {
      summaryViewEl.classList.add('hidden');
      detailViewEl.classList.remove('hidden');
    }

    clusterTitleEl.textContent = detail.cluster.display_name || detail.cluster.cluster_id;
    zoomState.cluster = 1;
    if (clusterGraphEl) {
      clusterGraphEl.innerHTML = svg;
      applyZoom('cluster');
      wireClusterGraph();
    }

    setRenameFormsEnabled(true);
    renameClusterInput.value = detail.cluster.display_name || detail.cluster.cluster_id;

    populateGroupSelects(detail.groups);
    populateClusterSelect(moveGroupTargetSelect);
    populateClusterSelect(moveProcedureTargetSelect);

    renderClusterList();
    setStatus(`Viewing cluster ${detail.cluster.cluster_id}`, 'info');
  } catch (error) {
    console.error(error);
    setStatus(error.message, 'error');
  }
}

function wireClusterGraph() {
  if (!clusterGraphEl || !currentClusterDetail) return;

  const svgElement = clusterGraphEl.querySelector('svg');
  if (!svgElement) return;

  // Get all nodes (groups/procedures and tables)
  const nodes = svgElement.querySelectorAll('g.node');

  nodes.forEach((node) => {
    const nodeId = node.getAttribute('id');
    if (!nodeId) return;

    // Check if this is a group/procedure node
    const group = currentClusterDetail.groups.find(g => g.group_id === nodeId);

    if (group) {
      // Single click: highlight + select in sidebar
      node.addEventListener('click', (event) => {
        event.stopPropagation();
        highlightConnectedEntities(svgElement, nodeId);
        handleNodeClick(group);
      });

      node.style.cursor = 'pointer';
    } else {
      // Table node: just highlight connections
      node.addEventListener('click', (event) => {
        event.stopPropagation();
        highlightConnectedEntities(svgElement, nodeId);
      });
      node.style.cursor = 'pointer';
    }
  });

  // Click on empty area: clear highlights and hide rename sections
  svgElement.addEventListener('click', (event) => {
    if (event.target === svgElement || event.target.closest('g.graph')) {
      clearHighlights(svgElement);
      hideSidebarControls();
    }
  });
}

function handleNodeClick(group) {
  if (!group) return;

  // Hide all sections first
  const renameGroupSection = document.getElementById('rename-group-section');
  const moveGroupSection = document.getElementById('move-group-section');
  const moveProcedureSection = document.getElementById('move-procedure-section');
  const renameTabHint = document.getElementById('rename-tab-hint');

  renameGroupSection?.classList.add('hidden');
  moveGroupSection?.classList.add('hidden');
  moveProcedureSection?.classList.add('hidden');
  renameTabHint?.classList.add('hidden');

  // Determine if this is a singleton (procedure) or a multi-procedure group
  if (group.is_singleton && group.procedures.length === 1) {
    // This is a procedure - show move procedure section
    moveProcedureSection?.classList.remove('hidden');

    const procedureName = group.procedures[0];
    if (moveProcedureSelect) {
      moveProcedureSelect.value = procedureName;
      moveProcedureSelect.scrollIntoView({ behavior: 'smooth', block: 'center' });
      moveProcedureSelect.focus();
    }
    setStatus(`Selected procedure: ${procedureName}`, 'info');
  } else {
    // This is a procedure group - show rename and move group sections
    renameGroupSection?.classList.remove('hidden');
    moveGroupSection?.classList.remove('hidden');

    if (renameGroupSelect) {
      renameGroupSelect.value = group.group_id;
      renameGroupInput.value = group.display_name || group.group_id;
    }
    if (moveGroupSelect) {
      moveGroupSelect.value = group.group_id;
    }
    if (renameGroupSelect) {
      renameGroupSelect.scrollIntoView({ behavior: 'smooth', block: 'center' });
      renameGroupSelect.focus();
    }
    setStatus(`Selected group: ${group.display_name || group.group_id}`, 'info');
  }

  // Switch to the Rename tab after showing controls
  const renameTab = document.querySelector('.sidebar-tab[data-tab="rename"]');
  if (renameTab) {
    renameTab.click();
  }
}

function populateGroupSelects(groups) {
  renameGroupSelect.innerHTML = '';
  moveGroupSelect.innerHTML = '';
  moveProcedureSelect.innerHTML = '';

  if (!groups) return;
  groups.forEach((group) => {
    const display = group.display_name || group.group_id;
    const groupOption = document.createElement('option');
    groupOption.value = group.group_id;
    groupOption.textContent = `${display} (${group.group_id})`;
    renameGroupSelect.appendChild(groupOption.cloneNode(true));
    moveGroupSelect.appendChild(groupOption);

    group.procedures.forEach((proc) => {
      const procOption = document.createElement('option');
      procOption.value = proc;
      procOption.textContent = proc;
      moveProcedureSelect.appendChild(procOption);
    });
  });

  if (groups.length > 0) {
    const first = groups[0];
    renameGroupInput.value = first.display_name || first.group_id;
  } else if (renameGroupInput) {
    renameGroupInput.value = '';
  }
}

async function sendCommand(payload) {
  try {
    const response = await fetchJson(`${API_BASE}/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    summaryData = response.summary;
    renderClusterList();
    updateTimestamp();
    await loadSummaryGraph();

    if (currentClusterId) {
      try {
        await showCluster(currentClusterId, { preserveView: true });
      } catch (error) {
        setStatus(error.message, 'error');
      }
    }

    setStatus(response.message ?? 'Command executed.', 'success');
  } catch (error) {
    console.error(error);
    setStatus(error.message, 'error');
  }
}

function handleForms() {
  renameClusterForm.addEventListener('submit', (event) => {
    event.preventDefault();
    if (!currentClusterId) return;
    const newName = renameClusterInput.value.trim();
    if (!newName) return;
    sendCommand({ type: 'rename_cluster', cluster_id: currentClusterId, new_name: newName });
  });

  renameClusterFormSidebar.addEventListener('submit', (event) => {
    event.preventDefault();
    const clusterId = renameClusterSelect.value;
    const newName = renameClusterInputSidebar.value.trim();
    if (!clusterId || !newName) return;
    sendCommand({ type: 'rename_cluster', cluster_id: clusterId, new_name: newName });
  });

  renameGroupForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const groupId = renameGroupSelect.value;
    const newName = renameGroupInput.value.trim();
    if (!groupId || !newName) return;
    sendCommand({ type: 'rename_group', group_id: groupId, new_name: newName });
  });

  moveGroupForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const groupId = moveGroupSelect.value;
    const targetCluster = moveGroupTargetSelect.value;
    if (!groupId || !targetCluster) return;
    sendCommand({ type: 'move_group', group_id: groupId, cluster_id: targetCluster });
  });

  moveProcedureForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const procedure = moveProcedureSelect.value;
    const targetCluster = moveProcedureTargetSelect.value;
    if (!procedure || !targetCluster) return;
    sendCommand({ type: 'move_procedure', procedure, cluster_id: targetCluster });
  });

  commandForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const command = commandInput.value.trim();
    if (!command) return;
    sendCommand({ command });
    commandInput.value = '';
  });
}

function wireGlobalActions() {
  refreshSummaryBtn.addEventListener('click', async () => {
    try {
      await loadSummaryData();
      await loadSummaryGraph();
      setStatus('Summary refreshed.', 'success');
    } catch (error) {
      console.error(error);
      setStatus(error.message, 'error');
    }
  });

  reloadSnapshotBtn.addEventListener('click', async () => {
    try {
      const response = await fetchJson(`${API_BASE}/reload`, { method: 'POST' });
      summaryData = response.summary;
      renderClusterList();
      updateTimestamp();
      await loadSummaryGraph();
      if (currentClusterId) {
        await showCluster(currentClusterId, { preserveView: true });
      }
      setStatus('Reloaded cluster data from snapshot.', 'success');
    } catch (error) {
      console.error(error);
      setStatus(error.message, 'error');
    }
  });

  resetClustersBtn.addEventListener('click', async () => {
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
      setStatus('Rebuilding clusters from catalog.json...', 'info');

      const response = await fetchJson(`${API_BASE}/rebuild`, { method: 'POST' });

      summaryData = response.summary;
      renderClusterList();
      updateTimestamp();
      await loadSummaryGraph();

      // Return to summary view if in detail view
      if (currentClusterId) {
        detailViewEl.classList.add('hidden');
        summaryViewEl.classList.remove('hidden');
        currentClusterId = null;
        currentClusterDetail = null;
        setRenameFormsEnabled(false);
      }

      const stats = response.statistics || {};
      const statsMsg = `Clusters reset successfully! Generated ${stats.clusters || 0} clusters, ${stats.procedure_groups || 0} groups.`;
      setStatus(statsMsg, 'success');
    } catch (error) {
      console.error(error);
      setStatus('Failed to reset clusters: ' + error.message, 'error');
    }
  });

  backButton.addEventListener('click', () => {
    detailViewEl.classList.add('hidden');
    summaryViewEl.classList.remove('hidden');
    currentClusterId = null;
    currentClusterDetail = null;
    setRenameFormsEnabled(false);
    if (renameClusterInput) renameClusterInput.value = '';
    if (renameGroupSelect) renameGroupSelect.innerHTML = '';
    if (renameGroupInput) renameGroupInput.value = '';

    // Reset rename tab sections using shared function
    hideSidebarControls();

    setStatus('Returned to summary view.', 'info');
  });
}

async function initialise() {
  setStatus('Loading summary…');
  try {
    await loadSummaryData();
    await loadSummaryGraph();
    setStatus('Summary loaded.', 'success');
  } catch (error) {
    console.error(error);
    setStatus(error.message, 'error');
  }
}

function highlightConnectedEntities(svgElement, selectedNodeId) {
  if (!svgElement) return;

  clearHighlights(svgElement);

  // Find edges connected to the selected node
  const edges = svgElement.querySelectorAll('g.edge');
  const connectedEdges = [];

  edges.forEach((edge) => {
    const title = edge.querySelector('title');
    if (!title) return;

    const edgeTitle = title.textContent.trim();
    const parts = edgeTitle.split(/--|->/);
    if (parts.length !== 2) return;

    const source = parts[0].trim();
    const target = parts[1].trim();

    if (source === selectedNodeId || target === selectedNodeId) {
      connectedEdges.push(edge);
    }
  });

  // Highlight the selected node
  const selectedNode = svgElement.querySelector(`g.node[id="${selectedNodeId}"]`);
  if (selectedNode) {
    selectedNode.classList.add('highlighted');
    selectedNode.classList.add('selected');
  }

  // Highlight connected edges
  connectedEdges.forEach((edge) => {
    edge.classList.add('highlighted');
  });

  // Dim all other nodes and edges
  svgElement.querySelectorAll('g.node:not(.highlighted)').forEach((node) => {
    node.classList.add('dimmed');
  });
  svgElement.querySelectorAll('g.edge:not(.highlighted)').forEach((edge) => {
    edge.classList.add('dimmed');
  });
}

function clearHighlights(svgElement) {
  if (!svgElement) return;

  svgElement.querySelectorAll('.highlighted, .dimmed, .selected, .secondary-highlight').forEach((element) => {
    element.classList.remove('highlighted', 'dimmed', 'selected', 'secondary-highlight');
  });
}

setRenameFormsEnabled(false);
handleForms();
wireGlobalActions();
wireZoomControls();
setupSidebarTabs();
initialise();

function applyZoom(target) {
  const scale = zoomState[target];
  const container = target === 'summary' ? summaryGraphEl : clusterGraphEl;
  if (!container) return;
  container.style.transform = `scale(${scale})`;
  const wrapper = target === 'summary' ? summaryGraphWrapper : clusterGraphWrapper;
  if (wrapper) wrapper.dataset.zoomLevel = String(scale.toFixed(2));
}

function updateZoom(target, action) {
  const STEP = 0.2;
  const MIN = 0.2;
  const MAX = 3;
  if (action === 'in') {
    zoomState[target] = Math.min(MAX, zoomState[target] + STEP);
  } else if (action === 'out') {
    zoomState[target] = Math.max(MIN, zoomState[target] - STEP);
  } else {
    zoomState[target] = 1;
  }
  applyZoom(target);
}

function wireZoomControls() {
  document.querySelectorAll('.zoom-controls').forEach((ctrl) => {
    const target = ctrl.dataset.target;
    ctrl.querySelectorAll('button').forEach((btn) => {
      btn.addEventListener('click', () => {
        const action = btn.dataset.zoom;
        updateZoom(target, action);
      });
    });
  });

  [summaryGraphWrapper, clusterGraphWrapper].forEach((wrapper) => {
    if (!wrapper) return;
    const target = wrapper === summaryGraphWrapper ? 'summary' : 'cluster';
    wrapper.addEventListener('wheel', (event) => {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      const delta = Math.sign(event.deltaY);
      updateZoom(target, delta > 0 ? 'out' : 'in');
    }, { passive: false });
  });
}

// ============================================================================
// TRASH OPERATIONS
// ============================================================================

async function loadTrash() {
  try {
    const trashData = await fetchJson(`${API_BASE}/trash`);
    renderTrash(trashData);
  } catch (error) {
    console.error('Failed to load trash:', error);
    setStatus('Failed to load trash: ' + error.message, 'error');
  }
}

function renderTrash(trashData) {
  const trashContent = document.getElementById('trash-content');
  if (!trashContent) return;

  const totalCount = trashData.total_count || 0;

  if (totalCount === 0) {
    trashContent.innerHTML = '<p class="trash-empty">Trash is empty</p>';
    return;
  }

  let html = '';

  // Render procedures
  if (trashData.procedures && trashData.procedures.length > 0) {
    html += '<div class="trash-section">';
    html += `<h4>Procedures (${trashData.procedures.length})</h4>`;
    trashData.procedures.forEach(proc => {
      const deletedDate = proc.deleted_at ? new Date(proc.deleted_at).toLocaleString() : 'Unknown';
      const originalCluster = proc.original_cluster || 'Unknown';
      const tableCount = proc.table_count || 0;

      html += '<div class="trash-item">';
      html += '  <div class="trash-item-header">';
      html += `    <span class="trash-item-name">${escapeHtml(proc.procedure_name)}</span>`;
      html += '  </div>';
      html += '  <div class="trash-item-meta">';
      html += `    <span>Original cluster: ${escapeHtml(originalCluster)}</span>`;
      html += `    <span>Tables: ${tableCount}</span>`;
      html += `    <span>Deleted: ${deletedDate}</span>`;
      html += '  </div>';
      html += '  <div class="trash-item-actions">';
      html += `    <button class="restore-btn" onclick="restoreProcedure('${escapeHtml(proc.procedure_name)}')">Restore</button>`;
      html += '  </div>';
      html += '</div>';
    });
    html += '</div>';
  }

  // Render tables
  if (trashData.tables && trashData.tables.length > 0) {
    html += '<div class="trash-section">';
    html += `<h4>Tables (${trashData.tables.length})</h4>`;
    trashData.tables.forEach(table => {
      const deletedDate = table.deleted_at ? new Date(table.deleted_at).toLocaleString() : 'Unknown';
      const wasGlobal = table.data?.was_global ? 'Yes' : 'No';
      const wasOrphaned = table.data?.was_orphaned ? 'Yes' : 'No';

      html += '<div class="trash-item">';
      html += '  <div class="trash-item-header">';
      html += `    <span class="trash-item-name">${escapeHtml(table.item_id)}</span>`;
      html += '  </div>';
      html += '  <div class="trash-item-meta">';
      html += `    <span>Was global: ${wasGlobal}</span>`;
      html += `    <span>Was orphaned: ${wasOrphaned}</span>`;
      html += `    <span>Deleted: ${deletedDate}</span>`;
      html += '  </div>';
      html += '  <div class="trash-item-actions">';
      html += `    <button class="restore-btn" onclick="restoreTable(${table.index})">Restore</button>`;
      html += '  </div>';
      html += '</div>';
    });
    html += '</div>';
  }

  trashContent.innerHTML = html;
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

async function restoreProcedure(procedureName) {
  // Prompt for target cluster
  const targetCluster = prompt(`Restore procedure '${procedureName}' to which cluster?`, currentClusterId || '');
  if (!targetCluster) return;

  try {
    const response = await fetchJson(`${API_BASE}/trash/restore`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        item_type: 'procedure',
        procedure_name: procedureName,
        target_cluster_id: targetCluster,
        force_new_group: false,
      }),
    });

    setStatus(response.message || 'Procedure restored successfully', 'success');

    // Reload trash and summary
    await loadTrash();
    await loadSummaryData();
    await loadSummaryGraph();

    // If viewing a cluster, reload it
    if (currentClusterId) {
      await showCluster(currentClusterId, { preserveView: true });
    }
  } catch (error) {
    console.error('Failed to restore procedure:', error);
    setStatus('Failed to restore procedure: ' + error.message, 'error');
  }
}

async function restoreTable(trashIndex) {
  if (!confirm('Restore this table from trash?')) return;

  try {
    const response = await fetchJson(`${API_BASE}/trash/restore`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        item_type: 'table',
        trash_index: trashIndex,
      }),
    });

    setStatus(response.message || 'Table restored successfully', 'success');

    // Reload trash and summary
    await loadTrash();
    await loadSummaryData();
    await loadSummaryGraph();

    // If viewing a cluster, reload it
    if (currentClusterId) {
      await showCluster(currentClusterId, { preserveView: true });
    }
  } catch (error) {
    console.error('Failed to restore table:', error);
    setStatus('Failed to restore table: ' + error.message, 'error');
  }
}

async function emptyTrash() {
  if (!confirm('Permanently delete all items in trash? This action cannot be undone.')) return;

  try {
    const response = await fetchJson(`${API_BASE}/trash/empty`, {
      method: 'POST',
    });

    setStatus(response.message || 'Trash emptied successfully', 'success');
    await loadTrash();
  } catch (error) {
    console.error('Failed to empty trash:', error);
    setStatus('Failed to empty trash: ' + error.message, 'error');
  }
}

// Wire trash panel events
const emptyTrashBtn = document.getElementById('empty-trash-btn');
if (emptyTrashBtn) {
  emptyTrashBtn.addEventListener('click', emptyTrash);
}

// Load trash when switching to trash tab
sidebarTabs.forEach((tab) => {
  tab.addEventListener('click', () => {
    if (tab.dataset.tab === 'trash') {
      loadTrash();
    }
  });
});

// Make restore functions globally available
window.restoreProcedure = restoreProcedure;
window.restoreTable = restoreTable;
