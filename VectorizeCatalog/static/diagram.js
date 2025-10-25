// ============================================================================
// DIAGRAM INTERACTION
// ============================================================================

function initializeDiagram(svgElement, clearHighlighting = true) {
  if (!svgElement) return;

  currentZoomLevel = 1.0;

  // Clear any existing highlighting/dimming from previous state
  // Skip clearing when restoring from saved state to preserve user selections
  if (clearHighlighting) {
    clearEdgeHighlighting();
  }

  // Check if handlers already attached (via a data attribute)
  // If already initialized, just reapply zoom and return
  if (svgElement.dataset.initialized === 'true') {
    applyZoom(svgElement, currentZoomLevel);
    return;
  }

  // Get all entity nodes - filter out the root graph element
  // Note: Cluster nodes may not have title elements, so we accept any node with valid ID
  const entities = Array.from(svgElement.querySelectorAll('g[id]')).filter(node => {
    const hasTitle = node.querySelector('title') !== null;
    const isNotGraph = !node.classList.contains('graph');
    const hasValidId = node.id && node.id !== '';
    // Include if: (has title OR has valid ID) AND not root graph
    return (hasTitle || hasValidId) && isNotGraph;
  });

  entities.forEach(entity => {
    // Single click: select and highlight
    entity.addEventListener('click', (e) => {
      // Don't stop propagation - let the SVG handler see this event
      // so it can properly distinguish between entity and background clicks
      selectEntity(entity.id, entity);
    });

    // Double click: open cluster detail view (for clusters only)
    entity.addEventListener('dblclick', (e) => {
      const entityType = detectEntityType(entity.id);
      if (entityType === 'cluster') {
        // Strip prefix before passing to showCluster
        const cleanId = entity.id.replace(/^cluster::/, '');
        showCluster(cleanId);
      }
    });
  });

  // Click on SVG background to deselect
  svgElement.addEventListener('click', (event) => {
    // Check if we clicked on an entity node (with title, not graph) or edge
    const clickedGroup = event.target.closest('g[id]');
    const clickedEdge = event.target.closest('g.edge');

    // Check if the clicked group is an actual entity
    // Note: Cluster nodes may not have title elements, so we check if it's not the root graph
    let isEntityClick = false;
    if (clickedGroup) {
      const isNotGraph = !clickedGroup.classList.contains('graph');
      const hasTitle = clickedGroup.querySelector('title') !== null;
      const hasId = clickedGroup.id && clickedGroup.id !== '';
      // Consider it an entity if: (has title OR has valid ID) AND not root graph
      isEntityClick = (hasTitle || hasId) && isNotGraph;
    }

    // If we clicked on an entity or edge, do nothing (their handlers will deal with it)
    if (isEntityClick || clickedEdge) {
      return;
    }

    // Otherwise, deselect
    deselectEntity();
  });

  // Mark as initialized to prevent duplicate handlers
  svgElement.dataset.initialized = 'true';

  // Apply current zoom level
  applyZoom(svgElement, currentZoomLevel);
}

function selectEntity(entityId, entityElement) {
  // Remove previous selection
  const previousSelection = document.querySelector('.diagram-container svg g.selected');
  if (previousSelection) {
    previousSelection.classList.remove('selected');
  }

  // Add selection to clicked entity
  if (entityElement) {
    entityElement.classList.add('selected');
  }

  selectedEntity = {
    id: entityId,
    type: detectEntityType(entityId),
    element: entityElement
  };

  // Highlight connected edges and dim others
  highlightConnectedEdges(entityId);

  // Show entity actions (async to fetch group data if needed)
  showEntityActions(selectedEntity).catch(err => {
    console.error('[selectEntity] showEntityActions failed:', err);
  });
}

function deselectEntity() {
  const previousSelection = document.querySelector('.diagram-container svg g.selected');
  if (previousSelection) {
    previousSelection.classList.remove('selected');
  }

  // Clear edge highlighting and dimming
  clearEdgeHighlighting();

  selectedEntity = null;
  hideEntityActions();
}

function highlightConnectedEdges(entityId) {
  const svgElement = document.querySelector('.diagram-container svg');
  if (!svgElement) return;

  clearEdgeHighlighting();

  // Get all nodes - filter out the root graph element and only get actual entity nodes
  // Note: Cluster nodes may not have title elements, so we accept any node with valid ID
  /*
  const allNodes = Array.from(svgElement.querySelectorAll('g[id]')).filter(node => {
    const hasTitle = node.querySelector('title') !== null;
    const isNotGraph = !node.classList.contains('graph');
    const hasValidId = node.id && node.id !== '';
    // Include if: (has title OR has valid ID) AND not root graph
    return (hasTitle || hasValidId) && isNotGraph;
  });
  */
  const allNodes = svgElement.querySelectorAll('g.node');
  const allEdges = svgElement.querySelectorAll('g.edge');

  const connectedEdges = [];
  const connectedNodeIds = new Set([entityId]);

  // Strip prefix from entityId to match against unprefixed edge titles
  const cleanEntityId = entityId.replace(/^(table::|tableX::|tableO::|proc::|cluster::|pg::)/, '');

  // Find edges connected to the selected node
  allEdges.forEach((edge) => {
    const title = edge.querySelector('title');
    if (!title) return;

    const edgeTitle = title.textContent.trim();
    // Handle both -- (undirected) and -> (directed) edges
    // Also handle HTML entities like &#45;&gt; (which is ->)
    const parts = edgeTitle.split(/(?:--|->|&#45;&#45;|&#45;&gt;)/);
    if (parts.length !== 2) return;

    const source = parts[0].trim();
    const target = parts[1].trim();

    // Check if this edge connects to our entity
    // Edge titles use unprefixed IDs, so compare with cleanEntityId
    const sourceMatches = source === cleanEntityId;
    const targetMatches = target === cleanEntityId;

    if (sourceMatches || targetMatches) {
      connectedEdges.push(edge);
      // Track connected node IDs - need to add prefixed versions
      // Figure out prefix by looking up the actual node
      const sourceNode = svgElement.querySelector(`g.node[id*="${source}"]`);
      const targetNode = svgElement.querySelector(`g.node[id*="${target}"]`);
      if (sourceNode) connectedNodeIds.add(sourceNode.id);
      if (targetNode) connectedNodeIds.add(targetNode.id);
    }
  });

  // Highlight the selected node and all connected nodes
  allNodes.forEach((node) => {
    const nodeId = node.getAttribute('id');
    // Always highlight the clicked entity itself, even if edge matching fails
    const isClickedEntity = nodeId === entityId;
    const isConnected = connectedNodeIds.has(nodeId);

    if (isClickedEntity || isConnected) {
      node.classList.add('highlighted');
      // Set opacity on both the group and its shape elements
      node.style.opacity = '1';
      const shapes = node.querySelectorAll('ellipse, polygon, path');
      shapes.forEach(shape => shape.style.opacity = '1');
    } else {
      node.classList.add('dimmed');
      // Set opacity on both the group and its shape elements
      node.style.opacity = '0.2';
      const shapes = node.querySelectorAll('ellipse, polygon, path');
      shapes.forEach(shape => shape.style.opacity = '0.2');
    }
  });

  // Highlight connected edges and dim others
  allEdges.forEach((edge) => {
    if (connectedEdges.includes(edge)) {
      edge.classList.add('highlighted');
      // Set opacity on both the edge group and its path/polygon elements
      edge.style.opacity = '1';
      const shapes = edge.querySelectorAll('path, polygon');
      shapes.forEach(shape => shape.style.opacity = '1');
    } else {
      edge.classList.add('dimmed');
      // Set opacity on both the edge group and its path/polygon elements
      edge.style.opacity = '0.2';
      const shapes = edge.querySelectorAll('path, polygon');
      shapes.forEach(shape => shape.style.opacity = '0.2');
    }
  });
}

function clearEdgeHighlighting() {
  const svgElement = document.querySelector('.diagram-container svg');
  if (!svgElement) return;

  // Remove all highlighting and dimming classes from nodes
  // Use g.node selector to get all Graphviz nodes
  svgElement.querySelectorAll('g.node').forEach((element) => {
    element.classList.remove('highlighted', 'dimmed', 'selected', 'secondary-highlight');
    // Remove inline opacity style from both group and shape elements
    element.style.opacity = '';
    const shapes = element.querySelectorAll('ellipse, polygon, path');
    shapes.forEach(shape => shape.style.opacity = '');
  });

  // Also clear from all edges
  svgElement.querySelectorAll('g.edge').forEach((element) => {
    element.classList.remove('highlighted', 'dimmed', 'selected', 'secondary-highlight');
    // Remove inline opacity style from both edge group and shape elements
    element.style.opacity = '';
    const shapes = element.querySelectorAll('path, polygon');
    shapes.forEach(shape => shape.style.opacity = '');
  });
}

function detectEntityType(entityId) {
  // Use prefix-based detection for clear, consistent entity type identification
  if (entityId.startsWith('table::')) {
    return 'table';
  } else if (entityId.startsWith('tableX::')) {
    return 'missing_table';
  } else if (entityId.startsWith('tableO::')) {
    return 'orphaned_table';
  } else if (entityId.startsWith('proc::')) {
    return 'procedure';
  } else if (entityId.startsWith('cluster::')) {
    return 'cluster';
  } else if (entityId.startsWith('pg::')) {
    return 'group';
  }
  return 'unknown';
}

async function showEntityActions(entity) {
  const cleanId = entity.id.replace(/^(table::|tableX::|tableO::|proc::|cluster::|pg::)/, '');

  // For groups, check if it's a singleton (represents a single procedure)
  let isSingleton = false;
  if (entity.type === 'group') {
    // Fetch group data to check if it's a singleton
    try {
      for (const cluster of clusterData.clusters) {
        const res = await fetch(`${API_BASE}/api/cluster/${cluster.cluster_id}`);
        const data = await res.json();
        const groupData = data.groups.find(g => g.group_id === cleanId);
        if (groupData) {
          isSingleton = groupData.is_singleton;
          break;
        }
      }
    } catch (e) {
      console.error('Failed to check if group is singleton:', e);
    }
  }

  // Map entity types to display labels
  const typeLabels = {
    'table': 'Table',
    'missing_table': 'Missing Table',
    'orphaned_table': 'Orphaned Table',
    'group': isSingleton ? 'Procedure' : 'Procedure Group',
    'cluster': 'Cluster',
    'procedure': 'Procedure',
    'view': 'View',
    'function': 'Function'
  };
  const displayType = typeLabels[entity.type] || entity.type.charAt(0).toUpperCase() + entity.type.slice(1);

  let actions = '';
  if (entity.type === 'table' || entity.type === 'missing_table' || entity.type === 'orphaned_table') {
    actions = `
      <button onclick="queryTable('${cleanId}')">Query this Table</button>
      <button onclick="findProceduresUsingTable('${cleanId}')">Find Procedures Using</button>
      <button onclick="deleteTable('${cleanId}')" class="danger-btn">Delete Table</button>
      <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
    `;
  } else if (entity.type === 'group') {
    // For singleton groups (single procedure), show delete procedure option
    if (isSingleton) {
      actions = `
        <button onclick="showGroupDetails('${cleanId}')">View Group Details</button>
        <button onclick="moveGroupToCluster('${cleanId}')">Move to Another Cluster</button>
        <button onclick="deleteProcedure('${cleanId}')" class="danger-btn">Delete Procedure</button>
        <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
      `;
    } else {
      actions = `
        <button onclick="showGroupDetails('${cleanId}')">View Group Details</button>
        <button onclick="moveGroupToCluster('${cleanId}')">Move to Another Cluster</button>
        <button onclick="deleteGroup('${cleanId}')" class="danger-btn">Delete All Procedures</button>
        <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
      `;
    }
  } else if (entity.type === 'cluster') {
    actions = `
      <button onclick="showCluster('${cleanId}')">View Cluster Details</button>
      <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
    `;
  } else {
    actions = `
      <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
    `;
  }

  // Create or get floating panel
  let floatingPanel = document.getElementById('entity-action-panel-floating');
  if (!floatingPanel) {
    floatingPanel = document.createElement('div');
    floatingPanel.id = 'entity-action-panel-floating';
    floatingPanel.className = 'entity-action-panel-floating';
    const diagramContainer = document.querySelector('.diagram-container');
    if (diagramContainer) {
      diagramContainer.appendChild(floatingPanel);
    }
  }

  floatingPanel.innerHTML = `
    <h4 class="drag-handle">
      <span>Selected: ${displayType}</span>
      <button class="close-btn" onclick="deselectEntity()" title="Close">&times;</button>
    </h4>
    <div class="entity-name">${cleanId}</div>
    <div class="action-buttons">
      ${actions}
    </div>
  `;

  floatingPanel.style.display = 'block';

  // Always reattach drag functionality after innerHTML update
  // (innerHTML destroys the drag-handle element and its listeners)
  makePanelDraggable(floatingPanel);
}

function makePanelDraggable(panel) {
  // Remove any existing drag handlers to prevent duplicates
  if (panel._dragHandlers) {
    const dragHandle = panel.querySelector('.drag-handle');
    if (dragHandle && panel._dragHandlers.dragStart) {
      dragHandle.removeEventListener('mousedown', panel._dragHandlers.dragStart);
    }
    if (panel._dragHandlers.drag) {
      document.removeEventListener('mousemove', panel._dragHandlers.drag);
    }
    if (panel._dragHandlers.dragEnd) {
      document.removeEventListener('mouseup', panel._dragHandlers.dragEnd);
    }
  }

  let isDragging = false;
  let currentX;
  let currentY;
  let initialX;
  let initialY;

  const dragHandle = panel.querySelector('.drag-handle');
  if (!dragHandle) return;

  function dragStart(e) {
    // Don't drag if clicking on the close button
    if (e.target.closest('.close-btn')) {
      return;
    }

    // Get current position from style or compute from viewport position
    const rect = panel.getBoundingClientRect();
    initialX = e.clientX - rect.left;
    initialY = e.clientY - rect.top;

    isDragging = true;
    panel.classList.add('dragging');
  }

  function drag(e) {
    if (!isDragging) return;

    e.preventDefault();

    currentX = e.clientX - initialX;
    currentY = e.clientY - initialY;

    // Keep panel within viewport bounds
    const rect = panel.getBoundingClientRect();
    const maxX = window.innerWidth - rect.width;
    const maxY = window.innerHeight - rect.height;

    currentX = Math.max(0, Math.min(currentX, maxX));
    currentY = Math.max(0, Math.min(currentY, maxY));

    panel.style.left = `${currentX}px`;
    panel.style.top = `${currentY}px`;
    panel.style.right = 'auto'; // Remove right positioning when dragging
  }

  function dragEnd() {
    isDragging = false;
    panel.classList.remove('dragging');
  }

  // Store handlers for cleanup on next call
  panel._dragHandlers = { dragStart, drag, dragEnd };

  // Attach event listeners
  dragHandle.addEventListener('mousedown', dragStart);
  document.addEventListener('mousemove', drag);
  document.addEventListener('mouseup', dragEnd);
}

function hideEntityActions() {
  const floatingPanel = document.getElementById('entity-action-panel-floating');
  if (floatingPanel) {
    floatingPanel.style.display = 'none';
  }

  // Also hide left panel (legacy, kept for trash items)
  const panel = document.getElementById('entity-action-panel');
  if (panel) {
    panel.style.display = 'none';
  }
}

// Zoom functions
function applyZoom(svgElement, zoomLevel) {
  if (!svgElement) return;
  svgElement.style.transform = `scale(${zoomLevel})`;
  svgElement.style.transformOrigin = 'top left';
}

function zoomIn() {
  currentZoomLevel = Math.min(currentZoomLevel + 0.1, 3.0);
  const svg = document.querySelector('.diagram-container svg');
  applyZoom(svg, currentZoomLevel);
  updateZoomDisplay();
}

function zoomOut() {
  currentZoomLevel = Math.max(currentZoomLevel - 0.1, 0.5);
  const svg = document.querySelector('.diagram-container svg');
  applyZoom(svg, currentZoomLevel);
  updateZoomDisplay();
}

function zoomReset() {
  currentZoomLevel = 1.0;
  const svg = document.querySelector('.diagram-container svg');
  applyZoom(svg, currentZoomLevel);
  updateZoomDisplay();
}

function updateZoomDisplay() {
  const display = document.querySelector('.zoom-level');
  if (display) {
    display.textContent = Math.round(currentZoomLevel * 100) + '%';
  }
}

// Entity action handlers
window.queryTable = function(tableName) {
  promptInput.value = `show me information about table \`${tableName}\``;
  executeCommand();
};

window.findProceduresUsingTable = function(tableName) {
  promptInput.value = `which procedures access \`${tableName}\``;
  executeCommand();
};

window.showGroupDetails = async function(groupId) {
  try {
    // Find the group data from clusterData
    let groupData = null;
    let clusterName = null;
    let missingTables = new Set();

    // First, fetch summary to get missing tables list
    const summaryRes = await fetch(`${API_BASE}/api/cluster/summary`);
    const summaryData = await summaryRes.json();

    // Extract missing tables from table_nodes
    if (summaryData.table_nodes) {
      summaryData.table_nodes.forEach(node => {
        if (node.is_missing) {
          missingTables.add(node.table);
        }
      });
    }

    for (const cluster of clusterData.clusters) {
      const res = await fetch(`${API_BASE}/api/cluster/${cluster.cluster_id}`);
      const data = await res.json();

      const foundGroup = data.groups.find(g => g.group_id === groupId);
      if (foundGroup) {
        groupData = foundGroup;
        clusterName = cluster.display_name || cluster.cluster_id;
        break;
      }
    }

    if (!groupData) {
      showNotification(`Group ${groupId} not found`, 'error', false);
      return;
    }

    // Build HTML to display group details
    const displayName = groupData.display_name || groupData.group_id;
    const proceduresList = groupData.procedures.map(p => `<li><code>${p}</code></li>`).join('');

    // For tables, mark missing tables with special styling (not clickable)
    const tablesList = groupData.tables.map(t => {
      if (missingTables.has(t)) {
        // Missing table - render as plain text with styling, not clickable
        return `<li><span class="missing-table" title="Missing table (doesn't exist in catalog)">${t}</span> <span style="color: #999;">(missing)</span></li>`;
      } else {
        // Normal table - wrap in code to make clickable
        return `<li><code>${t}</code></li>`;
      }
    }).join('');

    // Find the cluster info for breadcrumb
    let clusterId = null;
    for (const cluster of clusterData.clusters) {
      const res = await fetch(`${API_BASE}/api/cluster/${cluster.cluster_id}`);
      const data = await res.json();
      if (data.groups.find(g => g.group_id === groupId)) {
        clusterId = cluster.cluster_id;
        break;
      }
    }

    const html = `
      <div style="padding: 1rem;">
        <div class="breadcrumb">
          <a href="#" onclick="showClusterSummary(); return false;">Summary</a>
          <span class="breadcrumb-separator">→</span>
          <a href="#" onclick="showCluster('${clusterId}'); return false;">${clusterName}</a>
          <span class="breadcrumb-separator">→</span>
          <span class="breadcrumb-current">${displayName}</span>
        </div>

        <h2>Procedure Group: ${displayName}</h2>
        <p><strong>Group ID:</strong> ${groupData.group_id}</p>
        <p><strong>Cluster:</strong> ${clusterName}</p>
        <p><strong>Type:</strong> ${groupData.is_singleton ? 'Singleton' : 'Multi-procedure group'}</p>

        <h3>Procedures (${groupData.procedures.length})</h3>
        <ul>${proceduresList || '<li>None</li>'}</ul>

        <h3>Tables Accessed (${groupData.tables.length})</h3>
        <ul>${tablesList || '<li>None</li>'}</ul>
      </div>
    `;

    contentArea.innerHTML = html;

    // Make entities clickable (procedures and non-missing tables)
    if (typeof window.makeEntitiesClickable === 'function') {
      window.makeEntitiesClickable(contentArea);
    }

    setStatus(`Viewing group ${displayName}`, 'info');
  } catch (e) {
    console.error('Failed to load group details:', e);
    showNotification(`Failed to load group details: ${e.message}`, 'error', false);
  }
};

window.moveGroupToCluster = function(groupId) {
  const targetCluster = prompt('Enter target cluster ID:');
  if (targetCluster) {
    promptInput.value = `move group ${groupId} to cluster ${targetCluster}`;
    executeCommand();
  }
};

window.addToPrompt = function(entityName) {
  const currentValue = promptInput.value.trim();
  const wrappedText = '`' + entityName + '`';

  if (currentValue) {
    promptInput.value = currentValue + ' ' + wrappedText;
  } else {
    promptInput.value = wrappedText;
  }

  promptInput.focus();
  promptInput.setSelectionRange(promptInput.value.length, promptInput.value.length);
};

window.deleteProcedure = async function(procedureName) {
  if (!confirm(`Delete procedure '${procedureName}'? It will be moved to trash.`)) {
    return;
  }

  try {
    showNotification('Deleting procedure...', 'loading', false);

    // Use unified command endpoint with explicit "procedure" keyword to avoid misclassification
    const res = await fetch(`${API_BASE}/api/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        command: `delete procedure \`${procedureName}\``
      })
    });

    const data = await res.json();

    if (data.ok) {
      showNotification('Procedure deleted successfully', 'success', true);
      setStatus('Procedure deleted', 'success');

      // Reload clusters and trash
      await loadClusterList();
      await loadTrash();

      // Refresh current view
      if (currentClusterId) {
        await showCluster(currentClusterId);
      } else {
        await showClusterSummary();
      }
    } else {
      const errorMsg = data.result?.answer || data.message || 'Failed to delete procedure';
      showNotification(errorMsg, 'error', false);
      setStatus('Failed to delete procedure', 'error');
    }
  } catch (e) {
    console.error('Failed to delete procedure:', e);
    showNotification('Failed to delete procedure: ' + e.message, 'error', false);
    setStatus('Failed to delete procedure', 'error');
  }
};

window.deleteTable = async function(tableName) {
  if (!confirm(`Delete table '${tableName}' from catalog? It will be moved to trash.`)) {
    return;
  }

  try {
    showNotification('Deleting table...', 'loading', false);

    // Use unified command endpoint with explicit "table" keyword
    const res = await fetch(`${API_BASE}/api/command`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        command: `delete table \`${tableName}\``
      })
    });

    const data = await res.json();

    if (data.ok) {
      showNotification('Table deleted successfully', 'success', true);
      setStatus('Table deleted', 'success');

      // Reload clusters and trash
      await loadClusterList();
      await loadTrash();

      // Refresh current view
      if (currentClusterId) {
        await showCluster(currentClusterId);
      } else {
        await showClusterSummary();
      }
    } else {
      const errorMsg = data.result?.answer || data.message || 'Failed to delete table';
      showNotification(errorMsg, 'error', false);
      setStatus('Failed to delete table', 'error');
    }
  } catch (e) {
    console.error('Failed to delete table:', e);
    showNotification('Failed to delete table: ' + e.message, 'error', false);
    setStatus('Failed to delete table', 'error');
  }
};

window.deleteGroup = async function(groupId) {
  try {
    // First, fetch the group data to get the list of procedures
    let groupData = null;
    let clusterName = null;

    for (const cluster of clusterData.clusters) {
      const res = await fetch(`${API_BASE}/api/cluster/${cluster.cluster_id}`);
      const data = await res.json();

      const foundGroup = data.groups.find(g => g.group_id === groupId);
      if (foundGroup) {
        groupData = foundGroup;
        clusterName = cluster.display_name || cluster.cluster_id;
        break;
      }
    }

    if (!groupData) {
      showNotification(`Group ${groupId} not found`, 'error', false);
      return;
    }

    const displayName = groupData.display_name || groupData.group_id;
    const procedureCount = groupData.procedures.length;

    // Confirm deletion
    const confirmMsg = `Delete all ${procedureCount} procedures in group '${displayName}'?\n\n` +
      `Procedures:\n${groupData.procedures.map(p => `  - ${p}`).join('\n')}\n\n` +
      `All procedures will be moved to trash.`;

    if (!confirm(confirmMsg)) {
      return;
    }

    // Delete procedures one by one
    let successCount = 0;
    let failCount = 0;
    const errors = [];

    showNotification(`Deleting ${procedureCount} procedures...`, 'loading', false);

    for (let i = 0; i < groupData.procedures.length; i++) {
      const procedureName = groupData.procedures[i];

      try {
        setStatus(`Deleting procedure ${i + 1}/${procedureCount}: ${procedureName}`, 'info');

        const res = await fetch(`${API_BASE}/api/command`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            command: `delete procedure \`${procedureName}\``
          })
        });

        const data = await res.json();

        if (data.ok) {
          successCount++;
        } else {
          failCount++;
          const errorMsg = data.result?.answer || data.message || 'Unknown error';
          errors.push(`${procedureName}: ${errorMsg}`);
        }
      } catch (e) {
        failCount++;
        errors.push(`${procedureName}: ${e.message}`);
      }
    }

    // Show summary
    if (failCount === 0) {
      showNotification(`Successfully deleted all ${successCount} procedures`, 'success', true);
      setStatus('All procedures deleted', 'success');
    } else {
      const summaryMsg = `Deleted ${successCount} procedures, ${failCount} failed.\n\nErrors:\n${errors.join('\n')}`;
      showNotification(summaryMsg, 'error', false);
      setStatus(`Deleted ${successCount}/${procedureCount} procedures`, 'error');
      console.error('Deletion errors:', errors);
    }

    // Reload clusters and trash
    await loadClusterList();
    await loadTrash();

    // Refresh current view
    if (currentClusterId) {
      await showCluster(currentClusterId);
    } else {
      await showClusterSummary();
    }

  } catch (e) {
    console.error('Failed to delete group:', e);
    showNotification('Failed to delete group: ' + e.message, 'error', false);
    setStatus('Failed to delete group', 'error');
  }
};

// Make functions globally available
window.initializeDiagram = initializeDiagram;
window.deselectEntity = deselectEntity;
window.selectEntity = selectEntity;

// Make zoom functions global
window.zoomIn = zoomIn;
window.zoomOut = zoomOut;
window.zoomReset = zoomReset;
