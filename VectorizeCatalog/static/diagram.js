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
  showEntityActions(selectedEntity);
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
  const panel = document.getElementById('entity-action-panel');
  if (!panel) return;

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
      <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
    `;
  } else if (entity.type === 'group') {
    actions = `
      <button onclick="showGroupDetails('${cleanId}')">View Group Details</button>
      <button onclick="moveGroupToCluster('${cleanId}')">Move to Another Cluster</button>
      <button onclick="addToPrompt('${cleanId}')" class="secondary">Add to Prompt</button>
    `;
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

  panel.innerHTML = `
    <div class="entity-action-panel">
      <h4>Selected: ${displayType}</h4>
      <div class="entity-name">${cleanId}</div>
      <div class="action-buttons">
        ${actions}
      </div>
    </div>
  `;

  panel.style.display = 'block';
}

function hideEntityActions() {
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
    const tablesList = groupData.tables.map(t => `<li><code>${t}</code></li>`).join('');

    const html = `
      <div style="padding: 1rem;">
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

// Make functions globally available
window.initializeDiagram = initializeDiagram;
window.deselectEntity = deselectEntity;
window.selectEntity = selectEntity;

// Make zoom functions global
window.zoomIn = zoomIn;
window.zoomOut = zoomOut;
window.zoomReset = zoomReset;
