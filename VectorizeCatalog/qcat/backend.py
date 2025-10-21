# qcat/backend.py
"""
Qcat backend service - semantic SQL catalog search.
Follows the same pattern as cluster/backend.py for consistency.

Flow: QcatService.execute_text() → qcat.agent.agent_answer() → qcat.llm_intent.classify_intent() → qcat.ops functions
"""
from __future__ import annotations
from typing import Dict, Any, Optional, List
import numpy as np


class QcatService:
    """
    Qcat service for semantic SQL catalog operations.
    Wraps items + embeddings and provides execute_text() interface.
    """

    def __init__(self, items: List[Dict[str, Any]], emb: np.ndarray):
        """
        Initialize QcatService

        Args:
            items: List of catalog items (tables, procedures, views, functions)
            emb: Embeddings matrix (N x D)
        """
        self.items = items
        self.emb = emb

    def execute_text(
        self,
        query: str,
        schema_filter: Optional[str] = None,
        name_pattern: Optional[str] = None,
        intent_override: Optional[str] = None,
        accept_proposal: bool = False,
        k: int = 10,
    ) -> Dict[str, Any]:
        """
        Execute natural language query via qcat agent.

        This follows the consistent flow:
          QcatService.execute_text()
            → qcat.agent.agent_answer()
              → qcat.llm_intent.classify_intent()
                → qcat.ops functions
                  → qcat.formatters

        Args:
            query: Natural language query
            schema_filter: Optional schema to filter results
            name_pattern: Optional pattern for name matching
            intent_override: Optional intent to force
            accept_proposal: Whether to accept low-confidence proposals
            k: Number of results to return

        Returns:
            Dict with answer, entities, etc.
        """
        from qcat.agent import agent_answer

        return agent_answer(
            query=query,
            items=self.items,
            emb=self.emb,
            schema_filter=schema_filter,
            name_pattern=name_pattern,
            intent_override=intent_override,
            accept_proposal=accept_proposal,
            k=k,
        )
