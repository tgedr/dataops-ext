import pytest
from tgedr_dataops_abs.source import SourceException
from tgedr_dataops_ext.source.delta_table_source import DeltaTableSource
from typing import List, Dict, Any, Optional


def test_get_missing_url():
    """Test get method raises exception when URL context is missing"""
    
    # Create a concrete implementation for testing
    class ConcreteDeltaTable(DeltaTableSource):
        @property
        def _storage_options(self):
            return None
        
        def list(self, context: Optional[Dict[str, Any]] = None) -> List[str]:
            return []
    
    o = ConcreteDeltaTable()
    with pytest.raises(SourceException, match="you must provide context for url"):
        o.get(context={})


def test_list_missing_url():
    """Test list method raises exception when URL context is missing"""
    
    # Use LocalDeltaTable which has a concrete list implementation
    from tgedr_dataops_ext.source.local_delta_table import LocalDeltaTable
    
    o = LocalDeltaTable()
    with pytest.raises(SourceException, match="you must provide context for url"):
        o.list(context={})
