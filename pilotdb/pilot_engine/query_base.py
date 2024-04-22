from dataclasses import dataclass
from typing import Dict, List

@dataclass
class Query():
    original_query: str
    final_sample_query: str
    pilot_query: str
    column_mapping: List[Dict]
    page_size_col: str
    page_id_col: str
    group_cols: List[str]