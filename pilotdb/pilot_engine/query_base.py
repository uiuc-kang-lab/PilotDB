from dataclasses import dataclass
from typing import Dict, List

@dataclass
class Query():
    original_query: str         # the exact query without sampling clause
    final_sample_query: str     # the approximate query with sampling
    pilot_query: str            # the approximate query with sampling and page statistics
    column_mapping: List[Dict]  # a list of {"aggregate": <aggregate>, "<statistics>": <column_name>}
    page_size_col: str          # the column name of page size, i.e., count(*)
    page_id_col: str            # the column name of page id, i.e., {page_id}
    group_cols: List[str]       # a list of column names of original groups, 
                                #   [] if the original query does not have group by