from dataclasses import dataclass
from typing import Dict, List

@dataclass
class Query():
    original_query: str             # the exact query without sampling clause
    final_sample_query: str         # the approximate query with sampling
    pilot_query: str                # the approximate query with sampling and page statistics
    column_mapping: List[Dict]      # a list of {"aggregate": <aggregate>, "<statistics>": <column_name>}
    group_cols: List[str]           # a list of column names of original groups, 
                                    #   [] if the original query does not have group by
    subquery_dict: Dict[str, str]|None=None   # a dictionary of subquery name and its query, i.e {"subquery_name": "subquery"}
    res_2_page_id: Dict[str, str]|None=None   # the mapping from result column name to page id, i.e. {"r1": "page_id_0"}
    