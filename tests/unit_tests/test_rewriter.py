from pilotdb.execute import execute_aqp
from pilotdb.query import Query

import sys
import json
import yaml

dbname = sys.argv[1]
qid = sys.argv[2]

with open(f"../../benchmarks/{dbname}/query_{qid}.sql") as f:
    query_str = f.read()

with open(f"../../benchmarks/{dbname}/meta.json") as f:
    meta = json.load(f)

with open(f"../../db_configs/duckdb_{dbname}.yml") as f:
    db_config = yaml.safe_load(f)

query = Query(query_str, meta["table_cols"], meta["table_size"], name=f"{dbname}-{qid}")
execute_aqp(query, db_config=db_config, pilot_sample_rate=0.05)
