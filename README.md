# PilotDB

## Setup

Install conda environment, DBMSs, and python packages

```batch
source install.sh
```

Install PilotDB as a python package
```batch
pip install -e .
```

Setup the database configuration in `db_configs/<dbms>.yml`

## Experiments
To approximately execute TPC-H query 6 with pilot sampling rate 0.05% on postgres
```batch
python run_pilotdb.py \
    --benchmark tpch \
    --qid 6 \
    --pilot_sample_rate 0.05 \
    --dbms postgres \
    --db_config_file db_configs/postgres.yml \
    --process_mode aqp
```

To exactly execute TPC-H query 1 on postgres
```batch
python run_pilotdb.py \
    --benchmark tpch \
    --qid 1 \
    --process_model exact
```