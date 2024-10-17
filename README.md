# PilotDB

An online middleware approximate query processing system that
1. provide user-specified error guarantees (i.e., a priori error guarantees)
2. requires no maintenance
3. requires no modifications to the underlying database management system
4. achieves up to 126x speedups

<p float="middle">
    <img src="./.assets/aqp.jpg" width="600"/>
</p>

## Setup

Install conda environment, DBMSs, and python packages

```batch
source install.sh
```

Install PilotDB as a Python package
```batch
pip install -e .
```

## Interface
We provide a Python interface to PilotDB.
```python
import pilotdb
db_config = {
    "dbms": "postgres", # or duckdb, sqlserver
    "username": "tester",
    "dbname": "testdb",
    "host": "hostname",
    "port": "dbport",
    "password": "password"
}
conn = pilotdb.connect("postgres", db_config)
result = pilotdb.run(
    conn,
    query="SELECT AVG(x) FROM T",
    error=0.05,
    probability=0.05 # the failure probability
)
pilotdb.close(conn)
```

## Demo

The following demo shows the a priori error guarantees and query acceleration of PilotDB.
<p float="middle">
    <img src="./.assets/demo.jpg" width="1200"/>
</p>

## Reproduce Results

You can follow the procedures below to reproduce the experimental results in
our paper.

### Set up DBMSs

You need to set up the underlying database manage system with data loaded.
1. make sure PostgreSQL, DuckDB, and SQL Server have been installed by 
executing the script `install.sh`. 

2. generate data for each workload using the following resource

    - [TPC-H](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp): 
    The official TPC-H toolkit.

    - [SSB](https://github.com/electrum/ssb-dbgen): We use the community version of
    the data generation script since the original SSB dataset is not open-sourced.
    We double-checked the correctness of those scripts.

    - [ClickBench](https://github.com/ClickHouse/ClickBench/): The official GitHub
    repo of ClickBench by ClickHouse.

    - [Instacart](https://www.kaggle.com/c/instacart-market-basket-analysis/data):
    The official Instacart data released in Kaggle.

    - [DSB](https://github.com/microsoft/dsb/tree/main): The official GitHub repo
    of DSB by Microsoft.

### Set up connections to the DBMS

You need to set up the connection to the underlying DBMS in the yaml 
file `db_configs/<dbname>_<benchmark>.yml`. For example, to run TPC-H benchmark 
on PostgreSQL, create and complet the file `db_configs/postgres_tpch.yml`. You 
can find the schema of the configuration file in `db_configs/dbname_benchmark.yml`.

### Run scripts of experiments

Based on the experiment you want to reproduce, you can run the 
corresponding script in the folder `experiments`.

1. To run the experiments of error guarantees (Section 5.2), you can execute the
bash script `experiments/error_guarantees/run_pilotdb.sh`.

2. To run the experiments of the quey speedups on various DBMSs and workloads 
(Section 5.3), you can execute the bash scripts 
`experiments/query_speedups/run_pilotdb_<benchmark>.sh`, where `<benchmark>` 
$\in$ `{tpch,ssb,clickbench,instacart}`.

3. To run the experiments of the quey speedups on skewed data distribution 
(Section 5.3), you can execute the bash scripts
`experiments/query_speedups/run_pilotdb_dsb.sh`.

4. To run the experiments of the query speedups under different target errors
(Section 5.3), you can execute the bash script 
`experiments/query_speedups/run_pilotdb_varying_errors.sh`

6. To run the experiments of ablation study (Section 5.5), you can execute the 
bash scripts `experiments/ablation/run_pilotdb_<config>.sh`, where `<config>` 
$\in$ `{oracle,uniform}`.

7. To run the experiments of PilotDB without BSAP (Supplementary Material B.1), 
you can execute the bash script `experiments/error_guarantees/run_pilotdb_nobsap.sh`.


8. To run the sensitivity experiments (Supplementary Material B.2),
you can execute the bash scripts `experiments/sensitivity/run_sensitivity_<knob>.sh`,
where `<knob>` $\in$ `{pilot-rate,selectivity}`.

9. To reproduce the Figure 4, you can execute the bash script 
`experiments/others/sampling_method_system_efficiency/run.sh`.

10. To reproduce the failure example in Section 4.3, you can execute the python
script `others/standard_clt_coverage_example/run_duckdb.py`.