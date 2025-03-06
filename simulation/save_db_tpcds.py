import yaml
import os

with open("tpcds.yaml") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

for table in config:
    if os.path.exists(f"/mydata/dsb_tables/{table}.csv"):
        continue
    columes = config[table]
    query = f"SELECT *, CAST((CAST(CAST(ctid AS TEXT) AS point))[0] AS INT) page_id FROM {table}"
    # dump the table and columes from psql to a csv file
    code = os.system(f"""psql -d tpcds50g -U yxx404 -c "COPY ({query}) TO '/mydata/dsb_tables/{table}.csv' WITH DELIMITER ',' CSV HEADER" """)
    # if code == 0:
    #     # delete the table
    #     os.system(f"psql -d tpcds50g -U yxx404 -c 'DROP TABLE {table}'")
