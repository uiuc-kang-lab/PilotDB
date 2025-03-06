import pandas as pd
import numpy as np
import sys

gt = 3693776

print("loading data ...")
store_sales = pd.read_parquet("~/data/store_sales.parquet")
store = pd.read_csv("/mydata/dsb_tables/store.csv")

start_id = int(sys.argv[1])
end_id = int(sys.argv[2])

for run_id in range(start_id, end_id):
    
    print(f"running wander join: runid={run_id} ...")
    
    store_sales_pages = set()
    store_pages = set()
    n_index_lookup = 0
    samples = []
    weights = []
    n_reach_01 = 0 # reach 0.1% error for 10 times
        
    with open(f"test_query_wj-{run_id}.csv", "w") as f:
        f.write("n_index_lookup,n_page_access,count_est,error\n")

    while True:
        weight = 1
        # sample one record from store
        store_sample = store.sample(n=1)
        weight /= len(store)
        store_sample = store_sample[store_sample["s_number_employees"] == 19095040]
        if len(store_sample) == 0:
            samples.append(0)
            weights.append(weight)
            continue
        store_pages.add(store_sample["page_id"].values[0])
        store_sample = store_sample.drop(columns=["page_id"])
        
        # join with store_sales
        join = pd.merge(store_sample, store_sales, left_on="s_store_sk", right_on="ss_store_sk")
        n_index_lookup += 1
        if len(join) == 0:
            samples.append(0)
            weights.append(weight)
            continue
        join_sample = join.sample(n=1)
        store_sales_pages.add(join_sample["page_id"].values[0])
        weight /= len(join)
        samples.append(1)
        weights.append(weight)
        
        # estimation
        count_est = np.mean([s / w for s, w in zip(samples, weights)])
        error = abs(count_est - gt) / gt * 100
        n_page_access = len(store_pages) + len(store_sales_pages)
        print(f"n_index_lookup: {n_index_lookup}, n_page_access: {n_page_access}, count_est: {count_est}, error: {error}")
        with open(f"test_query_wj-{run_id}.csv", "a") as f:
            f.write(f"{n_index_lookup},{n_page_access},{count_est},{error}\n")
        if error < 0.1:
            n_reach_01 += 1
            if n_reach_01 == 10:
                break
        