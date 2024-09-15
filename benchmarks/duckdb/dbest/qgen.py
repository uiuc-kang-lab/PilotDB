# a.) Multi-column-pair analysis
# SELECT AGG(col1) FROM T WHERE ? < col2 < ?
# b.) Group-by analysis
# randomly generate the AGG and pred
# SELECT AGG(ss_sales_price) FROM store_sales WHERE ? < ss_sold_date_sk < ? GROUP BY ss_store_sk
# c.) Join analysis contains 42 randomly
# SELECT AGG(ss_whole_sale_cost), AGG(ss_net_profit) FROM store_sales, store WHERE ss_store_sk = s_store_sk WHERE s_number_of_employees = ?

# statistics of store.s_number_employees
import random
import argparse
import os

groupby_template = "SELECT {AGG}(ss_sales_price) FROM store_sales WHERE {date_lb} <= ss_sold_date_sk AND ss_sold_date_sk < {date_ub} GROUP BY ss_store_sk" # 30 = 3*10
join_template_1 = "SELECT {AGG}(ss_wholesale_cost) FROM store_sales, store WHERE ss_store_sk = s_store_sk AND s_number_employees = {n_employee}" # 21 = 3*7
join_template_2 = "SELECT {AGG}(ss_net_profit) FROM store_sales, store WHERE ss_store_sk = s_store_sk AND s_number_employees = {n_employee}" # 21 = 3*7

aggs = {"COUNT", "SUM", "AVG"}

def parse_date_dist():
    date_dist = {}
    with open("date_dist.txt", "r") as f:
        lines = f.readlines()[2:]
        for line in lines:
            cols = line.split("|")
            count = cols[0].strip()
            date = cols[1].strip()
            if len(date) == 0:
                continue
            date_dist[int(date)] = int(count)
    return date_dist

def parse_n_employee_dist():
    n_employee_dist = {}
    with open("n_employees_dist.txt", "r") as f:
        lines = f.readlines()[2:]
        for line in lines:
            cols = line.split("|")
            count = cols[0].strip()
            n_employee = cols[1].strip()
            if len(n_employee) == 0:
                continue
            n_employee_dist[int(n_employee)] = int(count)
    return n_employee_dist

def gen_agg_query():
    queries=  []
    with open("agg.sql") as f:
        for line in f:
            query = line.strip()
            queries.append(query)
    return queries

def gen_groupby_query(date_dist: dict):
    min_date = min(date_dist.keys())
    max_date = max(date_dist.keys())
    bucket_length = (max_date - min_date + 1) // 10
    queries = []
    for i in range(10):
        lb = min_date + i * bucket_length
        ub = lb + bucket_length
        for agg in aggs:
            query = groupby_template.format(AGG=agg, date_lb=lb, date_ub=ub)
            queries.append(query)
    return queries

def gen_join_query(n_employee_dist: dict, n_query: int):
    n_employees = list(n_employee_dist.keys())
    n_employees_choices = random.choices(n_employees, k=n_query)
    queries = []
    for n_employee in n_employees_choices:
        for agg1 in aggs:
            query = join_template_1.format(AGG=agg1, n_employee=n_employee)
            queries.append(query)
            query = join_template_2.format(AGG=agg1, n_employee=n_employee)
            queries.append(query)
    return queries

def main(args):
    os.system("rm -f query_*.sql")
    random.seed(args.seed)
    date_dist = parse_date_dist()
    n_employee_dist = parse_n_employee_dist()
    agg_queries = gen_agg_query()
    join_queries = gen_join_query(n_employee_dist, args.n_query)
    groupby_queries = gen_groupby_query(date_dist)

    for i in range(len(agg_queries)):
        with open(f"query_agg{i+1}.sql", "w") as f:
            f.write(agg_queries[i])
        
    for i in range(len(join_queries)):
        with open(f"query_join{i+1}.sql", "w") as f:
            f.write(join_queries[i])
    
    for i in range(len(groupby_queries)):
        with open(f"query_groupby{i+1}.sql", "w") as f:
            f.write(groupby_queries[i])

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_query", type=int, default=7)
    parser.add_argument("--seed", type=int, default=2333)
    args = parser.parse_args()
    main(args)


