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
import json

template_1 = "SELECT {AGG1}(ss_sales_price) FROM store_sales WHERE {date_lb} <= ss_sold_date_sk AND ss_sold_date_sk < {date_ub}"
template_2 = "SELECT {AGG1}(ss_sales_price) FROM store_sales WHERE {date_lb} <= ss_sold_date_sk AND ss_sold_date_sk < {date_ub} GROUP BY ss_store_sk"
template_3 = "SELECT {AGG1}(ss_wholesale_cost), {AGG2}(ss_net_profit) FROM store_sales, store WHERE ss_store_sk = s_store_sk AND s_number_employees = {n_employee}"
template_1_uniform = "SELECT AVG(ss_sales_price) as avg_1, STDDEV(ss_sales_price) as std_1, COUNT(*) as sample_size FROM store_sales {sampling_method} WHERE {date_lb} <= ss_sold_date_sk AND ss_sold_date_sk < {date_ub}"
template_2_uniform = "SELECT AVG(ss_sales_price) as avg_1, STDDEV(ss_sales_price) as std_1, COUNT(*) as sample_size FROM store_sales {sampling_method} WHERE {date_lb} <= ss_sold_date_sk AND ss_sold_date_sk < {date_ub} GROUP BY ss_store_sk"
template_3_uniform = "SELECT AVG(ss_wholesale_cost) as avg_1, STDDEV(ss_wholesale_cost) as std_1, AVG(ss_net_profit) as avg_2, STDDEV(ss_net_profit) as std_2, COUNT(*) as sample_size FROM store_sales, store {sampling_method} WHERE ss_store_sk = s_store_sk AND s_number_employees = {n_employee}"
aggs = ["COUNT", "AVG", "SUM"]

uniform_query_template = """pilot_query = \"\"\"
{pilot_query};
\"\"\"

results_mapping = {results_mapping}

subquery_dict = {subquery_dict}
"""

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

def gen_query_1(date_dist: dict, n_query: int):
    min_date = min(date_dist.keys())
    max_date = max(date_dist.keys())
    bucket = int((max_date - min_date + 1)*0.1)
    queries = []
    uniform_queries = []
    for _ in range(n_query):
        lb = random.choice(range(min_date, max_date - bucket))
        ub = lb + bucket
        for agg in aggs:
            query = template_1.format(AGG1=agg, date_lb=lb, date_ub=ub)
            uniform_query = template_1_uniform.format(sampling_method="{sampling_method}", date_lb=lb, date_ub=ub)
            queries.append(query)
            results_mapping = []
            if agg == "SUM":
                results_mapping.append({"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"})
            elif agg == "AVG":
                results_mapping.append({"aggregate": "avg", "mean": "avg_1", "std": "std_1"})
            elif agg == "COUNT":
                results_mapping.append({"aggregate": "count", "size": "sample_size"})
            uniform_queries.append({
                "pilot_query": uniform_query,
                "results_mapping": results_mapping,
                "subquery_dict": []
            })
    return queries, uniform_queries

def gen_query_2(date_dist: dict, n_query: int):
    min_date = min(date_dist.keys())
    max_date = max(date_dist.keys())
    bucket = int((max_date - min_date + 1)*0.1)
    queries = []
    uniform_queries = []
    for _ in range(n_query):
        lb = random.choice(range(min_date, max_date - bucket))
        ub = lb + bucket
        for agg in aggs:
            query = template_2.format(AGG1=agg, date_lb=lb, date_ub=ub)
            uniform_query = template_2_uniform.format(sampling_method="{sampling_method}", date_lb=lb, date_ub=ub)
            results_mapping = []
            if agg == "SUM":
                results_mapping.append({"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"})
            elif agg == "AVG":
                results_mapping.append({"aggregate": "avg", "mean": "avg_1", "std": "std_1"})
            elif agg == "COUNT":
                results_mapping.append({"aggregate": "count", "size": "sample_size"})
            queries.append(query)
            uniform_queries.append({
                "pilot_query": uniform_query,
                "results_mapping": results_mapping,
                "subquery_dict": []
            })
    return queries, uniform_queries

def gen_query_3(n_employee_dist: dict, n_query: int):
    n_employees = list(n_employee_dist.keys())
    queries = []
    uniform_queries = []
    for _ in range(n_query):
        n_employee = random.choice(n_employees)
        for agg1 in aggs:
            for agg2 in aggs:
                query = template_3.format(AGG1=agg1, AGG2=agg2, n_employee=n_employee)
                uniform_query = template_3_uniform.format(sampling_method="{sampling_method}", n_employee=n_employee)
                queries.append(query)
                results_mapping = []
                if agg1 == "COUNT":
                    results_mapping.append({"aggregate": "count", "size": "sample_size"})
                elif agg1 == "AVG":
                    results_mapping.append({"aggregate": "avg", "mean": "avg_1", "std": "std_1"})
                elif agg1 == "SUM":
                    results_mapping.append({"aggregate": "sum", "mean": "avg_1", "std": "std_1", "size": "sample_size"})
                if agg2 == "COUNT":
                    results_mapping.append({"aggregate": "count", "size": "sample_size"})
                elif agg2 == "AVG":
                    results_mapping.append({"aggregate": "avg", "mean": "avg_2", "std": "std_2"})
                elif agg2 == "SUM":
                    results_mapping.append({"aggregate": "sum", "mean": "avg_2", "std": "std_2", "size": "sample_size"})
                uniform_queries.append({
                    "pilot_query": uniform_query,
                    "results_mapping": results_mapping,
                    "subquery_dict": []
                })
    return queries, uniform_queries

def main(args):
    os.system("rm -f query_*.sql")
    random.seed(args.seed)
    date_dist = parse_date_dist()
    n_employee_dist = parse_n_employee_dist()
    queries_1, queries_1_uniform = gen_query_1(date_dist, args.n_query)
    queries_2, queries_2_uniform = gen_query_2(date_dist, args.n_query)
    queries_3, queries_3_uniform = gen_query_3(n_employee_dist, args.n_query)
    qid = 1
    for query, uniform_query in zip(queries_1, queries_1_uniform):
        with open("query_{}.sql".format(qid), "w") as f:
            f.write(query)

        uniform_query_info = uniform_query_template.format(pilot_query=uniform_query["pilot_query"], 
                                                           results_mapping=json.dumps(uniform_query["results_mapping"]), 
                                                           subquery_dict=json.dumps(uniform_query["subquery_dict"]))
        with open("../uniform/dsb_s-{}.py".format(qid), "w") as f:
            f.write(uniform_query_info)

        qid += 1

    for query, uniform_query in zip(queries_2, queries_2_uniform):
        with open("query_{}.sql".format(qid), "w") as f:
            f.write(query)
        
        uniform_query_info = uniform_query_template.format(pilot_query=uniform_query["pilot_query"], 
                                                           results_mapping=json.dumps(uniform_query["results_mapping"]), 
                                                           subquery_dict=json.dumps(uniform_query["subquery_dict"]))
        with open("../uniform/dsb_s-{}.py".format(qid), "w") as f:
            f.write(uniform_query_info)

        qid += 1

    for query, uniform_query in zip(queries_3, queries_3_uniform):
        with open("query_{}.sql".format(qid), "w") as f:
            f.write(query)
        
        uniform_query_info = uniform_query_template.format(pilot_query=uniform_query["pilot_query"], 
                                                           results_mapping=json.dumps(uniform_query["results_mapping"]), 
                                                           subquery_dict=json.dumps(uniform_query["subquery_dict"]))
        with open("../uniform/dsb_s-{}.py".format(qid), "w") as f:
            f.write(uniform_query_info)
    
        qid += 1
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_query", type=int, default=10)
    parser.add_argument("--seed", type=int, default=2333)
    args = parser.parse_args()
    main(args)


