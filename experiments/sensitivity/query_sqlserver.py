original_query = """ select
      sum(l_extendedprice * l_discount) as revenue
  from
      lineitem
  where
      l_shipdate >= '1994-01-01'
      and l_shipdate < DATEADD(YEAR, 1, '1994-01-01')
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < 24;"""

query_sel = """ select
      sum(l_extendedprice * l_discount) as revenue
  from
      lineitem
  where
      l_shipdate >= '1994-01-01'
      and l_shipdate < DATEADD(YEAR, 1, '1994-01-01')
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < {pred};"""

preds = [6, 12, 16, 22, 26]
all_count = 248254408
count = [24827442, 54618559, 74487164, 104277344, 124141749]
