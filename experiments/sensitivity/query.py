original_query = """ select
      sum(l_extendedprice * l_discount) as revenue
  from
      lineitem
  where
      l_shipdate >= date '1994-01-01'
      and l_shipdate < date '1994-01-01' + interval '1' year
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < 24;"""

query_sel = """ select
      sum(l_extendedprice * l_discount) as revenue
  from
      lineitem
  where
      l_shipdate >= date '1994-01-01'
      and l_shipdate < date '1994-01-01' + interval '1' year
      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
      and l_quantity < {pred};"""

preds = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26]
all_count = 248254408
count = [
    4961996,
    14891316,
    24827442,
    34755878,
    44687646,
    54618559,
    64551475,
    74487164,
    84415959,
    94348307,
    104277344,
    114213142,
    124141749,
]
