SELECT o_orderpriority, COUNT(*) / sample_rate AS order_count FROM orders sampling_method WHERE o_orderdate >= CAST('1993-07-01' AS DATE) AND o_orderdate < CAST('1993-07-01' AS DATE) + INTERVAL '3' MONTH AND EXISTS(SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority