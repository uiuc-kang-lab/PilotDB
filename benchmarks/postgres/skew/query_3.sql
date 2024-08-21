SELECT 
    SUM(l_quantity), 
    EXTRACT(YEAR FROM o_orderdate) o_year
From 
    lineitem, 
    orders 
WHERE l_orderkey = o_orderkey 
GROUP BY o_year