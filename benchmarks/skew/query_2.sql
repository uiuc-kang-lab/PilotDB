SELECT 
    SUM(l_quantity) 
FROM 
    lineitem, 
    orders 
WHERE l_orderkey = o_orderkey