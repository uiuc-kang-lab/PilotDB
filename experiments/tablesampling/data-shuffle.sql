SELECT AVG(l_discount) FROM lineitem WHERE RANDOM() < {sample_rate}