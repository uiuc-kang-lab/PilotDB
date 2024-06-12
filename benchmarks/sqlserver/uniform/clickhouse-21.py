pilot_query = """
SELECT COUNT(*) AS sample_size
FROM hits
WHERE URL LIKE '%google%'
AND RAND(CHECKSUM(NEWID())) < {sampling_method};
"""

sampling_query = '''
SELECT COUNT(*) / {sample_rate}
FROM hits
WHERE URL LIKE '%google%' {sampling_method};
'''
results_mapping = [
     {"aggregate": "count", "size": "sample_size"}
]

subquery_dict = []