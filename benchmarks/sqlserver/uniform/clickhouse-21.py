pilot_query = """
SELECT COUNT(*) AS sample_size
FROM hits {sampling_method}
WHERE URL LIKE '%google%';
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