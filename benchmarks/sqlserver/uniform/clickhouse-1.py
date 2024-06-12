pilot_query = """
SELECT COUNT(*) as sample_size
FROM hits 
where RAND(CHECKSUM(NEWID())) < {sampling_method};
"""

sampling_query = '''
SELECT COUNT_BIG(*) / {sample_rate}
FROM hits
where RAND(CHECKSUM(NEWID())) < {sampling_method};
'''

results_mapping = [
    {"aggregate": "count", "size": "sample_size"}
]

subquery_dict = []