def get_log_file_path(dir: str, query_name: str, job_id: str) -> str:
    return f"{dir}/{query_name}-{job_id}.log"

def get_result_file_path(dir: str, query_name: str, job_id: str, method: str, dbms: str) -> str:
    return f"{dir}/{query_name}-{method}-{dbms}-{job_id}.csv"