from typing import Dict, List
import math

def aggregate_error_to_page_error(column_mapping: List[Dict], page_size_col: str, required_error: float=0.05):
    page_errors = {}
    for aggregate in column_mapping:
        if aggregate['aggregate'] == 'sum':
            page_required_error = min(1-math.sqrt(1-required_error), 
                                      math.sqrt(required_error+1)-1)
            
            if aggregate['page_sum'] in page_errors:
                page_errors[aggregate['page_sum']] = min(page_errors[aggregate['page_sum']], page_required_error)
            else:
                page_errors[aggregate['page_sum']] = page_required_error

            if "n_page" in page_errors:
                page_errors["n_page"] = min(page_errors["n_page"], page_required_error)
            else:
                page_errors["n_page"] = page_required_error

        elif aggregate['aggregate'] == 'avg':
            page_required_error = required_error / (2 - required_error)
            
            if aggregate['page_sum'] in page_errors:
                page_errors[aggregate['page_sum']] = min(page_errors[aggregate['page_sum']], page_required_error)
            else:
                page_errors[aggregate['page_sum']] = page_required_error
            
            if page_size_col in page_errors:
                page_errors[page_size_col] = min(page_errors[page_size_col], page_required_error)
            else:
                page_errors[page_size_col] = page_required_error

        elif aggregate['aggregate'] == 'count':
            page_required_error = min(1-math.sqrt(1-required_error), 
                                      math.sqrt(required_error+1)-1)

            if aggregate['page_size'] in page_errors:
                page_errors[aggregate['page_size']] = min(page_errors[aggregate['page_size']], page_required_error)
            else:
                page_errors[aggregate['page_size']] = page_required_error

            if "n_page" in page_errors:
                page_errors["n_page"] = min(page_errors["n_page"], page_required_error)
            else:
                page_errors["n_page"] = page_required_error
        
    return page_errors