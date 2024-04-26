from pilotdb.pilot_engine.commons import *

from typing import Dict, List
import math

def aggregate_error_to_page_error(column_mapping: List[Dict], required_error: float=0.05):
    page_errors = {}
    for aggregate in column_mapping:
        if aggregate['aggregate'] == SUM_OPERATOR:
            page_required_error = min(1-math.sqrt(1-required_error), 
                                      math.sqrt(required_error+1)-1)
            
            if aggregate[PAGE_SUM] in page_errors:
                page_errors[aggregate[PAGE_SUM]] = min(page_errors[aggregate['page_sum']], page_required_error)
            else:
                page_errors[aggregate[PAGE_SUM]] = page_required_error

            if "n_page" in page_errors:
                page_errors["n_page"] = min(page_errors["n_page"], page_required_error)
            else:
                page_errors["n_page"] = page_required_error

        elif aggregate['aggregate'] == AVG_OPERATOR:
            page_required_error = required_error / (2 - required_error)
            
            if aggregate[PAGE_SUM] in page_errors:
                page_errors[aggregate[PAGE_SUM]] = min(page_errors[aggregate['page_sum']], page_required_error)
            else:
                page_errors[aggregate[PAGE_SUM]] = page_required_error
            
            if aggregate[PAGE_SIZE] in page_errors:
                page_errors[aggregate[PAGE_SIZE]] = min(page_errors[aggregate['page_size']], page_required_error)
            else:
                page_errors[aggregate[PAGE_SIZE]] = page_required_error

        elif aggregate['aggregate'] == COUNT_OPERATOR:
            page_required_error = min(1-math.sqrt(1-required_error), 
                                      math.sqrt(required_error+1)-1)

            if aggregate[PAGE_SIZE] in page_errors:
                page_errors[aggregate[PAGE_SIZE]] = min(page_errors[aggregate['page_size']], page_required_error)
            else:
                page_errors[aggregate[PAGE_SIZE]] = page_required_error

            if "n_page" in page_errors:
                page_errors["n_page"] = min(page_errors["n_page"], page_required_error)
            else:
                page_errors["n_page"] = page_required_error
        elif aggregate['aggregate'] == DIV_OPERATOR:
            sum_required_error = required_error / (2 - required_error)
            page_required_error = min(1-math.sqrt(1-sum_required_error), 
                                      math.sqrt(sum_required_error+1)-1)
            if aggregate[FIRST_ELEMENT] in page_errors:
                page_errors[aggregate[FIRST_ELEMENT]] = min(page_errors[aggregate[FIRST_ELEMENT]], page_required_error)
            else:
                page_errors[aggregate[FIRST_ELEMENT]] = page_required_error
            
            if aggregate[SECOND_ELEMENT] in page_errors:
                page_errors[aggregate[SECOND_ELEMENT]] = min(page_errors[aggregate[SECOND_ELEMENT]], page_required_error)
            else:
                page_errors[aggregate[SECOND_ELEMENT]] = page_required_error
        else:
            raise NotImplemented(f"operator {aggregate['aggregate']} is not implemented")

    return page_errors