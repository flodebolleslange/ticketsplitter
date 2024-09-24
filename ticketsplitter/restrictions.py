from .databases import read_lines
import pandas as pd
import polars as pl

"""various functions to analyse and store ticket restrictions"""

def extract_restrictions(file):

    restriction_text = read_lines(file)

    refresh = []
    record_type = []
    CF_marker = []
    restriction_code = []
    desc = []
    out_desc = []
    return_desc = []
    type_out = []
    type_return = []
    change = []

    for restriction in restriction_text:
        n = " " + restriction
        if n[2:3] == "RH":
            refresh.append(n[1])
            record_type.append(n[2:4])
            CF_marker.append(n[4])
            restriction_code.append(n[5:7])
            desc.append(n[7:37])
            out_desc.append(n[37:87])
            return_desc.append(n[87:137])
            type_out.append(n[137])
            type_return.append(n[138])
            change.append(n[139])

    pd.Series(refresh)
    pd.Series(record_type)
    pd.Series(refresh)
    pd.Series(record_type)  
    pd.Series(CF_marker)  
    pd.Series(restriction_code)  
    pd.Series(desc)  
    pd.Series(out_desc)  
    pd.Series(return_desc)  
    pd.Series(type_out)  
    pd.Series(type_return)  
    pd.Series(change)

    rhdf = pd.DataFrame(


    )

    return rhdf

def find_restriction(restriction):

    rh = pl.SQLContext(rh_table=rhdf, eager=True)

    return 1