# add the csv importing from the extractor

def find_NLCs(NLC_code):

    """creates a list of NLCs associated with a station"""
    codes = []
    codes.append(NLC_code)

    # find the groups the station is in
    UIC_code = "70" + NLC_code + "0"

    for group in group_members:
        if group[4] == UIC_code:
            group_NLC = group[2][2:6]
            codes.append(group_NLC)

    # find the clusters the station is in
    search_codes = codes
    for NLC_code in search_codes:
        for cluster in clusters:
            if cluster[2] == NLC_code:
                codes.append(cluster[1])

    return codes