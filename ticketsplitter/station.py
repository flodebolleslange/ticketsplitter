from .databases import read_lines
import pandas as pd

"""defines some functions to unpack station data"""

def extract_stations(file):

    refresh = [] 
    record_type = []
    UIC_code = []
    end_date = []
    start_date = []
    quote_date = []
    admin_area = []
    NLC_code = []
    description = []
    CRS_code = []
    IR_code = []
    ERS_country = []
    ERS_code = []
    fare_group = []
    county = []
    PTE_code = []
    zone_number = []
    zone_index = []
    region = []
    hierarchy = []
    cc_description_out = []
    cc_description_return = []
    ATB_description_out = []
    ATB_description_return = []
    facilities = []
    LUL_encoding = []

    station_text = read_lines(file)

    for station in station_text:
        if station[0] == "R":
            if station[1] == "L":

                n = " " + station

                refresh.append(n[1])
                record_type.append(n[2])
                UIC_code.append(n[3:10])
                end_date.append(n[10:18])
                start_date.append(n[18:26])
                quote_date.append(n[26:34])
                admin_area.append(n[34:37])
                NLC_code.append(n[37:41])
                description.append(n[41:57])
                CRS_code.append(n[57:60])
                IR_code.append(n[60:65])
                ERS_country.append(n[65:67])
                ERS_code.append(n[67:70])
                fare_group.append(n[70:76])
                county.append(n[76:78])
                PTE_code.append(n[78:80])
                zone_number.append(n[80:84])
                zone_index.append(n[84:86])
                region.append(n[86])
                hierarchy.append(n[87])
                cc_description_out.append(n[88:129])
                cc_description_return.append(n[129:144])
                ATB_description_out.append(n[145:205])
                ATB_description_return.append(n[205:235])
                facilities.append(n[235:261])
                LUL_encoding.append(n[261:290])

    stationdf = pd.DataFrame(
        {
        "refresh": pd.Series(refresh),
        "record type": pd.Series(record_type),
        "UIC code": pd.Series(UIC_code),
        "end date": pd.Series(end_date),
        "start date": pd.Series(start_date),
        "quote date": pd.Series(quote_date),
        "administration area": pd.Series(admin_area),
        "National Location Code": pd.Series(NLC_code),
        "description": pd.Series(description),
        "Reservation code": pd.Series(CRS_code),
        "International Reservation code": pd.Series(IR_code),
        "country": pd.Series(ERS_country),
        "ERS code": pd.Series(ERS_code),
        "fare group": pd.Series(fare_group),
        "county": pd.Series(county),
        "PTE code": pd.Series(PTE_code),
        "BR zone number": pd.Series(zone_number),
        "BR zone index": pd.Series(zone_index),
        "region": pd.Series(region),
        "hierarchy": pd.Series(hierarchy),
        "ticket text 1": pd.Series(cc_description_out),
        "ticket text 2": pd.Series(cc_description_return),
        "ticket text 3": pd.Series(ATB_description_out),
        "ticket text 4": pd.Series(ATB_description_return),
        "facilities": pd.Series(facilities),
        "LUL encoding": pd.Series(LUL_encoding),
        }
    )

    return stationdf

def extract_clusters(file):
    
    """takes a cluster from a string of info into an ordered tuple"""

    refresh = []
    cluster_NLC = [] 
    member_NLC = []
    end_date = []
    start_date = []

    cluster_text = read_lines(file)

    for cluster in cluster_text:
        if cluster[0] == "R":
            n = " " + cluster
            refresh.append(n[1])
            cluster_NLC.append(n[2:6])
            member_NLC.append(n[6:10])
            start_date.append(n[18:26])
            end_date.append(n[10:18])

    stationdf = pd.DataFrame(
        {
        "refresh": pd.Series(refresh),
        "cluster code": pd.Series(cluster_NLC), 
        "member code": pd.Series(member_NLC),
        "end date": pd.Series(end_date), 
        "start date": pd.Series(start_date),
        }
    )

    return stationdf

# group member unpacking

def extract_members(file):
    
    """takes a group_member from a string of info into an ordered tuple"""

    refresh = []
    record_type = []
    group_UIC = []
    end_date = []
    member_UIC = []
    member_CRS = []

    group_member_text = read_lines(file)

    for group_member in group_member_text:
        if group_member[0] == "R" and group_member[1] == "M":
            n = " " + group_member
            refresh.append(n[1])
            record_type.append(n[2])
            group_UIC.append(n[3:10])
            end_date.append(n[10:18])
            member_UIC.append(n[18:25])
            member_CRS.append(n[25:28])

    memberdf = pd.DataFrame(
        {
        "refresh": refresh,
        "record_type": record_type,
        "group_UIC": group_UIC,
        "end_date": end_date,
        "member_UIC": member_UIC,
        "member_CRS": member_CRS,
        }
        )
    
    return memberdf