from .databases import read_lines

"""defines some functions to unpack station data"""

def dismantle_station(station):
    
    """takes a station from a string of info into an ordered tuple"""

    n = " " + station
    refresh = n[1]
    record_type = n[2]
    UIC_code = n[3:10]
    end_date = n[10:18]
    start_date = n[18:26]
    quote_date = n[26:34]
    admin_area = n[34:37]
    NLC_code = n[37:41]
    description = n[41:57]
    CRS_code = n[57:60]
    IR_code = n[60:65]
    ERS_country = n[65:67]
    ERS_code = n[67:70]
    fare_group = n[70:76]
    county = n[76:78]
    PTE_code = n[78:80]
    zone_number = n[80:84]
    zone_index = n[84:86]
    region = n[86]
    hierarchy = n[87]
    cc_description_out = n[88:129]
    cc_description_return = n[129:144]
    ATB_description_out = n[145:205]
    ATB_description_return = n[205:235]
    facilities = n[235:261]
    LUL_encoding = n[261:290]

    return (refresh, 
            record_type,
            UIC_code,
            end_date,
            start_date,
            quote_date,
            admin_area,
            NLC_code,
            description,
            CRS_code,
            IR_code,
            ERS_country,
            ERS_code,
            fare_group,
            county,
            PTE_code,
            zone_number,
            zone_index,
            region,
            hierarchy,
            cc_description_out,
            cc_description_return,
            ATB_description_out,
            ATB_description_return,
            facilities,
            LUL_encoding)

def extract_stations(file):
    
    """arguments: extraction file, destination file
    takes stations out of their file and puts them in the database"""

    stations = []

    station_text = read_lines(file)
    for station in station_text:
        if station[0] == "R":
            if station[1] == "L":

                station_data = dismantle_station(station)
                stations.append(station_data)
    
    print("extracted stations")
    return stations

# group_member unpacking

def dismantle_cluster(cluster):
    
    """takes a cluster from a string of info into an ordered tuple"""

    n = " " + cluster
    refresh = n[1]
    cluster_NLC = n[2:6]
    member_NLC = n[6:10]
    start_date = n[18:26]
    end_date = n[10:18]

    return (refresh, cluster_NLC, member_NLC, end_date, start_date)

def extract_clusters(file):
    
    """arguments: extraction file, destination file
    takes clusters out of their file and puts them in the database"""

    clusters = []

    cluster_text = read_lines(file)
    for cluster in cluster_text:
        if cluster[0] == "R":

            cluster_data = dismantle_cluster(cluster)
            clusters.append(cluster_data)

    print("extracted clusters")
    return clusters

# group_member unpacking

def dismantle_group_member(group_member):
    
    """takes a group_member from a string of info into an ordered tuple"""

    n = " " + group_member
    refresh = n[1]
    record_type = n[2]
    group_UIC = n[3:10]
    end_date = n[10:18]
    member_UIC = n[18:25]
    member_CRS = n[25:28]

    return (refresh, record_type, group_UIC, end_date, member_UIC, member_CRS)

def extract_group_members(file):
    
    """arguments: extraction file, destination file
    takes group_members out of their file and puts them in the database"""

    group_members = []

    group_member_text = read_lines(file)
    for group_member in group_member_text:
        if group_member[0] == "R" and group_member[1] == "M":

            group_member_data = dismantle_group_member(group_member)
            group_members.append(group_member_data)

    print("extracted group members")
    return group_members