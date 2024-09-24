import polars as pl

"""includes functions that help with searching"""

# import data from csv (see extraction)

df_flows = pl.read_csv(r"C:\Code\Ticketsplitter data\flows.csv",
                     schema_overrides={"origin_NLC": str, "destination_NLC": str,
                                       "flow_number": str})
df_tickets = pl.read_csv(r"C:\Code\Ticketsplitter data\tickets.csv",
                         schema_overrides={"flow": str})
df_stations = pl.read_csv(r"C:\Code\Ticketsplitter data\stations.csv",
                       schema_overrides={"UIC code": str, 
                                         "National Location Code": str,
                                         "county": str})
df_clusters = pl.read_csv(r"C:\Code\Ticketsplitter data\clusters.csv",
                       schema_overrides={"member code": str})
df_groups = pl.read_csv(r"C:\Code\Ticketsplitter data\group_members.csv",
                     schema_overrides={"group_UIC": str,
                                       "member_UIC": str})

flows = pl.SQLContext(flows_table=df_flows, eager=True)
tickets = pl.SQLContext(tickets_table=df_tickets, eager=True)
stations = pl.SQLContext(stations_table=df_stations, eager=True)
station_clusters = pl.SQLContext(clusters_table=df_clusters, eager=True)
station_groups = pl.SQLContext(groups_table=df_groups, eager=True)

def find_NLCs(NLC_code, station_groups, station_clusters):

    """creates a list of NLCs associated with a station"""
    codes = []
    codes.append(NLC_code)

    # find the groups the station is in
    UIC_code = "70" + str(NLC_code) + "0"

    station_group = station_groups.execute(
        """SELECT group_UIC FROM groups_table
          WHERE STARTS_WITH(member_UIC,'""" + UIC_code + """')"""
    )
    group_NLCs = station_group["group_UIC"].to_list()
    for NLC in group_NLCs:
        codes.append(NLC[2:6])

    return codes

def find_fares(origin_NLC, destination_NLC, flows, tickets, station_groups, station_clusters):

    origin_codes = find_NLCs(origin_NLC, station_groups, station_clusters)
    destination_codes = find_NLCs(destination_NLC, station_groups, station_clusters)
    search_flows = []

    for o_code in origin_codes:
        for d_code in destination_codes:
            more_flows = flows.execute(
            """
            WITH from_flows AS (
            SELECT * FROM flows_table WHERE STARTS_WITH(origin_NLC,'""" 
            + o_code
            + """')) SELECT flow_number FROM from_flows WHERE STARTS_WITH(destination_NLC,'""" + d_code
            + """')"""
            )
            list_flows = more_flows["flow_number"].to_list()
            for flow in list_flows:
                search_flows.append(flow)

    if len(search_flows) == 0:
        raise IndexError("no flow found")
    
    search_string = ""
    for flow_number in search_flows:
        search_string = search_string + "STARTS_WITH(flow,'" + flow_number + "') OR "
    search_string = search_string[:-3]

    result = tickets.execute(
    """
    SELECT *
    FROM tickets_table
    WHERE """ + search_string
    )

    return result