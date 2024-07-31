
from ticketfinder.operations.ticket import extract_flows, extract_tickets
from ticketfinder.operations.station import extract_stations, extract_clusters, extract_group_members
from ticketfinder.operations.databases import search_flows

"""create the functions for searching the database"""

# files which contain the data

ffl_file = r"C:\Users\waffl\Downloads\farefinder\data\tickets\fares\tickets\adult fares.FFL"
loc_file = r"C:\Users\waffl\Downloads\farefinder\data\tickets\names\locations.LOC"
fsc_file = r"C:\Users\waffl\Downloads\farefinder\data\tickets\names\clusters.FSC"

# extraction

flows = extract_flows(ffl_file)
tickets = extract_tickets(ffl_file)
stations = extract_stations(loc_file)
clusters = extract_clusters(fsc_file)
group_members = extract_group_members(loc_file)
data = (flows, tickets, stations, clusters, group_members)

"""flows = data[0]
tickets = data[1]
stations = data[2]
clusters = data[3]
group_members = data[4]"""

# implementation

def find_NLCs(NLC_code, data):

    flows = data[0]
    tickets = data[1]
    stations = data[2]
    clusters = data[3]
    group_members = data[4]

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


def find_fares(origin_CRS, destination_CRS, data):
    """Finds a list of fares between 2 stations using their CRS codes."""

    flows = data[0]
    tickets = data[1]
    stations = data[2]
    clusters = data[3]
    group_members = data[4]

    # find the origin and destination NLC from CRS
    print("finding stations")

    # find the NLC of the origin
    origin = []
    for station in stations:
        if station[9] == origin_CRS:
            origin.append(station)
    origin_NLC = origin[0][7]

    # find the NLCs of the origin
    origin_codes = find_NLCs(origin_NLC, data)

    # stop if there is no origin
    if origin_codes == []:
        raise NameError("could not find origin station")

    # find the NLC of the destination
    destination = []
    for station in stations:
        if station[9] == destination_CRS:
            destination.append(station)
    destination_NLC = destination[0][7]

    # find the NLCs of the destination
    destination_codes = find_NLCs(destination_NLC, data)

    # stop if there is no destination
    if destination_codes == []:
        raise NameError("could not find destination station")

    # find the flows that go between them

    print("finding routes")

    fare_flows = []

    for onlc in origin_codes:
        for dnlc in destination_codes:
            fare_flows_to_add = search_flows(onlc, dnlc, flows)
            for flow in fare_flows_to_add:
                fare_flows.append(flow)

    if fare_flows == []:
        raise KeyError("could not find a route between stations")

    # find the fares on that flow

    print("finding fares")

    fares = []
    for flow in fare_flows:
        flow_fares = []
        for ticket in tickets:
            if ticket[2] == flow[14]:
                flow_fares.append(ticket)
        for fare in flow_fares:
            fares.append(fare)

    return fares

def sort_fares(fares, key):

    # allow for sorting by different keys

    if key == "price":
        return sorted(fares, key=lambda tup: tup[4])

test1 = find_fares("CBG", "TTF", data)
test2 = sort_fares(test1, "price")

print(test2)