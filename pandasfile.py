from ticketfinder.operations.ticket import extract_flows, extract_tickets
from ticketfinder.operations.station import extract_stations, extract_clusters, extract_group_members
from ticketfinder.operations.databases import search_flows
import pandas as pd

"""create the functions for searching the database"""

# files which contain the data

ffl_file = r"C:\Users\waffl\Downloads\fares data\ticketfinder\data\fares\tickets\adult fares.FFL"
loc_file = r"C:\Users\waffl\Downloads\fares data\ticketfinder\data\names\locations.LOC"
fsc_file = r"C:\Users\waffl\Downloads\fares data\ticketfinder\data\names\stations.FSC"

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