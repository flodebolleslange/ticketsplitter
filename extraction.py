from ticketsplitter.ticket import extract_flows, extract_tickets
from ticketsplitter.station import extract_stations, extract_clusters, extract_members
import pandas as pd
from pandas import DataFrame

"""create the functions for searching the database"""

# files which contain the data

ffl_file = r"C:\Code\Raw data\fares\RJFAF154.FFL"
loc_file = r"C:\Code\Raw data\fares\RJFAF154.LOC"
fsc_file = r"C:\Code\Raw data\fares\RJFAF154.FSC"

# extraction

flows = extract_flows(ffl_file)
tickets = extract_tickets(ffl_file)
stations = extract_stations(loc_file)
clusters = extract_clusters(fsc_file)
group_members = extract_members(loc_file)

# insert into csv

flows.to_csv(r"C:\Code\Ticketsplitter data\flows.csv")
tickets.to_csv(r"C:\Code\Ticketsplitter data\tickets.csv")
stations.to_csv(r"C:\Code\Ticketsplitter data\stations.csv")
clusters.to_csv(r"C:\Code\Ticketsplitter data\clusters.csv")
group_members.to_csv(r"C:\Code\Ticketsplitter data\group_members.csv")