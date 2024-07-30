from pandasticket import extract_flows, extract_tickets
from pandasstation import extract_stations, extract_clusters, extract_group_members
import pandas as pd

"""create the functions for searching the database"""

# files which contain the data

ffl_file = r"C:\Users\simon\Documents\Code\RJFAF648\RJFAF648.FFL.FFL"
loc_file = r"C:\Users\simon\Documents\Code\RJFAF648\RJFAF648.LOC"
fsc_file = r"C:\Users\simon\Documents\Code\RJFAF648\RJFAF648.FSC"

# extraction

flows = extract_flows(ffl_file)
tickets = extract_tickets(ffl_file)
stations = extract_stations(loc_file)
clusters = extract_clusters(fsc_file)
group_members = extract_group_members(loc_file)