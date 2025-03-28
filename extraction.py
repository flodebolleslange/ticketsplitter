from ticketsplitter.ticket import extract_flows, extract_tickets
from ticketsplitter.station import extract_stations, extract_clusters, extract_members
from ticketsplitter.routes import extract_routes, extract_links, extract_route_points
from ticketsplitter.restrictions import Restrictions
import pandas as pd
from pandas import DataFrame

"""create the functions for searching the database"""

# files which contain the data

ffl_file = r"C:\Code\Raw data\fares\RJFAF154.FFL"
fsc_file = r"C:\Code\Raw data\fares\RJFAF154.FSC"
loc_file = r"C:\Code\Raw data\fares\RJFAF154.LOC"
rgl_file = r"C:\Code\Raw data\routeing\RJRG0831.RGL"
rgr_file = r"C:\Code\Raw data\routeing\RJRG0831.RGR"
rgs_file = r"C:\Code\Raw data\routeing\RJRG0831.RGS"
rts_file = r"C:\Code\Raw data\fares\RJFAF154.RST"

# extraction

#flows = extract_flows(ffl_file)
#tickets = extract_tickets(ffl_file)
stations = extract_stations(loc_file)
clusters = extract_clusters(fsc_file)
group_members = extract_members(loc_file)
routes = extract_routes(rgr_file)
links = extract_links(rgl_file)
route_points = extract_route_points(rgs_file)
Restrictions.extract_restrictions(rts_file)

# insert into csv

#flows.to_csv(r"C:\Code\Ticketsplitter data\flows.csv")
#tickets.to_csv(r"C:\Code\Ticketsplitter data\tickets.csv")
stations.to_csv(r"C:\Code\Ticketsplitter data\stations.csv")
clusters.to_csv(r"C:\Code\Ticketsplitter data\clusters.csv")
group_members.to_csv(r"C:\Code\Ticketsplitter data\group members.csv")
routes.to_csv(r"C:\Code\Ticketsplitter data\routes.csv")
links.to_csv(r"C:\Code\Ticketsplitter data\links.csv")
route_points.to_csv(r"C:\Code\Ticketsplitter data\route points.csv")