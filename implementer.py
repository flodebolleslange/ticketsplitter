from ticketsplitter.searcher import import_data
from ticketsplitter.routes import find_routes, find_maps
from ticketsplitter.searcher import find_fares
from ticketsplitter.restrictions import Restrictions

flows, tickets, stations, station_clusters, station_groups, routes, links, route_points = import_data()

import sys
sys.setrecursionlimit(10000)

test_object = Restrictions()
print(test_object.find_restriction("TN"))

#print(find_routes("NCL", "G21", routes, links))

#print(find_fares("7725", "7728", flows, tickets, station_groups, station_clusters))
