from .databases import read_lines
import pandas as pd
import polars as pl

"""defines some functions for unpacking routeing data"""

def extract_routes(file):

    routes_text = read_lines(file)

    origin = []
    destination = []
    routes = []
    descriptions = []

    for route in routes_text:
        if route[0] == "/":
            description = route[2:]
        if route[0] != "/":
            n = route
            descriptions.append(description)
            origin.append(n[0:3])
            destination.append(n[4:7])
            routes.append(n[8:])

    routesdf = pd.DataFrame({
        "description": pd.Series(descriptions),
        "origin_CRS": pd.Series(origin),
        "destination_CRS": pd.Series(destination),
        "maps": pd.Series(routes)
    })

    return routesdf

def extract_links(file):

    maps_text = read_lines(file)
    descriptions = []
    origin = []
    #destination = []
    maps = []

    for map in maps_text:
        if map[0] == "/" and map[1] != "!":
            descriptions.append(map[2:-1])
        elif map[0] != "/":
            n = map
            origin.append(n[0:7])
            #destination.append(n[4:7])
            maps.append(n[8:10])

    mapsdf = pd.DataFrame({
        "description": pd.Series(descriptions),
        "link": pd.Series(origin),
        #"destination": pd.Series(destination),
        "map": pd.Series(maps)
    })

    return mapsdf

def extract_route_points(file):
    route_text = read_lines(file)
    descriptions = []
    station_code = []
    route_points = []

    for station in route_text:
        if station[0] == "/" and station[1] != "!":
            descriptions.append(station[2:-1])
        elif station[0] != "/":
            code_list = station.split(',')
            station_code.append(code_list[0])
            code_list.pop(0)
            points = ""
            for code in code_list:
                points = points + code
            route_points.append(points)
    
    route_points_df = pd.DataFrame({
        "description": pd.Series(descriptions),
        "station_code": pd.Series(station_code),
        "route_points": pd.Series(route_points)
    })

    return route_points_df

def find_maps(origin_CRS, destination_CRS, routes):

    search_maps = routes.execute(
        """WITH from_routes AS (
        SELECT * FROM routes_table WHERE STARTS_WITH(origin_CRS,'"""
        + origin_CRS +"""')) SELECT maps FROM from_routes WHERE STARTS_WITH(destination_CRS,'"""
        + destination_CRS + """')"""
    )

    relevant_maps = []

    list_map_lists = search_maps["maps"].to_list()
    for item in list_map_lists:
        item = item[:-1]
        map_list = []
        while len(item) > 2:
            map_list.append(item[-2:])
            item = item[:-3]
        map_list.append(item)
        relevant_maps.append(map_list)

    return(relevant_maps)

def find_routes(origin_CRS, destination_CRS, routes, links):
    global n
    n = 0

    global past_station_list
    past_station_list = []

    search_list = find_maps(origin_CRS, destination_CRS, routes)
    relevant_links = []
    
    for search_maps in search_list:
        for search_map in search_maps:
            relevant_links_df = links.execute(
                """SELECT * FROM links_table WHERE STARTS_WITH(map,'""" + search_map + """')"""
            )
            links_in_map = relevant_links_df["link"].to_list()
            for link in links_in_map:
                relevant_links.append(link)

    def next_station(viable_links, relevant_links, used_route, known_routes, links):
        global n
        n += 1
        if n == 9999:
            raise MemoryError
        global past_station_list
        print(" ")
        print(" ")
        current_location = used_route[-1]
        past_station_list.append(current_location)

        def translate_CRS_list(list):
            result = []
            for code in list:
                route_points_df = links.execute(
                """SELECT description FROM links_table WHERE STARTS_WITH(link,'"""
                + code + """')""")
                route_points_to_sort = route_points_df["description"].to_list()
                addition = route_points_to_sort[0].split(' - ')
                result.append(addition[0])
            return result

        if used_route[-1] == destination_CRS:
            result = translate_CRS_list(used_route)
            past_station_list = translate_CRS_list(past_station_list)
            print(n)
            return result
            used_route = [origin_CRS]
            viable_links = [[]]

        print("viable links: ")
        print(viable_links)
        print(" ")

        if viable_links[-1] == ['new']:
            get_viable_links = []
            viable_links[-1] = []
            for link in relevant_links:
                if link[0:3] == current_location:
                    get_viable_links.append(link)
            for item in set(get_viable_links):
                viable_links[-1].append(item)
            viable_links.append(viable_links[-1])
            print("added more links")

            viable_links = viable_links[:-1]

        if viable_links[-1] == []:
            used_route.pop()
            viable_links.pop()
            return next_station(viable_links, relevant_links, used_route, known_routes, links)
        
        print("viable links: ")
        print(viable_links)
            
        for link in viable_links[-1]:
            test_route = used_route.copy()
            test_route.append(link[4:7])
            print("used route")
            print(used_route)
            print(" ")
            print("test route")
            print(test_route)
            print(" ")
            if len(set(test_route)) == len(test_route):
                print("adding link")
                print(link)
                print(" ")
                viable_links[-1].remove(link)
                viable_links.append(['new'])
                used_route.append(link[4:7])
                return next_station(viable_links, relevant_links, used_route, known_routes, links)
            else:
                if len(viable_links[-1]) == 1:
                    print("local links 1")
                    used_route.pop()
                    viable_links.pop()
                    print(viable_links)
                    return next_station(viable_links, relevant_links, used_route, known_routes, links)
                else:
                    print("removing link")
                    viable_links[-1].remove(link)
                    print(link)
                    print(viable_links[-1])
                    print(" ")
                    return next_station(viable_links, relevant_links, used_route, known_routes, links)
                    

    return next_station([['new']], relevant_links, [origin_CRS], [], links)