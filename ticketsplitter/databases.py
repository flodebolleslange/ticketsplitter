"""defines some useful universal functions"""

def read_lines(file):
    f = open(file, "r")
    return f.readlines()

def search_flows(origin_code, destination_code, flows):
    """implements binary search for the flows document"""

    try:
        int(origin_code[1])
        # create data to use later
        searchlist = flows

        def numberise(code):
            """recieves a code as a string and gets rid of annoying letters at the start"""

            try:
                int(code)
                return int(code)
            
            except:
                priority = [("G", 0),
                            ("H", 1),
                            ("I", 2),
                            ("J", 3),
                            ("K", 4),
                            ("L", 5),
                            ("M", 6),
                            ("N", 7),
                            ("Q", 8),
                            ("R", 9),
                            ("S", 10),
                            ("T", 11),]
                
                letter = code[0]
                for item in priority:
                    if item[0] == letter:
                        number = int(str(item[1]) + code[1:])
                return number

        try:
            origin = int(origin_code)
            searchlist = searchlist[:307186]

            # midpoint store is the index of the first item in the searchlist in the full list of flows
            midpoint_store = 0

        except:

            searchlist = searchlist[307186:]
            origin = numberise(origin_code)

            # midpoint store is the index of the first item in the searchlist in the full list of flows
            midpoint_store = 307186

        while len(searchlist) > 1000:

            midpoint = len(searchlist) // 2 + midpoint_store
            quarterpoint = len(searchlist) // 4 + midpoint_store
            threequarterpoint = 3 * (len(searchlist) // 4) + midpoint_store

            # shave off the half that doesn't have the origin in it

            if numberise(flows[midpoint][2]) > origin:
                searchlist = searchlist[:len(searchlist)//2]

            elif numberise(flows[midpoint][2]) < origin:
                searchlist = searchlist[len(searchlist)//2:]
                midpoint_store = midpoint

            # shave off the top and bottom quarters if the midpoint is the origin

            elif numberise(flows[midpoint][2]) == origin:
                
                if numberise(flows[quarterpoint][2]) < origin:
                    if numberise(flows[threequarterpoint][2]) > origin:
                        searchlist = searchlist[len(searchlist)//4 : 3*len(searchlist)//4]
                    midpoint_store = midpoint - quarterpoint

                if numberise(flows[quarterpoint][2]) < origin:
                    searchlist = searchlist[len(searchlist)//4:]
                elif numberise(flows[threequarterpoint][2]) > origin:
                    searchlist = searchlist[:3*len(searchlist)//4]

        # length of searchlist will be less than 1000 at this point so time is not so precious
        # linear search through the remaining flows

    except:
        searchlist = flows

    result = []
    for flow in searchlist:
        if flow[2] == origin_code and flow[3] == destination_code:
            result.append(flow)

    return result