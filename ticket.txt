from .databases import read_lines

"""defines some functions to unpack ticket data"""

def dismantle_flow(flow):
    
    """takes a flow from a string of info into an ordered tuple"""

    n = " " + flow
    refresh = n[1]
    record_type = n[2]
    origin_NLC = n[3:7]
    destination_NLC = n[7:11]
    route = n[11:16]
    status = n[16:19]
    type = n[19]
    direction = n[20]
    end_date = n[21:29]
    start_date = n[29:37]
    operator = n[37:40]
    via_london = n[40]
    standard_discount = n[41]
    fare_manual = n[42]
    flow_number = n[43:50]
    return (refresh, record_type, origin_NLC, destination_NLC, route, status, type, direction,
            start_date, end_date, operator, via_london, standard_discount, fare_manual, flow_number)

def extract_flows(file):
    
    """arguments: extraction file, destination file
    takes flows out of their file and puts them in the database"""

    flows = []

    flow_text = read_lines(file)
    for flow in flow_text:
        if flow[0] == "R":
            if flow[1] == "F":

                flow_data = dismantle_flow(flow)
                flows.append(flow_data)

    print("extracted flows")
    return flows

# ticket unpacking

def dismantle_ticket(ticket):
    
    """takes a ticket from a string of info into an ordered tuple"""

    n = " " + ticket
    refresh = n[1]
    record_type = n[2]
    flow = n[3:10]
    ticket_code = n[10:13]
    fare = n[13:21]
    restriction = n[21:23]
    return (refresh, record_type, flow, ticket_code, fare, restriction)

def extract_tickets(file):
    
    """arguments: extraction file, destination file
    takes tickets out of their file and puts them in the database"""

    n = 0
    tickets = []

    ticket_text = read_lines(file)
    for ticket in ticket_text:
        if ticket[0] == "R":
            if ticket[1] == "T":

                n += 1
                ticket_data = dismantle_ticket(ticket)
                tickets.append(ticket_data)

    print("extracted tickets")
    return tickets