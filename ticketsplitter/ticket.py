from .databases import read_lines
import pandas as pd

"""defines some functions to unpack ticket data"""

def extract_flows(file):

    """extracts flows to a dataframe"""
    
    flow_text = read_lines(file)

    refresh = []
    record_type = []
    origin_NLC = []
    destination_NLC = []
    route = []
    status = []
    flow_type = []
    direction = []
    start_date = []
    end_date = []
    operator = []
    via_london = []
    standard_discount = []
    fare_manual = []
    flow_number = []

    for flow in flow_text:
        if flow[0] == "R":
            if flow[1] == "F":
                n = " " + flow

                refresh.append(n[1])
                record_type.append(n[2])
                origin_NLC.append(n[3:7])
                destination_NLC.append(n[7:11])
                route.append(n[11:16])
                status.append(n[16:19])
                flow_type.append(n[19])
                direction.append(n[20])
                end_date.append(n[21:29])
                start_date.append(n[29:37])
                operator.append(n[37:40])
                via_london.append(n[40])
                standard_discount.append(n[41])
                fare_manual.append(n[42])
                flow_number.append(n[43:50])

    pd.Series(refresh),
    pd.Series(record_type),
    pd.Series(origin_NLC),
    pd.Series(destination_NLC),
    pd.Series(route),
    pd.Series(status),
    pd.Series(flow_type),
    pd.Series(direction),
    pd.Series(start_date),
    pd.Series(end_date),
    pd.Series(operator),
    pd.Series(via_london),
    pd.Series(standard_discount),
    pd.Series(fare_manual),
    pd.Series(flow_number)

    flowsdf = pd.DataFrame(
        {"refresh": refresh,
        "record type": record_type,
        "origin NLC": origin_NLC,
        "destination_NLC": destination_NLC,
        "route": route,
        "status": status,
        "flow_type": flow_type,
        "direction": direction,
        "start_date": start_date,
        "end_date": end_date,
        "operator": operator,
        "via_london": via_london,
        "standard_discount": standard_discount,
        "fare_manual": fare_manual,
        "flow_number": flow_number,
        }
    )

    return flowsdf

# ticket unpacking

def extract_tickets(file):

    ticket_text = read_lines(file)

    refresh = []
    record_type = []
    flow = []
    ticket_code = []
    fare = []
    restriction = []

    for ticket in ticket_text:
        if ticket[0] == "R":
            if ticket[1] == "T":
                n = " " + ticket

                refresh.append(n[1])
                record_type.append(n[2])
                flow.append(n[3:10])
                ticket_code.append(n[10:13])
                fare.append(n[13:21])
                restriction.append(n[21:23])

    pd.Series(refresh)
    pd.Series(record_type)
    pd.Series(flow) 
    pd.Series(ticket_code) 
    pd.Series(fare) 
    pd.Series(restriction)

    ticketsdf = pd.DataFrame(
    {
    "refresh": refresh,
    "record_type": record_type,
    "flow": flow,
    "ticket_code": ticket_code,
    "fare": fare,
    "restriction": restriction,
    })

    return ticketsdf