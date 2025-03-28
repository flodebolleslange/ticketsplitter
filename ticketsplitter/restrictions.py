from .databases import read_lines
import pandas as pd
import polars as pl

"""various functions to analyse and store ticket restrictions"""

class Restriction():
    def __init__(self):
        pass
    
class Restrictions():
    
    def __init__(self):
        self.rhdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction headers.csv")
        self.hddf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction header date bands.csv")
        self.hldf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction header route locations.csv")
        self.hcdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction header allowed changes.csv")
        self.trdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\time restrictions.csv")
        self.tddf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\time restriction date bands.csv")
        self.ttdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\time restriction TOC.csv")
        self.srdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\train restrictions.csv")
        self.sddf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction date bands.csv")
        self.sqdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction quota exemptions.csv")
        self.spdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction privilege data.csv")
        self.sedf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction privilege pass exceptions.csv")
        self.rrdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\railcard restrictions.csv")
        self.ecdf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\exception codes.csv")
        self.cadf_sheet = pl.read_csv(r"C:\Code\Ticketsplitter data\restrictions\ticket calendars.csv")
        
        self.hdf_sheets = (self.rhdf_sheet, self.hddf_sheet, self.hldf_sheet, self.hcdf_sheet)
        self.tdf_sheets = (self.trdf_sheet, self.tddf_sheet, self.ttdf_sheet)
        self.sdf_sheets = (self.srdf_sheet, self.sddf_sheet, self.sqdf_sheet, self.spdf_sheet, self.sedf_sheet)
        self.df_sheets = self.hdf_sheets + self.tdf_sheets + self.sdf_sheets + (self.rrdf_sheet, self.ecdf_sheet, self.cadf_sheet)
        
        self.rhdf = pl.SQLContext(tableau=self.rhdf_sheet, eager=True)
        self.hddf = pl.SQLContext(tableau=self.hddf_sheet, eager=True)
        self.hldf = pl.SQLContext(tableau=self.hldf_sheet, eager=True)
        self.hcdf = pl.SQLContext(tableau=self.hcdf_sheet, eager=True)
        self.trdf = pl.SQLContext(tableau=self.trdf_sheet, eager=True)
        self.tddf = pl.SQLContext(tableau=self.tddf_sheet, eager=True)
        self.ttdf = pl.SQLContext(tableau=self.ttdf_sheet, eager=True)
        self.srdf = pl.SQLContext(tableau=self.srdf_sheet, eager=True)
        self.sddf = pl.SQLContext(tableau=self.sddf_sheet, eager=True)
        self.sqdf = pl.SQLContext(tableau=self.sqdf_sheet, eager=True)
        self.spdf = pl.SQLContext(tableau=self.spdf_sheet, eager=True)
        self.sedf = pl.SQLContext(tableau=self.sedf_sheet, eager=True)
        self.rrdf = pl.SQLContext(tableau=self.rrdf_sheet, eager=True)
        self.ecdf = pl.SQLContext(tableau=self.ecdf_sheet, eager=True)
        self.cadf = pl.SQLContext(tableau=self.cadf_sheet, eager=True)
        
        #self.dfs = (self.rhdf, self.hddf, self.hldf, self.hcdf, self.trdf, self.tddf, self.ttdf, self.srdf, self.sddf, self.sqdf, self.spdf, self.sedf, self.rrdf, self.ecdf, self.cadf)
        self.hdfs = (self.rhdf, self.hddf, self.hldf, self.hcdf)
        self.tdfs = (self.trdf, self.tddf, self.ttdf)
        self.sdfs = (self.srdf, self.sddf, self.sqdf, self.spdf, self.sedf)
        self.dfs = self.hdfs + self.tdfs + self.sdfs + (self.rrdf, self.ecdf, self.cadf)

    def extract_restrictions(file):

        restriction_text = read_lines(file)
        
        # restriction dates
        
        refresh = []
        record_type = []
        CF_marker = []
        date_from = []
        date_to = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "RD":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                date_from.append(n[5:13])
                date_to.append(n[13:21])
        rddf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "date from": pd.Series(date_from),
        "date to": pd.Series(date_to)})
        rddf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction dates.csv")
        
        # restriction header

        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        desc = []
        out_desc = []
        return_desc = []
        type_out = []
        type_return = []
        change = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "RH":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                desc.append(n[7:37])
                out_desc.append(n[37:87])
                return_desc.append(n[87:137])
                type_out.append(n[137])
                type_return.append(n[138])
                change.append(n[139])
        rhdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),  
        "description": pd.Series(desc),  
        "outbound description": pd.Series(out_desc),  
        "return description": pd.Series(return_desc),  
        "outbound": pd.Series(type_out),  
        "return": pd.Series(type_return),  
        "change allowed": pd.Series(change)})
        rhdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction headers.csv")
        
        # restriction header date bands

        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        date_from = []
        date_to = []
        days = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "HD":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                date_from.append(n[7:11])
                date_to.append(n[11:15])
                days.append(n[15:22])
        hddf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "date from": pd.Series(date_from),
        "date to": pd.Series(date_to),
        "days": pd.Series(days)})
        hddf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction header date bands.csv")
        
        # restriction header route locations
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        location_CRS = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "HL":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                location_CRS.append(n[7:10])
        hldf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "location": pd.Series(location_CRS)})  
        hldf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction header route locations.csv")
        
        # restriction header allowed changes
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        allowed_change = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "HC":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                allowed_change.append(n[7:10])
        hcdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "allowed change": pd.Series(allowed_change)})  
        hcdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\restriction header allowed changes.csv")
        
        # ha does not exist yet
        
        # time restriction
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        sequence_number = []
        direction = []
        time_from = []
        time_to = []
        action = []
        location = []
        timetable_type = []
        train_type = []
        min_fare_flag = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "TR":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                sequence_number.append(n[7:11])
                direction.append(n[11])
                time_from.append(n[12:16])
                time_to.append(n[16:20])
                action.append(n[20])
                location.append(n[21:24])
                timetable_type.append(n[24])
                train_type.append(n[25])
                min_fare_flag.append(n[26])
        trdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "sequence number": pd.Series(sequence_number),
        "direction": pd.Series(direction),
        "time from": pd.Series(time_from),
        "time to": pd.Series(time_to),
        "action": pd.Series(action),
        "location": pd.Series(location),
        "timetable type": pd.Series(timetable_type),
        "train type": pd.Series(train_type),
        "min fare flag": pd.Series(min_fare_flag)})  
        trdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\time restrictions.csv")
                
        # time restriction date bands
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        sequence_number = []
        direction = []
        time_from = []
        time_to = []
        days = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "TD":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                sequence_number.append(n[7:11])
                direction.append(n[11])
                time_from.append(n[12:16])
                time_to.append(n[16:20])
                days.append(n[20:27])
        tddf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "sequence number": pd.Series(sequence_number),
        "direction": pd.Series(direction),
        "time from": pd.Series(time_from),
        "time to": pd.Series(time_to),
        "days": pd.Series(days)})
        tddf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\time restriction date bands.csv")
        
        # time restriction TOC
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        sequence_number = []
        direction = []
        toc = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "TT":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                sequence_number.append(n[7:11])
                direction.append(n[11])
                toc.append(n[12:14])
        ttdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "sequence number": pd.Series(sequence_number),
        "direction": pd.Series(direction),
        "operator": pd.Series(toc)})
        ttdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\time restriction TOC.csv")
        
        # tp does not exist yet
        
        # te does not exist yet
        
        # train restriction
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        train_ID = []
        direction = []
        quota = []
        sleeper = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "SR":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                train_ID.append(n[7:13])
                direction.append(n[13])
                quota.append(n[14])
                sleeper.append(n[15])
        srdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "train ID": pd.Series(train_ID),
        "quota": pd.Series(quota),
        "sleeper": pd.Series(sleeper)})  
        srdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\train restrictions.csv")
        
        # train restriction date bands
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        train_ID = []
        direction = []
        date_from = []
        date_to = []
        days = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "SD":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                train_ID.append(n[7:13])
                direction.append(n[13])
                date_from.append(n[14:18])
                date_to.append(n[18:22])
                days.append(n[22:29])
        sddf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "train ID": pd.Series(train_ID),
        "date from": pd.Series(date_from),
        "date to": pd.Series(date_to),
        "days": pd.Series(days)})  
        sddf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction date bands.csv")
        
        # train restriction quota exemption
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        train_ID = []
        direction = []
        location = []
        quota = []
        action = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "SQ":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                train_ID.append(n[7:13])
                direction.append(n[13])
                location.append(n[14:17])
                quota.append(n[17])
                action.append(n[18])
        sqdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "train ID": pd.Series(train_ID),
        "direction": pd.Series(direction),
        "location": pd.Series(location),
        "quota": pd.Series(quota),
        "action": pd.Series(action)})  
        sqdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction quota exemptions.csv")
        
        # train restriction privilege data
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        train_ID = []
        direction = []
        classes = []
        tickets = []
        season = []
        first = []
        origin = []
        destination = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "SP":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                train_ID.append(n[7:13])
                direction.append(n[13])
                classes.append(n[14])
                tickets.append(n[15])
                season.append(n[16])
                first.append(n[17])
                origin.append(n[18:21])
                destination.append(n[21:24])
        spdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "train ID": pd.Series(train_ID),
        "direction": pd.Series(direction),
        "barred class": pd.Series(classes),
        "barred tickets": pd.Series(tickets),
        "barred season": pd.Series(season),
        "barred first": pd.Series(first),
        "origin": pd.Series(origin),
        "destination": pd.Series(destination)})
        spdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction privilege data.csv")
        
        # train restriction privilege pass exemption
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        train_ID = []
        direction = []
        exception = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "SE":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                train_ID.append(n[7:13])
                direction.append(n[13])
                exception.append(n[14])
        sedf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "train ID": pd.Series(train_ID),
        "direction": pd.Series(direction),
        "exception": pd.Series(exception)})
        sedf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\train restriction privilege pass exceptions.csv")
        
        # railcard restriction
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        sequence_number = []
        ticket = []
        route = []
        location = []
        restriction = []
        ban = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "RR":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                sequence_number.append(n[7:11])
                ticket.append(n[12:15])
                route.append(n[15:20])
                location.append(n[20:23])
                restriction_code.append(n[23:25])
                ban.append(n[25])
        rrdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "sequence number": pd.Series(sequence_number),
        "ticket": pd.Series(ticket),
        "route": pd.Series(route),
        "location": pd.Series(location),
        "restriction": pd.Series(restriction),
        "ban": pd.Series(ban)})
        rrdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\railcard restrictions.csv")   
        
        # exemption code
        
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        description = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "EC":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5])
                description.append(n[6:56])
        ecdf = pd.DataFrame({
        "refresh: ": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "description": pd.Series(description)})  
        ecdf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\exception codes.csv")                          
        
        # ticket calendars
                
        refresh = []
        record_type = []
        CF_marker = []
        restriction_code = []
        type = []
        route = []
        country = []
        date_from = []
        date_to = []
        days = []
        for restriction in restriction_text:
            n = " " + restriction
            if n[2:4] == "CA":
                refresh.append(n[1])
                record_type.append(n[2:4])
                CF_marker.append(n[4])
                restriction_code.append(n[5:7])
                type.append(n[8])
                route.append(n[9:14])
                country.append(n[14])
                date_from.append(n[15:19])
                date_to.append(n[19:23])
                days.append(n[23:30])               
        cadf = pd.DataFrame({
        "refresh": pd.Series(refresh),
        "record type": pd.Series(record_type),  
        "date marker": pd.Series(CF_marker),  
        "code": pd.Series(restriction_code),
        "type": pd.Series(type),
        "route": pd.Series(route),
        "country": pd.Series(country),
        "date from": pd.Series(date_from),
        "date to": pd.Series(date_to),
        "days": pd.Series(days)})  
        cadf.to_csv(r"C:\Code\Ticketsplitter data\restrictions\ticket calendars.csv")
        
    def find_restriction(self, code):
        restriction_data = []
        for df in self.dfs:
            restriction = df.execute(
            """SELECT * FROM tableau
            WHERE STARTS_WITH(code,'""" + code + """')""")
            restriction_data.append(restriction)
        return restriction_data
    
    def unpack_time_restirction(self, code):
        dataframes = self.find_restrictions(code)
    
    def unpack_train_restirction(self, code):
        pass