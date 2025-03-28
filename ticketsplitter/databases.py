"""Some useful universal functions for database extraction and/or searching"""

def read_lines(file):
    f = open(file, "r")
    return f.readlines()