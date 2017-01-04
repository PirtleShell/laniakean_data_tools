# -*- coding: utf-8 -*-
""" Laniakean Database Tools

Some one-time funcitons to fill database, as well as some tools for ease
of scripting data manipulation.

Robert Pirtle
"""
import sqlite3 as sql
import urllib.request
import json
import numpy as np
import parse

def read_pipe_file(pipe_filename):
    """ parse an ASCII pipe table
    turn a pipe table into a dict
    this project uses tables that are publicly accessible at
    http://edd.ifa.hawaii.edu/dfirst.php
    From an updated table originally published as Table 1 in
    Tully et al. 2013, AJ, 146, 86.
    """
    with open(pipe_filename, 'r') as myfile:
        data = myfile.read()
    lines = data.split('\n')
    header = [lines[i] for i in np.arange(0, 4)]

    # 2nd & 3rd lines are column names & variable type/length
    column_names = header[1].split('|')
    column_types = header[2].replace('s', 'S').split('|')

    # join the types as a format string for `parse`
    # {name1:type1} | {name2:type2} | ...
    parse_str = '|'.join(['{' + column_names[i] + ':' + column_types[-1] + '}'
                          for i, t in enumerate(column_types)])
    parse_query = parse.compile(parse_str)

    # remove header from galaxy data
    gal_data = [lines[i] for i in np.arange(5, len(lines)-1)]

    data = [parse_query.parse(gal_data[i]).named for i
            in np.arange(0, len(gal_data))]
    return data

def ra_in_rad(ra):
    """ right ascension in hhmmss.s to radians """
    # RA is a string formatted as hhmmss.s
    hrs = float(ra[:2])
    mins = float(ra[2:4])
    secs = float(ra[4:])

    degree = hrs*(360/24.0) + mins*(360/(24*60.0)) + secs*(360/(24*3600.0))
    rad = degree*(np.pi/180.0)
    return rad

def dec_in_rad(dec):
    """ declination in damas to radians
    dec is a string formatted as (+/-)ddamas
    """
    sign = 1.0 if dec[0] == '+' else -1.0
    degs = float(dec[1:3])
    mins = float(dec[3:5])
    secs = float(dec[5:])

    degree = sign * (degs + mins/60.0 + secs/3600.0)
    rad = degree * (np.pi/180.0)
    return rad

def connect():
    """ connect to lanex.db """
    print('Connecting to lanex.db.')
    return sql.connect('../../api/lanex_api/lanex.db')

def fetch_pgcs_from_api():
    """ fetches all pgcs from lanikean API """
    print('Fetching PGC numbers')
    url = 'http://laniakean.com/api/v1/galaxies/?list=true'
    with urllib.request.urlopen(url) as res:
        pgcs = json.loads(res.read().decode('utf8'))['pgcs']
    return pgcs


if __name__ == '__main__':
    pass
