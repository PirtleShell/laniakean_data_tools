# -*- coding: utf-8 -*-
"""
fill cosmicflows data table with a pipefile from EDD
data available at http://edd.ifa.hawaii.edu/dfirst.php
"""
import db

def fill_cosmicflows(pipe_filename):
    """ fill the cosmicflows table from pipefile. to be run once.
    schema:
    cosmicflows
    (id INTEGER PRIMARY KEY AUTOINCREMENT, pgc INTEGER, dist NUMERIC,
    numDist INTEGER, ra NUMERIC, dec NUMERIC, B NUMERIC, Ks NUMERIC,
    vhel NUMERIC)
    """
    # connect to db
    conn = db.connect()
    cur = conn.cursor()

    # translate EDD column names to what lanex cosmicflows table uses
    # EDD: lanex
    columns = {
        'pgc': 'pgc',
        'Dist': 'dist',
        'Nd': 'numDist',
        'RAJ': 'ra',
        'DeJ': 'dec',
        'Btot': 'B',
        'Ks': 'Ks',
        'Vhel': 'vhel'
    }

    print('Reading Cosmicflows data')
    data = db.read_pipe_file(pipe_filename)

    print('Inserting data')
    i = 0
    for galaxy in data:
        if i % 500 == 0:
            print('completed {}'.format(i))
        cols = [key for key in galaxy.keys() if key in columns.keys()]
        vals = [str(galaxy[col]) for col in cols]
        cols = [columns[col] for col in cols]
        insert_cmd = 'INSERT INTO cosmicflows(' + ', '.join(cols) + ') '
        insert_cmd += 'VALUES (' + ', '.join(vals) + ')'
        cur.execute(insert_cmd)
        i += 1

    print('done. committing insertions.')
    conn.commit()
    print('closing connection')
    conn.close()

def fix_ra_dec_strings(pipe_filename):
    """ fix right ascension and declination in cosmicflows
    originally inserted wrong RA & dec data
    updated with proper
    """
    # connect to db
    conn = db.connect()
    cur = conn.cursor()

    print('Reading Cosmicflows data')
    data = db.read_pipe_file(pipe_filename)

    i = 0
    for galaxy in data:
        if i % 500 == 0:
            print('updated {}'.format(i))

        update = 'UPDATE cosmicflows SET ra=\"' + galaxy['RAJ'] + '\", '
        update += 'dec =\"' + galaxy['DeJ'] + '\" '
        update += 'WHERE pgc=' + galaxy['pgc'] + ' LIMIT 1'
        # print(update)

        cur.execute(update)
        i += 1

    print('done. committing insertions.')
    conn.commit()
    print('closing connection')
    conn.close()

if __name__ == '__main__':
    # fill_cosmicflows('../cosmicflows3.all.txt')
    # fix_ra_dec_strings('extra/cosmicflows3.all.txt');
    pass
