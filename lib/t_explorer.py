# -*- coding: utf-8 -*-
"""
fill explorer table from pipefile
"""
import numpy as np
import db

def fill_explorer(pipe_filename):
    """ fill explorer table. to be run once.
    computes 3D x, y, z values from dist, RA & dec
    """
    # connect to db
    conn = db.connect()
    cur = conn.cursor()

    print('Reading Cosmicflows data')
    data = db.read_pipe_file(pipe_filename)

    i = 0
    for galaxy in data:
        if i % 500 == 0:
            print('completed {}'.format(i))

        dist = galaxy['Dist']
        ra = db.ra_in_rad(galaxy['RAJ'])
        dec = db.dec_in_rad(galaxy['DeJ'])

        # swap standard y and z due to canvas coordinate orientation
        # we'll want the galactic plane in the x-z plane of the canvas
        x = dist * np.cos(dec) * np.cos(ra)
        z = dist * np.cos(dec) * np.sin(ra)
        y = dist * np.sin(dec)

        loc = (str(galaxy['pgc']), str(x), str(y), str(z))
        insert_cmd = 'INSERT INTO explorer(pgc, x, y, z) '
        insert_cmd += 'VALUES (?, ?, ?, ?)'

        cur.execute(insert_cmd, loc)
        i += 1

    print('done. committing insertions.')
    conn.commit()
    print('closing connection')
    conn.close()

if __name__ == '__main__':
    print('it loaded!')
    # fill_explorer('extra/cosmicflows3.all.txt')
    # pass
