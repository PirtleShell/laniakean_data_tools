# -*- coding: utf-8 -*-
""" filling catalogs and commonNames table

search SIMBAD for name data, input them into database:
 * catalog cross-referenes => catalogs table
 * common names for galaxy => commonNames table
"""
import sys
import sqlite3
import json
from astroquery.simbad import Simbad
import db

# SIMBAD catalogs translated to catalog table column names
DESIRED_CATALOGS = {
    'NGC': 'ngc',
    'UGC': 'ugc',
    'M': 'messier',
    'APG': 'arp',
    'SDSS': 'sdss',
}
# whether to store data as string. an artifact now, as they all are...
IS_TEXT = {
    'NGC': True,
    'UGC': True,
    'M': True,
    'APG': True,
    'SDSS': True
}

# pylint: disable=C0103
failed_pgcs = []
failed_queries = []
# pylint: enable=C0103

def names_and_catalogs(pgc, cursor):
    """ fill catalogs and commonNames tables

    uses astroquery to search SIMBAD for PGC (LEDA)
    extracts name data looking for desired catalog names
    when found, they build a query to insert into catalogs

    SIMBAD uses 'NAME' to signify a common name
    occurences are inserted into commonNames

    PGCs not recognized by SIMBAD are stored in failed_pgcs
    Queries that fail for other reasons (bad internet, malformed, etc) are
    added to failed_queries

    Args:
        pgc: the PGC number of the object, known in SIMBAD as LEDA
        cursor: the sqlite3 cursor for the database
    """
    print('PGC %d\r'%pgc, end="")
    sys.stdout.flush()

    try:
        names = Simbad.query_objectids('PGC ' + str(pgc))
    except ConnectionError:
        print(pgc, 'failed. INTERNET?')
        failed_pgcs.append(pgc)
        return

    # None is returned when SIMBAD doesn't recognize the galaxy by its pgc
    if names is None:
        print(pgc, 'failed. PGC not recognized by SIMBAD.')
        failed_pgcs.append(pgc)
        return

    cols = ['pgc']
    vals = [str(pgc)]
    for row in names:
        data = row['ID'].decode('utf-8')
        # extract catalog name
        catalog = data.split(' ')[0]
        # check if in catalog data table
        if catalog in DESIRED_CATALOGS:
            # remove catalog and extra spaces
            if catalog in cols:
                continue
            if IS_TEXT[catalog]:
                name = '\''+ ' '.join(data[len(catalog):].split()) + '\''
            else:
                name = ' '.join(data[len(catalog):].split())
            cols.append(DESIRED_CATALOGS[catalog])
            vals.append(name)
            # print(cat,name)

        # check if it's a common name
        elif catalog == 'NAME':
            # remove catalog and extra spaces
            name = '\''+ ' '.join(data[len(catalog):].split()) + '\''
            query = 'INSERT INTO commonNames (pgc,name) VALUES ('
            query += str(pgc) + ',' + name + ')'
            try:
                cursor.execute(query)
            except sqlite3.OperationalError:
                print(query)
                failed_queries.append(query)

    # only insert if there were desired catalog references
    if len(cols) > 1:
        query = 'INSERT INTO catalogs (' + ','.join(cols) + ') '
        query += 'VALUES (' + ','.join(vals) + ')'
        try:
            cursor.execute(query)
        except sqlite3.OperationalError:
            print(query)
            failed_queries.append(query)


if __name__ == '__main__':
    # PGCS = [47404, 4, 17223, 19441, 143, 47495, 37050]
    PGCS = db.fetch_pgcs_from_api()

    CON = db.connect()
    CUR = CON.cursor()

    for i, pgc in enumerate(PGCS):
        if i%250 == 0:
            print('done', i)
        names_and_catalogs(pgc, CUR)

    print('Committing changes.')
    CON.commit()
    CON.close()
    print('Done.')

    with open('../../../data/failed.json', 'w') as fail:
        FAILED = {
            'failed': failed_pgcs,
            'failed_queries': failed_queries
        }
        fail.write(json.dumps(FAILED))

    print('\n\n***********************')
    print('The following PGCs failed:')
    print(failed_pgcs)
    print(failed_queries)
