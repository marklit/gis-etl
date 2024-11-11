#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=C0103 C0115 C0116 C0123 C0201 C0209 C0301 C0302 C3001
# pylint: disable=R0912 R0913 R0914 R0915 R0916 R1702 R1729 R1732 R1718
# pylint: disable=W0105 W0707 W0718 W1514

import json
from   multiprocessing import Pool
from   os.path         import exists, join as join_
from   pathlib         import Path
from   shlex           import quote
import tempfile

import duckdb
import pyproj
from   rich.progress   import track
from   shpyx           import run as execute
import typer


app = typer.Typer(rich_markup_mode='rich')


def get_epsg(filename):
    # This will be None for UTM-modified files like China/Fujian/Fuzhou.shx
    return pyproj.Proj(open(str(filename).split('.')[0] + '.prj').read())\
                 .crs\
                 .to_epsg()


def extract(manifest):
    filename, epsg_id = manifest

    if exists(filename.as_posix().replace('.shx', '.pq')):
        print('Already processed: %s' % filename.as_posix())
        return

    # If epsg_id is None, it is likely UTM-modified. Try using ogr2ogr to
    # convert the data into EPSG:4326 in a temp folder and process that data.
    # Leave the resulting .pq file along side the original .shx file.

    original_filename = str(filename.as_posix())
    working_filename = original_filename
    temp_dir = None

    if epsg_id is None:
        temp_dir = tempfile.TemporaryDirectory()

        cmd = 'ogr2ogr -f "ESRI Shapefile" -t_srs EPSG:4326 %(dest)s %(source)s' % {
            'source': quote(original_filename),
            'dest':   quote(join_(temp_dir.name,
                                  original_filename.split('/')[-1]))
        }

        print(cmd)

        try:
            execute(cmd)
        except Exception as exc:
            print(cmd)
            print(exc)
            return None

        working_filename = join_(temp_dir.name,
                                 original_filename.split('/')[-1])

    con = duckdb.connect(database=':memory:')

    for ext in ('spatial', 'parquet', 'lindel'):
        con.sql('LOAD %s' % ext)

    # Find the name of the geom column
    sql = 'DESCRIBE FROM ST_READ(?, keep_wkb=TRUE) LIMIT 1'

    wkb_cols = [x['column_name']
                for x in list(con.sql(sql,
                                      params=(working_filename,))
                                 .to_df()
                                 .iloc())
                if x['column_type'] in ('WKB_BLOB', 'GEOMETRY')]

    if not wkb_cols:
        print('No geom field name found in %s' % original_filename)
        return None

    # Do lat and long need to be flipped?

    # WIP: The statement below is very slow and only uses 2 CPU cores. See
    # if you can find a faster way to identify flipped lat-lons.

    # WIP: Why is the spatial extension complaining about the %(geom)s field?
    # Is it renaming it geom magically or something? keep_wkb will default
    # to False, is the Spatial extension offering "geom" as a default column
    # name to call on or something?
    sql = '''SELECT MIN(ST_XMIN(ST_TRANSFORM(geom,
                                             'EPSG:%(epsg)d',
                                             'EPSG:4326'))) as min_x
             FROM ST_READ(?)''' % {
                'geom': wkb_cols[0],
                'epsg': epsg_id}


    try:
        min_x = [x['min_x']
                    for x in list(con.sql(sql,
                                          params=(working_filename,))
                                     .to_df()
                                     .iloc())][0]
    except Exception as exc:
        print(exc)
        print(filename, epsg_id)
        print(sql)
        return None

    '''
    Examples of files that need lat & lon flipped.
    Their min_x values are well over 60.

    China/Guangdong/Shenzhen.shx
    China/Beijing/Beijing.shx
    China/Hongkong/Hongkong.shx
    South Korea/South_Korea_build_final.shx

    Northern cities where they don't need to be flipped. These should help
    decide the cut-off point.

    China/Heilongjiang/Daxinganling.shx
    China/Gansu/Jiayuguan.shx
    '''

    flip_lat_lon = min_x < 60

    # Convert to Parquet
    sql = """COPY (
               WITH a AS (
                 SELECT ST_TRANSFORM(%(geom)s::GEOMETRY,
                                    'EPSG:%(epsg)d',
                                    'EPSG:4326') geom
                 FROM   ST_READ(?, keep_wkb=TRUE)
                 WHERE  ('0x' || substr(%(geom)s::BLOB::TEXT, 7, 2))::INT < 8
               )
               SELECT   %(geom_flip)s AS geom
               FROM     a
               ORDER BY HILBERT_ENCODE([
                             ST_Y(ST_CENTROID(geom)),
                             ST_X(ST_CENTROID(geom))]::DOUBLE[2])
             ) TO '%(out)s' (
                    FORMAT            'PARQUET',
                    CODEC             'ZSTD',
                    COMPRESSION_LEVEL 22,
                    ROW_GROUP_SIZE    15000);""" % {
        'geom':      wkb_cols[0],
        'geom_flip': 'ST_FlipCoordinates(geom)' if flip_lat_lon else 'geom',
        'epsg':      epsg_id,
        'out':       original_filename.replace('.shx', '.pq')}

    try:
        con.sql(sql,
                params=(working_filename,))
    except Exception as exc:
        print(filename)
        print(exc)

    if epsg_id is None:
        cmd = 'rm -fr %s' % quote(temp_dir.name)

        try:
            execute(cmd)
        except Exception as exc:
            print(cmd)
            print(exc)
            return None

    print('Finished: %s' % original_filename)


@app.command()
def main(pool_size:int = typer.Option(8)):
    # Make sure the extensions are installed in embedded version of DuckDB
    con = duckdb.connect(database=':memory:')

    for ext in ('spatial', 'parquet'):
        con.sql('INSTALL %s' % ext)

    con.sql('INSTALL lindel FROM community')

    workload = [(filename, get_epsg(filename))
                for filename in Path('.').glob('**/*.shx')]

    pool = Pool(pool_size)
    pool.map(extract, [(filename, epsg_num)
                       for filename, epsg_num in workload])


def get_ewkb_geometry(filename):
    con = duckdb.connect(database=':memory:')
    con.sql('LOAD spatial')

    # Find the name of the geom column
    sql = 'DESCRIBE FROM ST_READ(?, keep_wkb=TRUE) LIMIT 1'
    wkb_cols = [x['column_name']
                for x in list(con.sql(sql,
                                      params=(filename.as_posix(),))
                                 .to_df()
                                 .iloc())
                if x['column_type'] in ('WKB_BLOB', 'GEOMETRY')]

    # WIP: Raise exception if you need this function to be more robust.
    if not wkb_cols:
        print('No geom field name found in %s' % filename.as_posix())
        return []

    # Get the number of records for each shape type.
    sql = '''SELECT   ('0x' || substr(%(geom)s::BLOB::TEXT, 7, 2))::INT
                            AS shape_type,
                      COUNT(*) cnt
             FROM     ST_READ(?, keep_wkb=TRUE)
             GROUP BY 1''' % {
             'geom': wkb_cols[0],
          }

    # WIP: Cast to floats instead
    # cannot convert float NaN to integer
    try:
        return [(float(x['shape_type']),
                 float(x['cnt']),
                 str(filename.as_posix()))
                for x in list(con.sql(sql,
                                      params=(filename.as_posix(),))
                                 .to_df()
                                 .iloc())]
    except Exception as exc:
        print('Failed: %s' % filename.as_posix())
        print(exc)

        # WIP: Raise exception if you need this function to be more robust.
        return []


@app.command()
def ewkb_stats(pool_size:int = typer.Option(8)):
    # Make sure the extensions are installed in embedded version of DuckDB
    con = duckdb.connect(database=':memory:')
    con.sql('INSTALL spatial')

    with open('shape_stats.json', 'w') as f:
       for filename in track(list(Path('.').glob('**/*.shx'))):
            for rec in get_ewkb_geometry(filename):
                shape_type, num_recs, filename = rec

                f.write(json.dumps({
                            'shape_type': int(shape_type),
                            'num_recs':   int(num_recs),
                            'filename':   filename}) + '\n')


if __name__ == "__main__":
    app()
