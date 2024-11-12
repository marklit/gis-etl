#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=C0103 C0114 C0115 C0116 C0123 C0201 C0207 C0209 C0301 C0302 C3001
# pylint: disable=R0912 R0913 R0914 R0915 R0916 R1702 R1729 R1732 R1718
# pylint: disable=W0105 W0707 W0718 W1514

import json
from   multiprocessing import Pool
from   os              import unlink
from   os.path         import exists, getsize, join as join_
from   pathlib         import Path
from   shlex           import quote
import tempfile

import duckdb
import geopandas as gpd
import pyproj
from   rich.progress    import track
from   shapely.geometry import shape
from   shapely          import wkt
from   shpyx            import run as execute
import typer


app = typer.Typer(rich_markup_mode='rich')


def get_epsg(filename):
    # This will be None for UTM-modified files like China/Fujian/Fuzhou.shx
    return pyproj.Proj(open(str(filename).split('.')[0] + '.prj').read())\
                 .crs\
                 .to_epsg()


# WIP: POLYGON Z, which is geometry shape type 235(?), is actually supported
# by DuckDB. This method shouldn't be needed. Just use
# geom::POLYGON_2D::GEOMETRY to remove the Z field.
def ewkb_to_pq(filename:str):
    assert filename.endswith('.shx')

    # Make sure the extensions are installed in embedded version of DuckDB
    con = duckdb.connect(database=':memory:')
    con.sql('INSTALL spatial;               LOAD spatial')
    con.sql('INSTALL lindel FROM community; LOAD lindel')

    # These were all POLYGON Z records. None of them that I looked at had any
    # height information.
    df = gpd.read_file(filename)
    df = df.to_crs(4326)

    temp_ = tempfile.NamedTemporaryFile(
                suffix='.csv',
                delete=False)

    with open(temp_.name, 'w') as f:
        f.write('"geom",\n')
        for feature in track(df.iloc(),
                             total=df.shape[0],
                             description=filename):
            geom = wkt.loads(wkt.dumps(shape(feature['geometry']),
                             output_dimension=2))
            f.write('"%s",\n' % geom.wkt)

    # Convert to Parquet
    sql = """COPY (
               SELECT   geom::GEOMETRY as geom
               FROM     READ_CSV(?, header=True)
               ORDER BY HILBERT_ENCODE([
                             ST_Y(ST_CENTROID(geom::GEOMETRY)),
                             ST_X(ST_CENTROID(geom::GEOMETRY))]::DOUBLE[2])
             ) TO '%(out)s' (
                    FORMAT            'PARQUET',
                    CODEC             'ZSTD',
                    COMPRESSION_LEVEL 22,
                    ROW_GROUP_SIZE    15000);""" % {
                'out': filename.replace('.shx', '.pq')}

    try:
        con.sql(sql,
                params=(temp_.name,))
    except Exception as exc:
        print(filename)
        print(exc)

    unlink(temp_.name)
    print('Finished EWKB to PQ: %s' % filename.replace('.shx', '.pq'))

    return None


def extract(manifest):
    filename, epsg_id, run_via_python = manifest

    target_pq = filename.as_posix().replace('.shx', '.pq')

    if exists(target_pq):
        if getsize(target_pq):
            print('Already processed: %s' % target_pq)
            return None

        unlink(target_pq) # Remove the empty PQ file and try to build again

    # If epsg_id is None, it is likely UTM-modified. Convert using ogr2ogr
    # into EPSG:4326 in a temp folder and process that data instead.
    # Leave the resulting .pq file along side the original .shx file.
    original_filename = str(filename.as_posix())
    working_filename  = original_filename
    temp_dir          = None

    if epsg_id is None:
        temp_dir = tempfile.TemporaryDirectory()

        cmd = 'ogr2ogr -f "ESRI Shapefile" ' \
              '-t_srs EPSG:4326 %(dest)s %(source)s' % {
                'source': quote(original_filename),
                'dest':   quote(join_(temp_dir.name,
                                      original_filename.split('/')[-1]))}

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

    if run_via_python:
        return ewkb_to_pq(working_filename)

    # If any geometry is outside of the 7 shape types GEOS supports,
    # then process with geopandas and shapely. None of these files have
    # flipped lat-lons.

    # WIP: POLYGON Z, which is geometry shape type 235(?), is actually supported
    # by DuckDB. The Z field can be dropped with the following:
    #
    # SELECT geom::POLYGON_2D::GEOMETRY FROM ST_READ('China/Macau/Macau.shp')
    #
    # The less than 8 test below should be loosened to allow for POLYGON Z
    # through.
    sql = '''SELECT COUNT(*) cnt
             FROM   ST_READ(?, keep_wkb=TRUE)
             WHERE  ('0x' || substr(%(geom)s::BLOB::TEXT, 7, 2))::INT > 7''' % {
             'geom':      wkb_cols[0],
          }

    if int(con.sql(sql, params=(working_filename,)).to_df().iloc()[0]['cnt']):
        return ewkb_to_pq(working_filename)

        if epsg_id is None:
            cmd = 'rm -fr %s' % quote(temp_dir.name)

            try:
                execute(cmd)
            except Exception as exc:
                print(cmd)
                print(exc)
                return None

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
    return None


@app.command()
def main(pool_size:int = typer.Option(8),
         run_via_python:bool = typer.Option(True)):
    # Make sure the extensions are installed in embedded version of DuckDB
    con = duckdb.connect(database=':memory:')

    for ext in ('spatial', 'parquet'):
        con.sql('INSTALL %s' % ext)

    con.sql('INSTALL lindel FROM community')

    # Run one at a time as these files had issues when running via a pool
    if run_via_python:
        for filename in Path('.').glob('**/*.shx'):
            extract((filename, get_epsg(filename), run_via_python))
    else:
        workload = [(filename,
                     get_epsg(filename),
                     run_via_python)
                    for filename in Path('.').glob('**/*.shx')]

        pool = Pool(pool_size)
        pool.map(extract, workload)


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
             GROUP BY 1''' % {'geom': wkb_cols[0]}

    try:
        return [(float(x['shape_type']) if x['shape_type'] else 0,
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
def ewkb_stats():
    # Make sure the extensions are installed in embedded version of DuckDB
    con = duckdb.connect(database=':memory:')
    con.sql('INSTALL spatial')

    with open('shape_stats.json', 'w') as f:
        for filename in track(list(Path('.').glob('**/*.shx'))):
            if str(filename.as_posix()) in skip:
                continue

            for rec in get_ewkb_geometry(filename):
                shape_type, num_recs, filename_ = rec

                f.write(json.dumps({
                            'shape_type': int(shape_type),
                            'num_recs':   int(num_recs),
                            'filename':   filename_}) + '\n')


if __name__ == "__main__":
    app()
