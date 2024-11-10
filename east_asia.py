from   multiprocessing import Pool
from   os.path         import exists
from   pathlib         import Path

import duckdb
import pyproj


# Make sure the extensions are installed in embedded version of DuckDB
con = duckdb.connect(database=':memory:')

for ext in ('spatial', 'parquet'):
    con.sql('INSTALL %s' % ext)

con.sql('INSTALL lindel FROM community')


# Get the list of projections
def get_epsg(filename):
    epsg_id = pyproj.Proj(open(str(filename).split('.')[0] + '.prj').read())\
                    .crs\
                    .to_epsg()

    # WIP: Make sure all four problematic locations look like 4326
    if not epsg_id:
        return 4326

    return epsg_id


workload = [(filename, get_epsg(filename))
            for filename in Path('.').glob('**/*.shx')]


def extract(manifest):
    filename, epsg_id = manifest

    if exists(filename.as_posix().replace('.shx', '.pq')):
        return

    con = duckdb.connect(database=':memory:')

    for ext in ('spatial', 'parquet', 'lindel'):
        con.sql('LOAD %s' % ext)

    # Find the name of the geom column
    sql = 'DESCRIBE FROM ST_READ(?, keep_wkb=TRUE) LIMIT 1'

    wkb_cols = [x['column_name']
                for x in list(con.sql(sql,
                                      params=(filename.as_posix(),))
                                 .to_df()
                                 .iloc())
                if x['column_type'] in ('WKB_BLOB', 'GEOMETRY')]

    if not wkb_cols:
        print('No geom field name found in %s' % filename.as_posix())
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
                                          params=(filename.as_posix(),))
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
        'out':       filename.as_posix().replace('.shx', '.pq')}

    try:
        con.sql(sql,
                params=(filename.as_posix(),))
    except Exception as exc:
        print(filename)
        print(exc)

    print('Finished: %s' % filename)


pool = Pool(8)
pool.map(extract, [(filename, epsg_num)
                   for filename, epsg_num in workload])
