# AI-Extracted, East Asian Building Footprints

The code below originated from this blog post: https://tech.marksblogg.com/asian-building-footprints-from-google-maps.html

The original paper for this dataset: https://spj.science.org/doi/10.34133/remotesensing.0138

## Prerequisites

```bash
$ sudo apt update
$ sudo apt install \
    python3-pip \
    python3-virtualenv

$ python3 -m venv ~/.clsm
$ source ~/.clsm/bin/activate

$ pip install \
    'duckdb==1.1.3' \
    pandas \
    pyproj \
    shapely
```

```bash
$ cd ~
$ wget -c https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip
$ unzip -j duckdb_cli-linux-amd64.zip
$ chmod +x duckdb
$ ~/duckdb
```

```sql
INSTALL lindel FROM community;
INSTALL parquet;
INSTALL spatial;
```

```bash
$ vi ~/.duckdbrc
```

```sql
.timer on
.width 180

LOAD lindel;
LOAD parquet;
LOAD spatial;
```

## Downloading the Dataset

The dataset is made available as a 21 GB ZIP file which totals 81 GB and 2,878 files when decompressed.

https://zenodo.org/records/8174931

## Converting to Parquet

WIP: Produce Parquet files of the building footprints instead of bounding boxes.

```python
from   multiprocessing import Pool
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
    return pyproj.Proj(open(str(filename).split('.')[0] + '.prj').read())\
                    .crs\
                    .to_epsg()


workload = [(filename, get_epsg(filename))
            for filename in Path('.').glob('**/*.shx')]


def extract(manifest):
    filename, epsg_id = manifest

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

    # WIP: Why is the spatial extension complaining about the %(geom)s field?
    # Is it renaming it geom magically or something?
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
    Examples of files that need to be flipped:

    (PosixPath('China/Guangdong/Shenzhen.shx'), 'wkb_geometry', np.float64(113.76798131399326))
    (PosixPath('China/Beijing/Beijing.shx'), 'wkb_geometry', np.float64(115.43053110645005))

    Northern cities where they don't need to be flipped. These should help
    decide the cut-off point.

    (PosixPath('China/Heilongjiang/Daxinganling.shx'), 'wkb_geometry', np.float64(50.333342042038495))
    (PosixPath('China/Gansu/Jiayuguan.shx'), 'wkb_geometry', np.float64(39.65811285004477))
    '''

    flip_lat_lon = min_x > 60

    # Convert to Parquet
    sql = """COPY (
                 WITH a AS (
                     SELECT ST_TRANSFORM(%(geom)s,
                                        'EPSG:%(epsg)d',
                                        'EPSG:4326') geom
                     FROM   ST_READ(?, keep_wkb=TRUE)
                     WHERE ('0x' || substr(%(geom)s::BLOB::TEXT, 7, 2))::int < 8
                  )
                  SELECT %(geom_flip) AS gem
                  FROM   a
                  ORDER BY HILBERT_ENCODE([
                                ST_Y(ST_CENTROID(geom)),
                                ST_X(ST_CENTROID(geom))]::double[2])
             ) TO '%(out)s.pq' (
                    FORMAT            'PARQUET',
                    CODEC             'ZSTD',
                    COMPRESSION_LEVEL 22,
                    ROW_GROUP_SIZE    15000);""" % {
        'geom': wkb_cols[0],
        'geom_flip': 'geom' if not flip_lat_lon
                            else 'ST_FlipCoordinates(geom)'
        'epsg': epsg_id,
        'out': filename.as_posix().replace('.shx', '.pq')}

    try:
        df = con.sql(sql,
                     params=(filename.as_posix(),)).to_df()
    except Exception as exc:
        print(filename)
        print(exc)


pool = Pool(20)

# WIP: Find any files that you couldn't get the projection for
resp = pool.map(extract, [(filename, epsg_num)
                          for filename, epsg_num in workload
                          if epsg_num])
```

WIP: Open every PQ file with filename=True and merge into a single PQ file.