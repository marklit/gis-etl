# AI-Extracted, East Asian Building Footprints

The code below originated from this blog post: https://tech.marksblogg.com/asian-building-footprints-from-google-maps.html

The original paper for this dataset: https://spj.science.org/doi/10.34133/remotesensing.0138

## Prerequisites

```bash
$ sudo apt update
$ sudo apt install \
    python3-pip \
    python3-virtualenv

$ virtualenv ~/.clsm
$ source ~/.clsm/bin/activate

$ pip install \
    duckdb \
    pyproj \
    shapely
```

```bash
$ cd ~
$ wget -c https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip
$ unzip -j duckdb_cli-linux-amd64.zip
$ chmod +x duckdb
$ ~/duckdb
```

```sql
INSTALL h3 FROM community;
INSTALL lindel FROM community;
INSTALL json;
INSTALL parquet;
INSTALL spatial;
```

```bash
$ vi ~/.duckdbrc
```

```sql
.timer on
.width 180
LOAD h3;
LOAD lindel;
LOAD json;
LOAD parquet;
LOAD spatial;
```

## Downloading the Dataset

The dataset is made available as a 21 GB ZIP file which totals 81 GB and 2,878 files when decompressed.

https://zenodo.org/records/8174931

## Converting to Parquet

WIP: Produce Parquet files of the building footprints instead of bounding boxes.

```python
from   collections     import Counter
import json
from   multiprocessing import Pool
from   pathlib         import Path

import duckdb
import pyproj


def get_epsg(filename):
    return pyproj.Proj(open(str(filename).split('.')[0] + '.prj').read())\
                    .crs\
                    .to_epsg()


workload = [(filename, get_epsg(filename))
            for filename in Path('.').glob('**/*.shx')]


def extract(manifest):
    filename, epsg_id = manifest

    con = duckdb.connect(database=':memory:')
    con.sql('INSTALL spatial; LOAD spatial')

    # Find the name of the geom column
    sql = 'DESCRIBE FROM ST_READ(?, keep_wkb=TRUE) LIMIT 1'

    wkb_cols = [x['column_name']
                for x in list(con.sql(sql,
                                      params=(filename.as_posix(),)).to_df().iloc())
                if x['column_type'] == 'WKB_BLOB']

    if not wkb_cols:
        return None

    # Create a bounding box of the geometry.
    # Exclude geometry that GEOS doesn't support.
    sql = """SELECT ST_AsText({min_x: MIN(ST_XMIN(ST_TRANSFORM(%(geom)s::GEOMETRY, 'EPSG:%(epsg)d', 'EPSG:4326'))),
                               min_y: MIN(ST_YMIN(ST_TRANSFORM(%(geom)s::GEOMETRY, 'EPSG:%(epsg)d', 'EPSG:4326'))),
                               max_x: MAX(ST_XMAX(ST_TRANSFORM(%(geom)s::GEOMETRY, 'EPSG:%(epsg)d', 'EPSG:4326'))),
                               max_y: MAX(ST_YMAX(ST_TRANSFORM(%(geom)s::GEOMETRY, 'EPSG:%(epsg)d', 'EPSG:4326')))}::BOX_2D::GEOMETRY) AS bbox
             FROM   ST_READ(?, keep_wkb=TRUE)
             WHERE ('0x' || substr(%(geom)s::BLOB::TEXT, 7, 2))::int < 8""" % {
        'geom': wkb_cols[0],
        'epsg': epsg_id}

    try:
        df = con.sql(sql,
                     params=(filename.as_posix(),)).to_df()
    except Exception as exc:
        print(filename)
        print(exc)
        return None

    if not df.empty:
        return filename.name\
                       .split('.')[0]\
                       .replace('_build_final', '')\
                       .upper(), \
               epsg_id, \
               df.iloc()[0]['bbox']

    return None


pool = Pool(20)
resp = pool.map(extract, [(filename, epsg_num)
                          for filename, epsg_num in workload
                          if epsg_num])

resp = [x
        for x in resp
        if x is not None]

with open('bboxes.csv', 'w') as f:
    f.write('filename, epsg, geom\n')

    for filename, epsg, geom in resp:
        f.write('"%s",%d,"%s"\n' % (filename, epsg, geom))
```

The bounding boxes for 348 cities, regions and countries were detected and saved into bboxes.csv. Around ten records ordered their coordinates by longitude and latitude while the remaining did the inverse. I ran the following Python code to normalise them all to longitude then latitude.

```python
import csv

from shapely     import wkt
from shapely.ops import transform


with open('bboxes.csv') as csv_file:
    reader = csv.DictReader(csv_file, skipinitialspace=True)

    with open('bboxes2.csv', 'w') as out:
        writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if ',' in row['geom']:
                poly = wkt.loads(row['geom'])

                if  poly.exterior.coords.xy[0][0] < \
                    poly.exterior.coords.xy[1][0]:
                    row['geom'] = transform(lambda x, y: (y, x), poly).wkt

                writer.writerow({key: value
                                 for key, value in row.items()})
```

