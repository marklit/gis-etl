# AI-Extracted, East Asian Building Footprints

The code below originated from this blog post: https://tech.marksblogg.com/asian-building-footprints-from-google-maps.html

The original paper for this dataset: https://spj.science.org/doi/10.34133/remotesensing.0138

## Prerequisites

```bash
$ sudo apt update
$ sudo apt install \
    gdal-bin \
    python3-pip \
    python3-virtualenv

$ python3 -m venv ~/.clsm
$ source ~/.clsm/bin/activate

$ pip install \
    'duckdb==1.1.3' \
    geopandas \
    pandas \
    pylint \
    pyproj \
    shapely \
    shpyx \
    typer
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

## Clone the git Repo

```bash
$ git clone https://github.com/marklit/gis-etl ~/gis-etl
$ pylint ~/gis-etl/east_asia.py
```

## Downloading the Dataset

The dataset is made available as a 21 GB ZIP file which totals 81 GB and 2,878 files when decompressed.

https://zenodo.org/records/8174931

Extract the above ZIP file to ``~/CLSM``

```bash
$ tree ~/CLSM/ -d -L 1
```

```
~/CLSM/
├── China
├── Japan
├── Mongolia
├── North Korea
└── South Korea
```

## Converting to Parquet

```bash
$ cd ~/CLSM
$ python ~/gis-etl/east_asia.py main
```

If you run into memory pressures or stability issues, consider running the single-threaded version that relies more on Python libraries than DuckDB.

The following can be run after the above and will only work on files that haven't been converted into Parquet successfully. It also includes a per-record progress bar with an estimated time remaining.

```bash
$ python ~/gis-etl/east_asia.py main --run-via-python
```

## Extract EWKB Geometry

This function is only meant for debugging purposes.

This will get the shape type IDs used in each Shapefile. It'll identify the filenames of any Shapefile using EWKB. So far, POLYGON Z is the only shape type
not supported by DuckDB's GEOS version that has turned up in this dataset.

```bash
$ python ~/gis-etl/east_asia.py ewkb-stats
$ ~/duckdb
```

```sql
FROM READ_JSON('shape_stats.json');
```

## Checking Resulting Parquet Files

### Any Empty Parquet files?

```bash
$ find . | grep -c shx$ # 358
$ find . | grep -c pq$  # 358
$ find . -size 0 | grep -c pq$ # 0
```

```python
from pathlib import Path


files = {}

for ext in ('pq', 'shx'):
    files[ext] = [x.as_posix().split('.')[0]
                  for x in Path('.').glob('**/*.%s' % ext)]

set(files['shx']) - set(files['pq'])
```

```python
set()
```

### Check lat-lons

First vector of first record from each file. Make sure it's lon-lat.

```bash
# Make sure the wildcard statement does return every PQ file
$ find . | grep pq$  | wc -l # 358
$ ls */*.pq */*/*.pq | wc -l # 358

$ function first_vertex () {
      echo "SELECT SPLIT_PART(geom::TEXT, ',', 1) geom
            FROM   READ_PARQUET('$1')
            LIMIT  1;" \
        | ~/duckdb -json | jq .[0].geom
  }

$ for FILENAME in */*.pq */*/*.pq; do
      echo `first_vertex "$FILENAME"`, $FILENAME
  done
```

### Check Record Counts

```bash
$ ~/duckdb working.duckdb
```

```sql
SELECT COUNT(DISTINCT filename)
FROM   READ_PARQUET(['*/*.pq',
                     '*/*/*.pq'],
                    filename=True); -- 358
```

Make sure there are plausible number of records for each file.

```sql
.maxrows 20

SELECT   COUNT(*),
         filename
FROM     READ_PARQUET(['*/*.pq',
                       '*/*/*.pq'],
                      filename=True)
GROUP BY 2
ORDER BY 1;
```

```
┌──────────────┬────────────────────────────────────────┐
│ count_star() │                filename                │
│    int64     │                varchar                 │
├──────────────┼────────────────────────────────────────┤
│         6702 │ China/Tibet/Ngari.pq                   │
│         9081 │ China/Hubei/Shennongjia.pq             │
│         9912 │ China/Xinjiang/Kunyu.pq                │
│        10304 │ China/Xinjiang/Shuanghe.pq             │
│        10400 │ China/Qinghai/Golog.pq                 │
│        10706 │ China/Macau/Macau.pq                   │
│        10765 │ China/Xinjiang/Beitun.pq               │
│        11558 │ China/Xinjiang/Tiemenguan.pq           │
│        12154 │ China/Tibet/Qamdo.pq                   │
│        13257 │ China/Tibet/Nagqu.pq                   │
│          ·   │          ·                             │
│          ·   │          ·                             │
│          ·   │          ·                             │
│      2925195 │ China/Hebei/Baoding.pq                 │
│      2938536 │ China/Beijing/Beijing.pq               │
│      2953535 │ China/Heilongjiang/Harbin.pq           │
│      3177243 │ China/Shandong/Linyi.pq                │
│      3337219 │ Japan/Japan1.pq                        │
│      3622496 │ China/Shandong/Weifang.pq              │
│      3793564 │ South Korea/South_Korea_build_final.pq │
│      5501408 │ China/Chongqing/Chongqing.pq           │
│      8597500 │ Japan/Japan2.pq                        │
│     12519164 │ Japan/Japan4.pq                        │
├──────────────┴────────────────────────────────────────┤
│ 358 rows (20 shown)                         2 columns │
└───────────────────────────────────────────────────────┘
```

```sql
SELECT COUNT(*)
FROM   READ_PARQUET(['*/*.pq',
                     '*/*/*.pq']); -- 281093422

.mode line

SELECT MIN(ST_XMIN(ST_GEOMFROMWKB(geom))),
       MAX(ST_XMAX(ST_GEOMFROMWKB(geom))),
       MIN(ST_YMIN(ST_GEOMFROMWKB(geom))),
       MAX(ST_YMAX(ST_GEOMFROMWKB(geom)))
FROM   READ_PARQUET(['*/*.pq',
                     '*/*/*.pq']);
-- Segmentation fault (core dumped)
```

### Heatmap

```bash
$ echo "CREATE OR REPLACE TABLE h3_heatmap (
            h3_7 UINT64,
            num_recs BIGINT)" \
    | ~/duckdb heatmap.duckdb
$ for FILENAME in */*.pq */*/*.pq; do
      echo $FILENAME
      echo "INSERT INTO h3_heatmap
                SELECT   H3_LATLNG_TO_CELL(ST_Y(ST_CENTROID(geom)),
                                           ST_X(ST_CENTROID(geom)),
                                           7) AS h3_7,
                         COUNT(*) AS num_recs
                FROM     READ_PARQUET('$FILENAME')
                GROUP BY 1" \
        | ~/duckdb heatmap.duckdb
  done

$ ~/duckdb heatmap.duckdb
```

```sql
COPY (
    SELECT   H3_CELL_TO_BOUNDARY_WKT(h3_7)::geometry geom,
             SUM(num_recs)::INT32 AS num_recs
    FROM     h3_heatmap
    WHERE    ST_X(ST_CENTROID(H3_CELL_TO_BOUNDARY_WKT(h3_7)::geometry)) < 175
    AND      ST_X(ST_CENTROID(H3_CELL_TO_BOUNDARY_WKT(h3_7)::geometry)) > -175
    GROUP BY 1
) TO 'h3_heatmap.h3_7.gpkg'
    WITH (FORMAT GDAL,
          DRIVER 'GPKG',
          LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES');
```

## Merge into fewer PQ files

```bash
$ echo "SELECT   COUNT(*) cnt,
                 filename
        FROM     READ_PARQUET(['*/*.pq',
                               '*/*/*.pq'],
                              filename=True)
        GROUP BY 2
        ORDER BY 1;" \
    | ~/duckdb -json \
    > resp.json

$ python ~/gis-etl/east_asia.py merge-pqs

$ du -hc east_asian_buildings_* | tail -n1 # 14 GB
$ ls -lh east_asian_buildings_*
```

```
529M .. east_asian_buildings_00.pq
515M .. east_asian_buildings_01.pq
493M .. east_asian_buildings_02.pq
517M .. east_asian_buildings_03.pq
525M .. east_asian_buildings_04.pq
558M .. east_asian_buildings_05.pq
522M .. east_asian_buildings_06.pq
515M .. east_asian_buildings_07.pq
515M .. east_asian_buildings_08.pq
528M .. east_asian_buildings_09.pq
487M .. east_asian_buildings_10.pq
486M .. east_asian_buildings_11.pq
542M .. east_asian_buildings_12.pq
512M .. east_asian_buildings_13.pq
493M .. east_asian_buildings_14.pq
532M .. east_asian_buildings_15.pq
491M .. east_asian_buildings_16.pq
552M .. east_asian_buildings_17.pq
522M .. east_asian_buildings_18.pq
552M .. east_asian_buildings_19.pq
593M .. east_asian_buildings_20.pq
507M .. east_asian_buildings_21.pq
610M .. east_asian_buildings_22.pq
529M .. east_asian_buildings_23.pq
657M .. east_asian_buildings_24.pq
581M .. east_asian_buildings_25.pq
```