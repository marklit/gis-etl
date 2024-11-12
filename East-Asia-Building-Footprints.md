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

## Extract EWKB Geometry

This will get the shape type IDs used in each Shapefile. It'll identify the filenames of any Shapefile using EWKB. So far, POLYGON Z is the only shape type
not supported by DuckDB's GEOS version that has turned up in this dataset.

This function is only meant for debugging purposes.

```bash
$ python ~/gis-etl/east_asia.py ewkb-stats
$ ~/duckdb
```

```sql
FROM READ_JSON('shape_stats.json');
```

## Checking Results

```bash
$ find . -size 0 | grep -c pq$ # 0

$ find . | grep -c pq$ # 357
$ find . | grep -c shx$ # 358
```

WIP: ``Japan/Japan4.shx`` still needs to be processed.

WIP: First vector of first record from each file. Make sure it's lon-lat.

```bash
$ function first_vertex () {
      echo "SELECT SPLIT_PART(geom::TEXT, ',', 1) geom
            FROM   READ_PARQUET('$1')
            LIMIT  1;" \
        | ~/duckdb_111/duckdb -json | jq .[0].geom
  }

$ for FILENAME in `find . | grep pq$`; do
      echo $FILENAME, `first_vertex $FILENAME`
  done


$ first_vertex China/Fujian/Fuzhou.pq # POLYGON ((461923.69775799505 2848796.600784136
$ first_vertex China/Hainan/Hainan.pq #POLYGON ((614622.0454424241 2077452.740299601
$ first_vertex China/Jiangsu/Suzhou.pq # POLYGON ((439976.44070133485 3448981.960646557

```

## Merge PQs

```bash
$ ~/duckdb working.duckdb
```

```sql
COPY (
    SELECT   geom,
             filename AS source
    FROM     READ_PARQUET(['*/*.pq',
                           '*/*/*.pq'],
                          filename=True)
    ORDER BY HILBERT_ENCODE([
                ST_Y(ST_CENTROID(geom)),
                ST_X(ST_CENTROID(geom))]::DOUBLE[2])
) TO 'east_asian_buildings.pq' (
        FORMAT            'PARQUET',
        CODEC             'ZSTD',
        COMPRESSION_LEVEL 22,
        ROW_GROUP_SIZE    15000);
```
