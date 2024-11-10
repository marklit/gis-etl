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
    shapely \
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

## Downloading the Dataset

The dataset is made available as a 21 GB ZIP file which totals 81 GB and 2,878 files when decompressed.

https://zenodo.org/records/8174931

## Converting to Parquet

```bash
$ python east_asia.py main
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

## Extract EWKB Geometry

```bash
$ python east_asia.py ewkb
$ ~/duckdb
```

```sql
FROM READ_JSON('shape_stats.json');
```
