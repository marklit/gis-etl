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
$ first_vertex China/Macau/Macau.pq # Only 309 bytes?

$ hexdump -C China/Macau/Macau.pq
```

```
00000000  50 41 52 31 15 02 19 2c  35 00 18 0d 64 75 63 6b  |PAR1...,5...duck|
00000010  64 62 5f 73 63 68 65 6d  61 15 02 00 15 0c 25 02  |db_schema.....%.|
00000020  18 04 67 65 6f 6d 00 16  00 19 0c 19 1c 18 03 67  |..geom.........g|
00000030  65 6f 18 cc 01 7b 22 76  65 72 73 69 6f 6e 22 3a  |eo...{"version":|
00000040  22 31 2e 31 2e 30 22 2c  22 70 72 69 6d 61 72 79  |"1.1.0","primary|
00000050  5f 63 6f 6c 75 6d 6e 22  3a 22 67 65 6f 6d 22 2c  |_column":"geom",|
00000060  22 63 6f 6c 75 6d 6e 73  22 3a 7b 22 67 65 6f 6d  |"columns":{"geom|
00000070  22 3a 7b 22 65 6e 63 6f  64 69 6e 67 22 3a 22 57  |":{"encoding":"W|
00000080  4b 42 22 2c 22 67 65 6f  6d 65 74 72 79 5f 74 79  |KB","geometry_ty|
00000090  70 65 73 22 3a 5b 5d 2c  22 62 62 6f 78 22 3a 5b  |pes":[],"bbox":[|
000000a0  31 2e 37 39 37 36 39 33  31 33 34 38 36 32 33 31  |1.79769313486231|
000000b0  35 37 65 33 30 38 2c 31  2e 37 39 37 36 39 33 31  |57e308,1.7976931|
000000c0  33 34 38 36 32 33 31 35  37 65 33 30 38 2c 2d 31  |348623157e308,-1|
000000d0  2e 37 39 37 36 39 33 31  33 34 38 36 32 33 31 35  |.797693134862315|
000000e0  37 65 33 30 38 2c 2d 31  2e 37 39 37 36 39 33 31  |7e308,-1.7976931|
000000f0  33 34 38 36 32 33 31 35  37 65 33 30 38 5d 7d 7d  |348623157e308]}}|
00000100  7d 00 18 28 44 75 63 6b  44 42 20 76 65 72 73 69  |}..(DuckDB versi|
00000110  6f 6e 20 76 31 2e 31 2e  33 20 28 62 75 69 6c 64  |on v1.1.3 (build|
00000120  20 31 39 38 36 34 34 35  33 66 37 29 00 29 01 00  | 19864453f7).)..|
00000130  00 50 41 52 31                                    |.PAR1|
00000135
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
$ python east_asia.py ewkb-shape-stats
$ ~/duckdb
```

```sql
FROM READ_JSON('shape_stats.json');
```
