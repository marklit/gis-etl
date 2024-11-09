# ORNL & FEMA's 131M American Buildings

The code below originated from this blog post: https://tech.marksblogg.com/ornl-fema-buildings.html

The original paper for this dataset:  https://www.nature.com/articles/s41597-024-03219-x

## Prerequisites

```bash
$ sudo apt update
$ sudo apt install \
    python3-pip \
    python3-virtualenv

$ python3 -m venv ~/.ornl
$ source ~/.ornl/bin/activate

$ pip install \
    xmljson
```

```bash
$ cd ~
$ wget -c https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip
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

The following will download 56 ZIP files with a footprint of 42 GB.

```bash
$ mkdir -p ~/ornl_buildings
$ cd ~/ornl_buildings

$ curl -s https://disasters.geoplatform.gov/USA_Structures/ \
    | grep -o 'https.*zip' \
    > urls.txt
$ cat urls.txt | xargs -P4 -I% wget -c %
```

## Converting to Parquet

```bash
$ for FILENAME in `ls Deliverable*.zip`; do
      STATE=`echo $FILENAME | grep -oE '[A-Z]{2}\.zip' | sed 's/.zip//g'`

      if ! test -f "$STATE.pq"; then
          echo $STATE

          mkdir -p working
          rm working/* || true
          unzip -qnj $FILENAME -d working

          GDB=`ls -S working/*.gdbtable | head -n1`

          echo "COPY(
                    SELECT   * EXCLUDE(Shape),
                             Shape geom
                    FROM     ST_READ('$GDB', keep_wkb=TRUE)
                    WHERE    ('0x' || substr(Shape::BLOB::TEXT, 7, 2))::int < 8
                    AND      ST_Y(ST_CENTROID(Shape::GEOMETRY)) IS NOT NULL
                    AND      ST_X(ST_CENTROID(Shape::GEOMETRY)) IS NOT NULL
                    ORDER BY HILBERT_ENCODE([
                                  ST_Y(ST_CENTROID(Shape::GEOMETRY)),
                                  ST_X(ST_CENTROID(Shape::GEOMETRY))]::double[2])
                ) TO '$STATE.pq' (
                    FORMAT            'PARQUET',
                    CODEC             'ZSTD',
                    COMPRESSION_LEVEL 22,
                    ROW_GROUP_SIZE    15000);" \
              | ~/duckdb_111/duckdb
      fi
  done
```
