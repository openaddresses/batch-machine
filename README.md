<h1 align="center">OA Machine</h1>

Scripts for performing ETL on a single source, passing the end result to the [openaddresses/batch](https://github.com/openaddresses/batch) service
Uses [OpenAddresses](https://github.com/openaddresses/openaddresses) data sources to work.

## Status

This code is being used to process the complete OA dataset on a weekly and on-demand
basis, with output visible at [batch.openaddresses.io](https://batch.openaddresses.io).

These scripts are wrapped by the main [openaddresses/batch](https://github.com/openaddresses/batch) processor.

## Use

It is highly recommended to use this tool via the provided docker file - using an unsupported/untested version
of GDAL (the core geospatial library) will result in widely varying results.

Should you run this in a virtual env, install [Tippecanoe](https://github.com/mapbox/tippecanoe.git) and [GDAL](https://pypi.org/project/GDAL/)
before use.

### Docker

```
docker build -t batch-machine .
docker run -it batch-machine bash
```

Or to direct your generated output to a folder on the host machine, use [Docker Volumes](https://docs.docker.com/storage/volumes/).

Given an input file like this residing in the current directory inside Docker:

```example.json
{
    "schema": 2,
    "coverage": {
        "country": "ca",
        "state": "nb"
    },
    "layers": {
        "addresses": [{
            "name": "state",
            "data": "http://geonb.snb.ca/downloads/gcadb/geonb_gcadb-bdavg_shp.zip",
            "protocol": "http",
            "compression": "zip",
            "conform": {
                "number": "civic_num",
                "street": "street_nam",
                "format": "shapefile",
            }
        }]
    }
}
```

You can create an output folder and then run the batch process on the desired layer's key and its child object's `name` as arguments:

```
mkdir my-output
openaddr-process-one --skip-preview --layer addresses --layersource state example.json my-output
```

Review https://github.com/openaddresses/openaddresses/blob/master/CONTRIBUTING.md for input json syntax.

Supported conform formats include `shapefile`, `geojson`, `csv`, `xml`, `gdb`, and `gpkg`.
