#!/bin/sh

spark-submit --class climate.ingest.ParquetIngest --driver-library-path /usr/local/lib /tmp/benchmark.jar --crs EPSG:4326 --input /home/cbrown/Documents/tiles/*.tif --output /home/cbrown/Documents/tif-output/ --layerName test
