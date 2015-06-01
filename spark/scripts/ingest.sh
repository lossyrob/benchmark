#!/bin/sh

spark-submit --driver-library-path /usr/local/lib /tmp/benchmark.jar --class clime.cmd.ParquetIngestCommand --crs EPSG:4326 --input /home/cbrown/Downloads/one-month-tiles/*.tif --output /home/cbrown/Documents/tif-output/ --layerName test
