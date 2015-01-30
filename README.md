# Benchmarking GeoTrellis
Project for benchmarking GeoTrellis.

### Setting up a benchmarking environment with vagrant and ansible.

If you have [vagrant](https://www.vagrantup.com/) and [ansible](http://www.ansible.com/home) installed, in the root directory of this repository:

`vagrant up`

Afterwords you can `vagrant ssh` into the machine, head to `/vagrant`, and run `./sbt`

#### GeoTiff benchmarking

GeoTiff benchmarks exist in the `geotiff` subproject. To run, you can use the command `/sbt "project geotiff" run`, and select a benchmark to run.
