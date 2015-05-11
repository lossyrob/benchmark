# Benchmarking GeoTrellis
Project for benchmarking GeoTrellis.

### Setting up a benchmarking environment with vagrant and ansible.

If you have [vagrant](https://www.vagrantup.com/) and [ansible](http://www.ansible.com/home) installed, in the root directory of this repository:

`vagrant up`

Afterwords you can `vagrant ssh` into the machine, head to `/vagrant`, and run `./sbt`

#### GeoTiff benchmarking

GeoTiff benchmarks exist in the `geotiff` subproject. To run, you can use the command `/sbt "project geotiff" run`, and select a benchmark to run.

#### Spark benchmarking

To conduct spark benchmarking, it is suggested to perform this on a Spark mesos cluster using the following steps.

1. Bring up a cluster using the [geotrellis-ec2-cluster]() project.
2. Create an uber-jar with SBT assembly (`project spark assembly`)
3. Copy the uber-jar to the mesos master machine and copy the benchmark scripts to the same machine. Run the benchmark scripts.
