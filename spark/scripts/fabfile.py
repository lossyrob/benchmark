#!/usr/bin/env python
from __future__ import with_statement
import os

from fabric.api import put, run, shell_env, env, get
import boto
# for fabric
env.user = 'ubuntu'

# env.host_string = 'ec2-52-7-137-13.compute-1.amazonaws.com'  # 2 instances
env.host_string = 'ec2-52-6-47-42.compute-1.amazonaws.com' # 4 instances

env.key_filename = '/home/cbrown/.ssh/geotrellis-spark-us-east-1.pem'

# Utility
current_dir = os.path.split(os.path.abspath(__name__))[0]
deployment_dir = os.path.join(current_dir, 'deployment')

# Send to remote
benchmark_jar = os.path.join('../target/scala-2.10/benchmark-spark-assembly-0.1.0.jar')
benchmark_script = os.path.join(current_dir, 'benchmark.sh')
ingest_script = os.path.join(current_dir, 'ingest.sh')


def get_credentials(aws_profile):
    aws_dir = os.path.expanduser('~/.aws')
    boto_config_path = os.path.join(aws_dir, 'config')
    aws_creds_path = os.path.join(aws_dir, 'credentials')
    boto.config.read([boto_config_path, aws_creds_path])
    aws_access_key_id = boto.config.get(aws_profile, 'aws_access_key_id')
    aws_secret_access_key = boto.config.get(aws_profile, 'aws_secret_access_key')
    return aws_access_key_id, aws_secret_access_key


def send_files():
    put(benchmark_jar, '/tmp/benchmark.jar', mirror_local_mode=True)
    put(benchmark_script, '/tmp/benchmark.sh', mirror_local_mode=True)
    put(ingest_script, '/tmp/ingest.sh', mirror_local_mode=True)


def ingest(climate_model, memory, aws_profile, page_size, local_dirs):
    aws_id, aws_key = get_credentials(aws_profile)
    environment = {
        'JAR': '/tmp/benchmark.jar',
        'AWS_ID': aws_id,
        'AWS_KEY': aws_key,
        'CLIMATE_MODEL': climate_model,
        'MEMORY': memory,
        'LOCAL_DIRS': local_dirs,
        'PAGE_SIZE': page_size
    }
    with shell_env(**environment):
        command = "nohup /tmp/ingest.sh &> /tmp/ingest-{}.out".format(climate_model)
        run(command)


def benchmark(memory, aws_profile, local_dirs):
    aws_id, aws_key = get_credentials(aws_profile)
    environment = {
        'JAR': '/tmp/benchmark.jar',
        'AWS_ID': aws_id,
        'AWS_KEY': aws_key,
        'MEMORY': memory,
        'LOCAL_DIRS': local_dirs
    }
    with shell_env(**environment):
        command = "nohup /tmp/benchmark.sh &> /tmp/benchmark.out"
        run(command)


def get_logs():
    get('/tmp/*.out', './')