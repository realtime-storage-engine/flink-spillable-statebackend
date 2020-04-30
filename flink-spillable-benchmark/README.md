# Spillable Benchmark

This benchmark uses a simple word count to compare performance of different state backends.

## Building

```
git clone https://github.com/realtime-storage-engine/flink-spillable-statebackend.git
cd flink-spillable-statebackend/flink-spillable-benchmark
mvn clean package
```

Benchmark JAR file (`flink-spillable-benchmark-1.0-SNAPSHOT.jar`) will be installed into `target` directory.

## Options

* jobName - name of the job
* wordNumber - number of different keys
* wordLength - length of `String` word
* wordRate - rate of source to emit words
* checkpointInterval - the checkpoint interval, in milliseconds

## Run Benchmark

1. set `state.backend` and backend-related configurations in flink-conf.yaml

2. submit job to yarn cluster on per-job mode as following

```bash
export jobName="spillable-bench"
export wordNumber="20000000"
export wordLength="16"
export wordRate="1000000"
export checkpointInterval="60000"
export mainClass="org.apache.flink.spillable.benchmark.WordCount"

./bin/flink run -d -m yarn-cluster -ynm ${jobName} -c ${mainClass} /path/to/benchmark-jar -jobName ${jobName} -checkpointInterval ${checkpointInterval} -wordNumber ${wordNumber} -wordRate ${wordRate} -wordLength ${wordLength}
```