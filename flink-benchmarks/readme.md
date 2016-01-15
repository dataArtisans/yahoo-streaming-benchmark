Start Flink state consuming topology: flink run -p 20 -c flink.benchmark.state.AdvertisingTopologyFlinkState flink-benchmarks-0.1.0.jar ../../conf/benchmarkConf.yaml 


Start state query tool: java -cp yahoo-streaming-benchmark/flink-benchmarks/target/flink-benchmarks-0.1.0.jar flink.benchmark.state.AkkaStateQuery --zookeeper cdh544-worker-0:2181