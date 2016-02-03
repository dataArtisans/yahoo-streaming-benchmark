/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.PooledRedisConnections;
import flink.benchmark.generator.HighKeyCardinalityGenerator;
import flink.benchmark.utils.ThroughputLogger;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.util.*;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyRedisDirect {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyRedisDirect.class);


    public static void main(final String[] args) throws Exception {

        // load yaml file
        Yaml yml = new Yaml(new SafeConstructor());
        Map<String, String> ymlMap = (Map) yml.load(new FileInputStream(args[0]));
        String kafkaZookeeperConnect = flink.benchmark.utils.Utils.getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
        ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
        ymlMap.put("bootstrap.servers", flink.benchmark.utils.Utils.getKafkaBrokers(ymlMap));
        for (Map.Entry e : ymlMap.entrySet()) {
            {
                e.setValue(e.getValue().toString());
            }
        }

        ParameterTool parameters = ParameterTool.fromMap(ymlMap);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);

        long windowSize = parameters.getLong("window.size", 60 * 60 * 1000); // 60 minutes
        String redisHost = parameters.get("redis.host");
        int numRedisThreads = parameters.getInt("flink.highcard.redis.threads", 20);

		// Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(parameters.getLong("flink.buffer-timeout", 100));
        env.enableCheckpointing(5000);
        
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))

        Properties kProps = parameters.getProperties();
        kProps.setProperty("auto.offset.reset", "latest");
        kProps.setProperty("group.id", "earlasdiest"+UUID.randomUUID());

        RichParallelSourceFunction<String> source;

        String sourceName;
        if(parameters.has("use.local.event.generator")){
            HighKeyCardinalityGenerator eventGenerator = new HighKeyCardinalityGenerator(parameters);
            source = eventGenerator.createSource();
            sourceName = "EventGenerator";
        }
        else{
            source = new FlinkKafkaConsumer082<>(parameters.getRequired("kafka.topic"), new SimpleStringSchema(), kProps);
            sourceName = "Kafka";
        }

        DataStream<String> messageStream = env.addSource(source, sourceName);

        messageStream.flatMap(new ThroughputLogger<String>(240, 1_000_000));

        messageStream
                .rebalance()
                // Parse the String as JSON
                .flatMap(new DeserializeBolt())

                //Filter the records if event type is "view"
                .filter(new EventFilterBolt())

                // project the event
                .<Tuple2<String, String>>project(2, 5)

                // process campaign
                .keyBy(0)
                .flatMap(new CampaignProcessor(windowSize, redisHost, numRedisThreads));


        env.execute();
    }

    public static class DeserializeBolt implements
      FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        transient JSONParser parser = null;

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
          throws Exception {
            if (parser == null) {
                parser = new JSONParser();
            }
            net.minidev.json.JSONObject obj = (net.minidev.json.JSONObject) parser.parse(input);

            Tuple7<String, String, String, String, String, String, String> tuple =
              new Tuple7<>(
                obj.getAsString("user_id"),
                obj.getAsString("page_id"),
                obj.getAsString("campaign_id"),
                obj.getAsString("ad_type"),
                obj.getAsString("event_type"),
                obj.getAsString("event_time"),
                obj.getAsString("ip_address"));
            out.collect(tuple);
        }
    }

    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.f4.equals("view");
        }
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Tuple2<String, String>, String> {

        private final long windowSize;
        
        private final String redisHost;
        private final int numConnections;
        
        private transient PooledRedisConnections redisConnections;

        public CampaignProcessor(long windowSize, String redisHost, int numConnections) {
            this.windowSize = windowSize;
            this.redisHost = redisHost;
            this.numConnections = numConnections;
        }

        @Override
        public void open(Configuration parameters) {
            LOG.info("Opening connection with Jedis to {}", redisHost);
            this.redisConnections = new PooledRedisConnections(redisHost, numConnections);
        }

        @Override
        public void flatMap(Tuple2<String, String> tuple, Collector<String> out) throws Exception {
            String campaign_id = tuple.f0;
            String event_time =  tuple.f1;

            long timestamp = Long.parseLong(event_time);
            long windowTimestamp = timestamp - (timestamp % windowSize) + windowSize;
            
            redisConnections.add(campaign_id, String.valueOf(windowTimestamp));
        }
    }
}
