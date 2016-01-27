/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.Utils;
import benchmark.common.advertising.PooledRedisConnections;
import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyRedisDirect {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyRedisDirect.class);


    public static void main(final String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Map conf = Utils.findAndReadConfigFile(parameterTool.getRequired("confPath"), true);
        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        long windowSize = 60 * 60 * 1000; // 60 minutes
        String redisHost = flinkBenchmarkParams.getRequired("jedis_server");
        int numRedisThreads = flinkBenchmarkParams.getInt("flink.highcard.redisthreads", 20);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

		// Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));
        env.enableCheckpointing(5000);
        
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))

        Properties kProps = flinkBenchmarkParams.getProperties();
    //    kProps.setProperty("auto.offset.reset", "earliest");
        kProps.setProperty("group.id", "earlasdiest"+UUID.randomUUID());
        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer082<>(
                        flinkBenchmarkParams.getRequired("topic"),
                        new SimpleStringSchema(),
                        kProps));

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

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple7<String, String, String, String, String, String, String> tuple = new Tuple7<>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("campaign_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address"));
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

    private static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<String, String>();
        flinkConfs.put("topic", getKafkaTopic(conf));
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("jedis_server", getRedisHost(conf));
        flinkConfs.put("group.id", "myGroup");

        return flinkConfs;
    }

    private static String getZookeeperServers(Map conf) {
        if(!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if(!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if(!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map conf) {
        if(!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String)conf.get("kafka.topic");
    }

    private static String getRedisHost(Map conf) {
        if(!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String)conf.get("redis.host");
    }

    public static String listOfStringToString(List<String> list, String port) {
        String val = "";
        for(int i=0; i<list.size(); i++) {
            val += list.get(i) + ":" + port;
            if(i < list.size()-1) {
                val += ",";
            }
        }
        return val;
    }
}
