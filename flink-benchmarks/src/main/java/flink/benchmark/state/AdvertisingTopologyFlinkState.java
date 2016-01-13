/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark.state;

import benchmark.common.advertising.RedisAdCampaignCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 *
 *
 * Implementation where all state is kept in Flink (not in redis)
 *
 */
public class AdvertisingTopologyFlinkState {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkState.class);


    public static void main(final String[] args) throws Exception {

        // load yaml file
        Yaml yml = new Yaml(new SafeConstructor());
        Map ymlMap = (Map) yml.load(new FileInputStream(args[0]));
        ymlMap.put("zookeeper.connect", "localhost:"+Integer.toString((Integer)ymlMap.get("zookeeper.port")) ); //TODO hack
        ymlMap.put("group.id", "abcaaak" + UUID.randomUUID());
        ymlMap.put("bootstrap.servers", "localhost:9092");
        ymlMap.put("auto.offset.reset", "earliest");
        ParameterTool parameters = ParameterTool.fromMap(ymlMap);

        long windowSize = parameters.getLong("window-size", 10_000);

        String zookeeper = parameters.get("zookeeper", "localhost:2181");
        String zooKeeperPath = parameters.get("zkPath", "/akkaQuery");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);

		// Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(parameters.getLong("flink.buffer-timeout", 100));

        if(parameters.has("flink.checkpoint-interval")) {
            // enable checkpointing for fault tolerance
            env.enableCheckpointing(parameters.getLong("flink.checkpoint-interval", 1000));
        }

        env.setParallelism(8);

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> rawMessageStream = env
                .addSource(new FlinkKafkaConsumer082<>(
                        parameters.getRequired("kafka.topic"),
                        new SimpleStringSchema(),
                        parameters.getProperties()));

        // log performance
        rawMessageStream.flatMap(new ThroughputLogger<String>(37 + 14 + 8));

        DataStream<Tuple2<String, Long>> joinedAdImpressions = rawMessageStream
            // Parse the String as JSON
            .flatMap(new DeserializeBolt())

            // perform join with redis data
            .flatMap(new RedisJoinBolt()) // campaign_id, event_time

            // extract timestamps and generate watermarks from event_time
            .assignTimestamps(new AdTimestampExtractor());

        ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(zooKeeperPath, zookeeper);

        RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);

        // campaign_id, window end time
        TypeInformation<Tuple3<String, Long, Long>> resultType = TypeInfoParser.parse("Tuple3<String, Long, Long>");
        DataStream<Tuple3<String, Long, Long>> result = joinedAdImpressions
            // process campaign
            .keyBy(0) // campaign_id
            .transform("Query Window",
                    resultType,
                    new QueryableWindowOperator(windowSize, registrationService));

        if(parameters.has("write-result-path")) {
            result.writeAsText(parameters.get("write-result-path")).disableChaining();
        }

        env.execute();
    }


    public static class DeserializeBolt implements FlatMapFunction<String, Tuple2<String, String>> {

        transient JSONParser p = null;
        @Override
        public void flatMap(String input, Collector<Tuple2<String, String>> out)
                throws Exception {
            if(p == null) {
                p = new JSONParser();;
            }
            JSONObject obj = (JSONObject) p.parse(input);
            // filter
            if(obj.getAsString("event_type").equals("view")) {
                // project
                Tuple2<String, String> tuple = new Tuple2<>(obj.getAsString("ad_id"), obj.getAsString("event_time"));
                out.collect(tuple);
            }
        }
    }


    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Long>> {

        RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            String redis_host = parameterTool.getRequired("redis.host");
            LOG.info("Opening connection with Jedis to {}", redis_host);
            this.redisAdCampaignCache = new RedisAdCampaignCache(redis_host);
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple2<String, Long>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }
            System.out.println("survived join");
            Tuple2<String, Long> tuple = new Tuple2<>(campaign_id, Long.parseLong(input.f1));
            out.collect(tuple);
        }

    }

    private static class AdTimestampExtractor extends AscendingTimestampExtractor<Tuple2<String, Long>> {
        @Override
        public long extractAscendingTimestamp(Tuple2<String, Long> element, long currentTimestamp) {
            return element.f1;
        }
    }


	/**
     * Simple utility to log throughput
     * @param <T>
     */
    public static class ThroughputLogger<T> implements FlatMapFunction<T, Integer> {
        long received = 0;
        long logfreq = 50000;
        long lastLog = -1;
        long lastElements = 0;
        private int elementSize;

        public ThroughputLogger(int elementSize) {
            this.elementSize = elementSize;
        }

        @Override
        public void flatMap(T element, Collector<Integer> collector) throws Exception {
            received++;
            if (received % logfreq == 0) {
                // throughput over entire time
                long now = System.currentTimeMillis();

                // throughput for the last "logfreq" elements
                if(lastLog == -1) {
                    // init (the first)
                    lastLog = now;
                    lastElements = received;
                } else {
                    long timeDiff = now - lastLog;
                    long elementDiff = received - lastElements;
                    double ex = (1000/(double)timeDiff);
                    LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. GB received {}",
                            timeDiff, elementDiff, elementDiff*ex, (received * elementSize) / 1024 / 1024 / 1024);
                    // reinit
                    lastLog = now;
                    lastElements = received;
                }
            }
        }
    }
}
