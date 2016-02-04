/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.PooledRedisConnections;
import flink.benchmark.generator.HighKeyCardinalityGeneratorSource;
import flink.benchmark.utils.ThroughputLogger;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To Run:  flink run -c flink.benchmark.AdvertisingTopologyRedisDirect target/flink-benchmarks-0.1.0.jar "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyRedisDirect {

  private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyRedisDirect.class);

  public static void main(final String[] args) throws Exception {

    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(config.getParameters());
    env.enableCheckpointing(5000);

    DataStream<String> messageStream = sourceStream(config, env);

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
      .flatMap(new CampaignProcessor(config.windowSize, config.redisHost, config.numRedisThreads));

    env.execute();
  }

  /**
   * Choose either Kafka or data generator as source
   */
  private static DataStream<String> sourceStream(BenchmarkConfig config, StreamExecutionEnvironment env) {
    RichParallelSourceFunction<String> source;
    String sourceName;
    if (config.useLocalEventGenerator) {
      HighKeyCardinalityGeneratorSource eventGenerator = new HighKeyCardinalityGeneratorSource(config);
      source = eventGenerator;
      sourceName = "EventGenerator";
    } else {
      source = new FlinkKafkaConsumer082<>(config.kafkaTopic, new SimpleStringSchema(), config.getParameters().getProperties());
      sourceName = "Kafka";
    }

    return env.addSource(source, sourceName);
  }

  /**
   * Deserialize the JSON
   */
  private static class DeserializeBolt implements
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

  /**
   * Filter out all but "view" events.
   */
  private static class EventFilterBolt implements
    FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
      return tuple.f4.equals("view");
    }
  }

  /**
   * Build windows directly in Redis
   */
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
      String event_time = tuple.f1;

      long timestamp = Long.parseLong(event_time);
      long windowTimestamp = timestamp - (timestamp % windowSize) + windowSize;

      redisConnections.add(campaign_id, String.valueOf(windowTimestamp));
    }
  }
}
