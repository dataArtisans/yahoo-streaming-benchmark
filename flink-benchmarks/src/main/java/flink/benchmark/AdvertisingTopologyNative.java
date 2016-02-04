/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * To Run:  flink run -c flink.benchmark.AdvertisingTopologyNative target/flink-benchmarks-0.1.0.jar "../conf/benchmarkConf.yaml
 *
 * Computes the windows using custom operator rather than Flink built-in primitives.  This is the approach
 * used in the original Yahoo! benchmark.
 */
public class AdvertisingTopologyNative {

  private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);


  public static void main(final String[] args) throws Exception {

    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(config.getParameters());

    if (config.checkpointsEnabled) {
      env.enableCheckpointing(config.checkpointInterval);
    }

    DataStream<String> messageStream = env.addSource(kafkaSource(config));

    messageStream.flatMap(new ThroughputLogger<String>(240, 1_000_000));

    messageStream
      .rebalance()
      // Parse the String as JSON
      .flatMap(new DeserializeBolt())

      //Filter the records if event type is "view"
      .filter(new EventFilterBolt())

      // project the event
      .<Tuple2<String, String>>project(2, 5)

      // perform join with redis data
      .flatMap(new RedisJoinBolt(config))

      // process campaign
      .keyBy(0)
      .flatMap(new CampaignProcessor(config));

    env.execute();
  }

  /**
   * Create Kafka Source
   */
  private static FlinkKafkaConsumer082<String> kafkaSource(BenchmarkConfig config) {
    return new FlinkKafkaConsumer082<>(
      config.kafkaTopic,
      new SimpleStringSchema(),
      config.getParameters().getProperties());
  }

  /**
   * Parse JSON
   */
  public static class DeserializeBolt implements
    FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

    @Override
    public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
      throws Exception {
      JSONObject obj = new JSONObject(input);
      Tuple7<String, String, String, String, String, String, String> tuple =
        new Tuple7<>(
          obj.getString("user_id"),
          obj.getString("page_id"),
          obj.getString("ad_id"),
          obj.getString("ad_type"),
          obj.getString("event_type"),
          obj.getString("event_time"),
          obj.getString("ip_address"));
      out.collect(tuple);
    }
  }

  /**
   * Filter down to only "view" events
   */
  public static class EventFilterBolt implements
    FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
      return tuple.getField(4).equals("view");
    }
  }

  /**
   * Map from ad id to campaign using cached Redis data
   */
  public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

    private final BenchmarkConfig config;
    RedisAdCampaignCache redisAdCampaignCache;

    RedisJoinBolt(BenchmarkConfig config){
      this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
      //initialize jedis
      LOG.info("Opening connection with Jedis to {}", config.redisHost);
      this.redisAdCampaignCache = new RedisAdCampaignCache(config.redisHost);
      this.redisAdCampaignCache.prepare();
    }

    @Override
    public void flatMap(Tuple2<String, String> input, Collector<Tuple3<String, String, String>> out) throws Exception {
      String ad_id = input.getField(0);
      String campaign_id = this.redisAdCampaignCache.execute(ad_id);
      if (campaign_id == null) {
        return;
      }

      Tuple3<String, String, String> tuple = new Tuple3<>(
        campaign_id,
        (String) input.getField(0),
        (String) input.getField(1));
      out.collect(tuple);
    }
  }

  /**
   * Write windows to Redis
   */
  public static class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

    CampaignProcessorCommon campaignProcessorCommon;

    BenchmarkConfig config;

    CampaignProcessor(BenchmarkConfig config){
      this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
      LOG.info("Opening connection with Jedis to {}", config.redisHost);

      this.campaignProcessorCommon = new CampaignProcessorCommon(config.redisHost);
      this.campaignProcessorCommon.prepare();
    }

    @Override
    public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

      String campaign_id = tuple.getField(0);
      String event_time = tuple.getField(2);
      this.campaignProcessorCommon.execute(campaign_id, event_time);
    }
  }
}
