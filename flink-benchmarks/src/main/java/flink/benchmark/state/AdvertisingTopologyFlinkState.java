/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark.state;

import benchmark.common.advertising.RedisAdCampaignCache;
import flink.benchmark.BenchmarkConfig;
import flink.benchmark.generator.EventGeneratorSource;
import flink.benchmark.generator.RedisHelper;
import flink.benchmark.utils.ThroughputLogger;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To Run:  flink run -c flink.benchmark.state.AdvertisingTopologyFlinkState target/flink-benchmarks-0.1.0.jar "../conf/benchmarkConf.yaml"
 * <p>
 * <p>
 * Implementation where all state is kept in Flink (not in redis)
 */
public class AdvertisingTopologyFlinkState {

  private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkState.class);


  public static void main(final String[] args) throws Exception {
    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(config.getParameters());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // use event time

    if (config.checkpointsEnabled) {
      env.enableCheckpointing(config.checkpointInterval);
    }

    ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(config.akkaZookeeperPath, config.akkaZookeeperQuorum);
    RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);
    TypeInformation<Tuple3<String, Long, Long>> queryWindowResultType = TypeInfoParser.parse("Tuple3<String, Long, Long>");

    DataStream<String> rawMessageStream = sourceStream(config, env);

    // log performance
    rawMessageStream.flatMap(new ThroughputLogger<String>(240, 1_000_000));

    // campaign_id, window end time
    DataStream<Tuple3<String, Long, Long>> result = rawMessageStream
      .flatMap(new DeserializeBolt())
      .flatMap(new RedisJoinBolt(config))
      .keyBy(0) // campaign_id
      .transform("Query Window", queryWindowResultType, new QueryableWindowOperator(config.windowSize, registrationService));

    env.execute();
  }

  /**
   * Choose source - either Kafka or data generator
   */
  private static DataStream<String> sourceStream(BenchmarkConfig config, StreamExecutionEnvironment env) {
    RichParallelSourceFunction<String> source;
    String sourceName;
    if (config.useLocalEventGenerator) {
      EventGeneratorSource eventGenerator = new EventGeneratorSource(config);
      source = eventGenerator;
      sourceName = "EventGenerator";
      prepareRedis(config, eventGenerator);
    } else {
      source = kafkaSource(config);
      sourceName = "Kafka";
    }

    return env.addSource(source, sourceName);
  }

  /**
   * Prepare Redis for test
   */
  private static void prepareRedis(BenchmarkConfig benchmarkConfig, EventGeneratorSource eventGenerator) {
    RedisHelper redisHelper = new RedisHelper(benchmarkConfig);
    redisHelper.prepareRedis(eventGenerator.getCampaigns());
    redisHelper.writeCampaignFile(eventGenerator.getCampaigns());
  }

  /**
   * Create a Kafka source
   */
  private static FlinkKafkaConsumer082<String> kafkaSource(BenchmarkConfig config) {
    return new FlinkKafkaConsumer082<>(
      config.kafkaTopic,
      new SimpleStringSchema(),
      config.getParameters().getProperties());
  }

  /**
   * Deserialize JSON
   */
  private static class DeserializeBolt implements FlatMapFunction<String, Tuple2<String, String>> {

    transient JSONParser p = null;

    @Override
    public void flatMap(String input, Collector<Tuple2<String, String>> out)
      throws Exception {
      if (p == null) {
        p = new JSONParser();
      }
      JSONObject obj = (JSONObject) p.parse(input);
      // filter
      if (obj.getAsString("event_type").equals("view")) {
        // project
        Tuple2<String, String> tuple = new Tuple2<>(obj.getAsString("ad_id"), obj.getAsString("event_time"));
        out.collect(tuple);
      }
    }
  }

  /**
   * Map from ad to campaign using Redis
   */
  private static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Long>> {

    RedisAdCampaignCache redisAdCampaignCache;
    private BenchmarkConfig config;

    RedisJoinBolt(BenchmarkConfig config){
      this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
      //initialize jedis
      String redis_host = config.redisHost;
      LOG.info("Opening connection with Jedis to {}", redis_host);
      this.redisAdCampaignCache = new RedisAdCampaignCache(redis_host);
      this.redisAdCampaignCache.prepare();
    }

    @Override
    public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Long>> out) throws Exception {
      String ad_id = input.getField(0);
      String campaign_id = this.redisAdCampaignCache.execute(ad_id);
      if (campaign_id == null) {
        return;
      }
      Tuple2<String, Long> tuple = new Tuple2<>(campaign_id, Long.parseLong(input.f1));
      out.collect(tuple);
    }
  }
}
