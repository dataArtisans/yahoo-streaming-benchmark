/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark.state;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import flink.benchmark.BenchmarkConfig;
import flink.benchmark.generator.HighKeyCardinalityGeneratorSource;
import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.UUID;

/**
 * To Run:  flink run -c flink.benchmark.state.AdvertisingTopologyFlinkStateHighKeyCard target/flink-benchmarks-0.1.0.jar "../conf/benchmarkConf.yaml"
 * <p>
 * <p>
 * Implementation where all state is kept in Flink (not in redis).  Designed for large #'s of campaigns
 */
public class AdvertisingTopologyFlinkStateHighKeyCard {


  public static void main(final String[] args) throws Exception {

    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

    // queryable state registration
    ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(config.akkaZookeeperPath, config.akkaZookeeperQuorum);
    RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);

    // flink environment
    StreamExecutionEnvironment env = setupFlinkEnvironment(config);
    final TypeInformation<Tuple3<String, Long, Long>> queryWindowResultType = TypeInfoParser.parse("Tuple3<String, Long, Long>");

    DataStream<String> rawMessageStream = streamSource(config, env);

    // log performance
    rawMessageStream.flatMap(new ThroughputLogger<>(240, 1_000_000));

    DataStream<UUID> campaignHits = rawMessageStream
        .flatMap(new Deserializer())
        .filter(new EventFilter())
        .assignTimestampsAndWatermarks(new AdTimestampExtractor()) // assign event time stamp and generate watermark
        .map(new Projector());

    // campaign_id, event time
    campaignHits
        .keyBy(identity())
        .transform("Query Window",
            queryWindowResultType,
            new QueryableWindowOperatorEvicting(config.windowSize, registrationService, true));

    env.execute();
  }

  /**
   * Do some Flink Configuration
   */
  private static StreamExecutionEnvironment setupFlinkEnvironment(BenchmarkConfig config) throws IOException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(config.getParameters());
    env.getConfig().enableObjectReuse();

    // enable checkpointing for fault tolerance
    if (config.checkpointsEnabled) {
      env.enableCheckpointing(config.checkpointInterval);
      if (config.checkpointToUri) {
        env.setStateBackend(new FsStateBackend(config.checkpointUri));
      }
    }

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    return env;
  }

  /**
   * The identity selector
   */
  private static KeySelector<UUID, UUID> identity() {
    return (KeySelector<UUID, UUID>) s -> s;
  }

  /**
   * Choose data source, either Kafka or data generator
   */
  private static DataStream<String> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env) {
    RichParallelSourceFunction<String> source;
    String sourceName;
    if (config.useLocalEventGenerator) {
      source = new HighKeyCardinalityGeneratorSource(config);
      sourceName = "EventGenerator";
    } else {
      source = kafkaSource(config);
      sourceName = "Kafka";
    }

    return env.addSource(source, sourceName);
  }

  /**
   * Setup kafka source
   */
  private static FlinkKafkaConsumer09<String> kafkaSource(BenchmarkConfig config) {
    return new FlinkKafkaConsumer09<>(
        config.kafkaTopic,
        new SimpleStringSchema(),
        config.getParameters().getProperties());
  }

  // --------------------------------------------------------------------------
  //   user functions
  // --------------------------------------------------------------------------

  /**
   * Parse JSON
   */
  private static class Deserializer extends
      RichFlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
        throws Exception {
      JSONObject obj = JSON.parseObject(input);
      Tuple7<String, String, String, String, String, String, String> tuple =
          new Tuple7<>(
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

  /**
   * Filter out everything except "view" events
   */
  private static class EventFilter implements
      FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) {
      return tuple.f4.equals("view");
    }
  }

  /**
   * Project to campaign id
   */
  private static class Projector implements MapFunction<Tuple7<String, String, String, String, String, String, String>, UUID> {

    @Override
    public UUID map(Tuple7<String, String, String, String, String, String, String> tuple) {
      return UUID.fromString(tuple.f2);
    }
  }

  /**
   * Generate timestamp and watermarks
   */
  private static class AdTimestampExtractor extends AscendingTimestampExtractor<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public long extractAscendingTimestamp(Tuple7<String, String, String, String, String, String, String> element) {
      return Long.parseLong(element.f5);
    }
  }
}
