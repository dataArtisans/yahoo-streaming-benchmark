/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark.state;

import benchmark.common.advertising.RedisAdCampaignCache;
import flink.benchmark.generator.EventGenerator;
import flink.benchmark.utils.ThroughputLogger;
import flink.benchmark.utils.Utils;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.util.Map;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  "../conf/benchmarkConf.yaml"
 * <p/>
 * <p/>
 * Implementation where all state is kept in Flink (not in redis)
 */
public class AdvertisingTopologyFlinkState {

  private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkState.class);


  public static void main(final String[] args) throws Exception {

    // load yaml file
    Yaml yml = new Yaml(new SafeConstructor());
    Map<String, String> ymlMap = (Map) yml.load(new FileInputStream(args[0]));
    String kafkaZookeeperConnect = Utils.getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
    String akkaZookeeperQuorum = Utils.getZookeeperServers(ymlMap, "");
    ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
    ymlMap.put("bootstrap.servers", Utils.getKafkaBrokers(ymlMap));
    for (Map.Entry e : ymlMap.entrySet()) {
      {
        e.setValue(e.getValue().toString());
      }
    }

    ParameterTool parameters = ParameterTool.fromMap(ymlMap);

    long windowSize = parameters.getLong("window.size", 10_000);

    String akkaZookeeperPath = parameters.get("akka.zookeeper.path", "/akkaQuery");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameters);

    // Set the buffer timeout (default 100)
    // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
    env.setBufferTimeout(parameters.getLong("flink.buffer.timeout", 100));

    if (parameters.has("flink.checkpoint.interval")) {
      // enable checkpointing for fault tolerance
      env.enableCheckpointing(parameters.getLong("flink.checkpoint-interval", 1000));
    }

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    RichParallelSourceFunction<String> source;
    String sourceName;
    if (parameters.has("use.local.event.generator")) {
      EventGenerator eventGenerator = new EventGenerator(parameters);
      eventGenerator.prepareRedis();
      eventGenerator.writeCampaignFile();
      source = eventGenerator.createSource();
      sourceName = "EventGenerator";
    } else {
      source = kafkaSource(parameters);
      sourceName = "Kafka";
    }

    DataStream<String> rawMessageStream = env.addSource(source, sourceName);

    // log performance
    rawMessageStream.flatMap(new ThroughputLogger<String>(240, 1_000_000));

    DataStream<Tuple2<String, Long>> joinedAdImpressions = rawMessageStream
      // Parse the String as JSON
      .flatMap(new DeserializeBolt())

        // perform join with redis data
      .flatMap(new RedisJoinBolt()); // campaign_id, event_time

      //.assignTimestamps(new AdTimestampExtractor());

    ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(akkaZookeeperPath, akkaZookeeperQuorum);

    RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);

    // campaign_id, window end time
    TypeInformation<Tuple3<String, Long, Long>> resultType = TypeInfoParser.parse("Tuple3<String, Long, Long>");
    DataStream<Tuple3<String, Long, Long>> result = joinedAdImpressions
      // process campaign
      .keyBy(0) // campaign_id
      .transform("Query Window",
        resultType,
        new QueryableWindowOperator(windowSize, registrationService));

    /*    if(parameters.has("write-result-path")) {
            result.writeAsText(parameters.get("write-result-path")).disableChaining();
        } */

    env.execute();
  }

  private static FlinkKafkaConsumer082<String> kafkaSource(ParameterTool parameters) {
    return new FlinkKafkaConsumer082<String>(
      parameters.getRequired("kafka.topic"),
      new SimpleStringSchema(),
      parameters.getProperties());
  }

  public static class DeserializeBolt implements FlatMapFunction<String, Tuple2<String, String>> {

    transient JSONParser p = null;

    @Override
    public void flatMap(String input, Collector<Tuple2<String, String>> out)
      throws Exception {
      if (p == null) {
        p = new JSONParser();
        ;
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
      if (campaign_id == null) {
        return;
      }
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

}
