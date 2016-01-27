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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  "../conf/benchmarkConf.yaml"
 * <p/>
 * <p/>
 * Implementation where all state is kept in Flink (not in redis)
 */
public class AdvertisingTopologyFlinkStateHighKeyCard {


  public static void main(final String[] args) throws Exception {

    // load yaml file
    Yaml yml = new Yaml(new SafeConstructor());
    Map<String, String> ymlMap = (Map) yml.load(new FileInputStream(args[0]));
    String kafkaZookeeperConnect = Utils.getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
    String akkaZookeeperQuorum = Utils.getZookeeperServers(ymlMap, "");
    ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
    ymlMap.put("bootstrap.servers", Utils.getKafkaBrokers(ymlMap));
    
    for (Map.Entry e : ymlMap.entrySet()) {
        e.setValue(e.getValue().toString());
    }

    ParameterTool parameters = ParameterTool.fromMap(ymlMap);

    String checkpointURI = parameters.get("flink.highcard.checkpointURI");
    long windowSize = parameters.getLong("window.size", 60 * 60 * 1000);
    
    // querable state registration
    String akkaZookeeperPath = parameters.get("akka.zookeeper.path", "/akkaQuery");
    ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(akkaZookeeperPath, akkaZookeeperQuorum);
    RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);
    
    
    // flink environment
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameters);
    env.getConfig().enableObjectReuse();
    
    
    // Set the buffer timeout (default 100)
    // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
    env.setBufferTimeout(parameters.getLong("flink.buffer.timeout", 100));

    if (parameters.has("flink.checkpoint.interval")) {
      // enable checkpointing for fault tolerance
      env.enableCheckpointing(parameters.getLong("flink.checkpoint.interval", 1000));
      env.setStateBackend(new FsStateBackend(checkpointURI));
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

    DataStream<UUID> campaignHits = rawMessageStream
            
        // Parse the String as JSON
        .flatMap(new Deserializer())

        // Filter the records if event type is "view"
        .filter(new EventFilter())

        // assign event time stamp and generate watermark
        .assignTimestamps(new AdTimestampExtractor())
    
        // project the event
        .map(new Projector());

    // campaign_id, event time
    campaignHits
      // process campaign
      .keyBy(new KeySelector<UUID, UUID>() {
        @Override
        public UUID getKey(UUID s) { return s; }
      })
      .transform("Query Window",
        resultType,
        new QueryableWindowOperatorEvicting(windowSize, registrationService, true));

    // for the sake of the experiment, simply drop the hourly window results
    
    env.execute();
  }

  private static FlinkKafkaConsumer08<String> kafkaSource(ParameterTool parameters) {
    Properties props = new Properties();
    props.putAll(parameters.getProperties());
    props.setProperty("group.id", UUID.randomUUID().toString());
    props.setProperty("auto.offset.reset", "earliest");
    
    return new FlinkKafkaConsumer08<>(
      parameters.getRequired("kafka.topic"),
      new SimpleStringSchema(),
      props);
  }
  
  // --------------------------------------------------------------------------
  //   user functions
  // --------------------------------------------------------------------------

  public static class Deserializer extends
          RichFlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

    private transient JSONParser parser = null;

    @Override
    public void open(Configuration parameters) throws Exception {
      parser = new JSONParser();
    }

    @Override
    public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
            throws Exception
    {
      JSONObject obj = (JSONObject) parser.parse(input);

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

  public static class EventFilter implements
          FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
    @Override
    public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) {
      return tuple.f4.equals("view");
    }
  }

  public static class Projector implements MapFunction<Tuple7<String, String, String, String, String, String, String>, UUID> {
    
    @Override
    public UUID map(Tuple7<String, String, String, String, String, String, String> tuple) {
      return UUID.fromString(tuple.f2);
    }
  }

  public static class AdTimestampExtractor extends AscendingTimestampExtractor<Tuple7<String, String, String, String, String, String, String>> {
    
    @Override
    public long extractAscendingTimestamp(Tuple7<String, String, String, String, String, String, String> element, long currentTimestamp) {
      return Long.parseLong(element.f5);
    }
  }

  private static final TypeInformation<Tuple3<String, Long, Long>> resultType = TypeInfoParser.parse("Tuple3<String, Long, Long>");
}
