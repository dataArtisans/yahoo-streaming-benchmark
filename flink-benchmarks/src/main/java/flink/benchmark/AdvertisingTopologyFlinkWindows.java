/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import flink.benchmark.generator.EventGenerator;
import flink.benchmark.utils.ThroughputLogger;
import flink.benchmark.utils.Utils;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyFlinkWindows {

  private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkWindows.class);


  public static void main(final String[] args) throws Exception {

    // load yaml file
    Yaml yml = new Yaml(new SafeConstructor());
    Map<String, String> ymlMap = (Map) yml.load(new FileInputStream(args[0]));
    String kafkaZookeeperConnect = Utils.getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
    ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
    ymlMap.put("bootstrap.servers", Utils.getKafkaBrokers(ymlMap));
    for (Map.Entry e : ymlMap.entrySet()) {
      {
        e.setValue(e.getValue().toString());
      }
    }

    ParameterTool parameters = ParameterTool.fromMap(ymlMap);

    long windowSize = parameters.getLong("window.size", 10_000);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameters);

    // Set the buffer timeout (default 100)
    // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
    env.setBufferTimeout(parameters.getLong("flink.buffer.timeout", 100));

    if (parameters.has("flink.checkpoint.interval")) {
      // enable checkpointing for fault tolerance
      env.enableCheckpointing(parameters.getLong("flink.checkpoint.interval", 1000));
    }

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    RichParallelSourceFunction<String> source;
    String sourceName;
    if(parameters.has("use.local.event.generator")){
      EventGenerator eventGenerator = new EventGenerator(parameters);
      eventGenerator.prepareRedis();
      eventGenerator.writeCampaignFile();
      source = eventGenerator.createSource();
      sourceName = "EventGenerator";
    }
    else{
      source = kafkaSource(parameters);
      sourceName = "Kafka";
    }

    DataStream<String> rawMessageStream = env.addSource(source, sourceName);

    // log performance
    rawMessageStream.flatMap(new ThroughputLogger<String>(240, 1_000_000));

    DataStream<Tuple2<String, String>> joinedAdImpressions = rawMessageStream
      // Parse the String as JSON
      .flatMap(new DeserializeBolt())

        //Filter the records if event type is "view"
      .filter(new EventFilterBolt())

        // project the event
      .<Tuple2<String, String>>project(2, 5) //ad_id, event_time

        // perform join with redis data
      .flatMap(new RedisJoinBolt()) // campaign_id, event_time

        // extract timestamps and generate watermarks from event_time
      .assignTimestamps(new AdTimestampExtractor());

    // campaign_id, window end time, count
    DataStream<Tuple3<String, String, Long>> result = null;


    WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowStream = joinedAdImpressions.map(new MapToImpressionCount())
      // process campaign
      .keyBy(0) // campaign_id
      .timeWindow(Time.of(windowSize, TimeUnit.MILLISECONDS));

    // set a custom trigger
    windowStream.trigger(new EventAndProcessingTimeTrigger());

    result = windowStream.apply(new ReduceFunction<Tuple3<String, String, Long>>() {
      @Override
      public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t0, Tuple3<String, String, Long> t1) throws Exception {
        t0.f2 += t1.f2;
        return t0;
      }
    }, new WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
      @Override
      public void apply(Tuple keyTuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
        Iterator<Tuple3<String, String, Long>> valIter = values.iterator();
        Tuple3<String, String, Long> tuple = valIter.next();
        if (valIter.hasNext()) {
          throw new IllegalStateException("Unexpected");
        }
        tuple.f1 = Long.toString(window.getEnd());
        out.collect(tuple); // collect end time here
      }
    });

    // write result to redis
    if (parameters.has("add.result.sink")) {
      result.addSink(new RedisResultSink());
    }
    if (parameters.has("add.result.sink.optimized")) {
      result.addSink(new RedisResultSinkOptimized());
    }

    env.execute("AdvertisingTopologyFlinkWindows " + parameters.toMap().toString());
  }

  private static FlinkKafkaConsumer082<String> kafkaSource(ParameterTool parameters) {
    return new FlinkKafkaConsumer082<String>(
      parameters.getRequired("kafka.topic"),
      new SimpleStringSchema(),
      parameters.getProperties());
  }

  public static class EventAndProcessingTimeTrigger implements Trigger<Object, TimeWindow> {

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
      ctx.registerEventTimeTimer(window.maxTimestamp());
      // register system timer only for the first time
      OperatorState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", false);
      if (!firstTimerSet.value()) {
        ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
        firstTimerSet.update(true);
      }
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
      return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
      // schedule next timer
      ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
      return TriggerResult.FIRE;
    }
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
      JSONObject obj = (JSONObject) parser.parse(input);

      Tuple7<String, String, String, String, String, String, String> tuple =
        new Tuple7<String, String, String, String, String, String, String>(
          obj.getAsString("user_id"),
          obj.getAsString("page_id"),
          obj.getAsString("ad_id"),
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
      return tuple.getField(4).equals("view");
    }
  }

  public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

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
      Collector<Tuple2<String, String>> out) throws Exception {
      String ad_id = input.getField(0);
      String campaign_id = this.redisAdCampaignCache.execute(ad_id);
      if (campaign_id == null) {
        return;
      }

      Tuple2<String, String> tuple = new Tuple2<String, String>(
        campaign_id,
        (String) input.getField(1)); // event_time
      out.collect(tuple);
      // LOG.info("Joined to {}", tuple);
    }
  }


  private static class AdTimestampExtractor implements TimestampExtractor<Tuple2<String, String>> {

    long maxTimestampSeen = 0;

    @Override
    public long extractTimestamp(Tuple2<String, String> element, long currentTimestamp) {
      long timestamp = Long.parseLong(element.f1);
      maxTimestampSeen = Math.max(timestamp, maxTimestampSeen);
      return timestamp;
    }

    @Override
    public long extractWatermark(Tuple2<String, String> element, long currentTimestamp) {
      return Long.MIN_VALUE;
    }

    @Override
    public long getCurrentWatermark() {
      long watermark = maxTimestampSeen - 1L;
      return watermark;
    }
  }

  private static class MapToImpressionCount implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {
    @Override
    public Tuple3<String, String, Long> map(Tuple2<String, String> t3) throws Exception {
      return new Tuple3<String, String, Long>(t3.f0, t3.f1, 1L);
    }
  }

  private static class RedisResultSink extends RichSinkFunction<Tuple3<String, String, Long>> {
    private Jedis flushJedis;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      ParameterTool pt = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

      flushJedis = new Jedis(pt.getRequired("redis.host"));
      // flushJedis.select(1); // select db 1
    }

    @Override
    public void invoke(Tuple3<String, String, Long> result) throws Exception {
      // set (campaign, count)
      //    flushJedis.hset("campaign-counts", result.f0, Long.toString(result.f2));

      String campaign = result.f0;
      String timestamp = result.f1;
      String windowUUID = flushJedis.hmget(campaign, timestamp).get(0);
      if (windowUUID == null) {
        windowUUID = UUID.randomUUID().toString();
        flushJedis.hset(campaign, timestamp, windowUUID);

        String windowListUUID = flushJedis.hmget(campaign, "windows").get(0);
        if (windowListUUID == null) {
          windowListUUID = UUID.randomUUID().toString();
          flushJedis.hset(campaign, "windows", windowListUUID);
        }
        flushJedis.lpush(windowListUUID, timestamp);
      }

      flushJedis.hset(windowUUID, "seen_count", Long.toString(result.f2));
      flushJedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
      flushJedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
    }

    @Override
    public void close() throws Exception {
      super.close();
      flushJedis.close();
    }
  }

  private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple3<String, String, Long>> {
    private Jedis flushJedis;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      ParameterTool pt = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

      flushJedis = new Jedis(pt.getRequired("redis.host"));
      flushJedis.select(1); // select db 1
    }

    @Override
    public void invoke(Tuple3<String, String, Long> result) throws Exception {
      // set campaign id -> (window-timestamp, count)
      flushJedis.hset(result.f0, result.f1, Long.toString(result.f2));
    }

    @Override
    public void close() throws Exception {
      super.close();
      flushJedis.close();
    }
  }
}
