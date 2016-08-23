/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package storm.benchmark;

import benchmark.common.Utils;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This is a basic example of a Storm topology.
 */
public class AdvertisingTopology {

  private static class DeserializeBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

      JSONObject obj = JSON.parseObject(tuple.getString(0));
      _collector.emit(tuple, new Values(obj.getString("user_id"),
          obj.getString("page_id"),
          obj.getString("ad_id"),
          obj.getString("ad_type"),
          obj.getString("event_type"),
          obj.getString("event_time"),
          obj.getString("ip_address")));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address"));
    }
  }

  private static class EventFilterBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      if (tuple.getStringByField("event_type").equals("view")) {
        _collector.emit(tuple, tuple.getValues());
      }
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address"));
    }
  }

  private static class EventProjectionBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getStringByField("ad_id"),
          tuple.getStringByField("event_time")));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ad_id", "event_time"));
    }
  }

  private static class RedisJoinBolt extends BaseRichBolt {
    private OutputCollector _collector;
    transient RedisAdCampaignCache redisAdCampaignCache;
    private String redisServerHost;

    RedisJoinBolt(String redisServerHost) {
      this.redisServerHost = redisServerHost;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
      this.redisAdCampaignCache.prepare();
    }

    @Override
    public void execute(Tuple tuple) {
      String ad_id = tuple.getStringByField("ad_id");
      String campaign_id = this.redisAdCampaignCache.execute(ad_id);
      if (campaign_id == null) {
        _collector.fail(tuple);
        return;
      }
      _collector.emit(tuple, new Values(campaign_id,
          tuple.getStringByField("ad_id"),
          tuple.getStringByField("event_time")));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("campaign_id", "ad_id", "event_time"));
    }
  }

  private static class CampaignProcessor extends BaseRichBolt {

    private OutputCollector _collector;
    transient private CampaignProcessorCommon campaignProcessorCommon;
    private String redisServerHost;

    CampaignProcessor(String redisServerHost) {
      this.redisServerHost = redisServerHost;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
      this.campaignProcessorCommon.prepare();
    }

    @Override
    public void execute(Tuple tuple) {

      String campaign_id = tuple.getStringByField("campaign_id");
      String event_time = tuple.getStringByField("event_time");

      this.campaignProcessorCommon.execute(campaign_id, event_time);
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  private static String joinHosts(List<String> hosts, String port) {
    String joined = null;
    for (String s : hosts) {
      if (joined == null) {
        joined = "";
      } else {
        joined += ",";
      }

      joined += s + ":" + port;
    }
    return joined;
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    Options opts = new Options();
    opts.addOption("conf", true, "Path to the config file.");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(opts, args);
    String configPath = cmd.getOptionValue("conf");
    Map commonConfig = Utils.findAndReadConfigFile(configPath, true);

    String zkServerHosts = joinHosts((List<String>) commonConfig.get("zookeeper.servers"),
        Integer.toString((Integer) commonConfig.get("zookeeper.port")));
    String zkPath = (String) commonConfig.get("kafka.zookeeper.path");

    String redisServerHost = (String) commonConfig.get("redis.host");
    String kafkaTopic = (String) commonConfig.get("kafka.topic");
    int kafkaPartitions = ((Number) commonConfig.get("kafka.partitions")).intValue();
    int workers = ((Number) commonConfig.get("storm.workers")).intValue();
    int ackers = ((Number) commonConfig.get("storm.ackers")).intValue();
    int cores = ((Number) commonConfig.get("process.cores")).intValue();
    int parallel = Math.max(1, cores / 7);

    ZkHosts hosts = new ZkHosts(zkServerHosts, zkPath + "/brokers");

    SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, zkPath, UUID.randomUUID().toString());
    spoutConfig.stateUpdateIntervalMs = 10_000;
    spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = true;
    spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    builder.setSpout("ads", kafkaSpout, kafkaPartitions);
    builder.setBolt("event_deserializer", new DeserializeBolt(), parallel).shuffleGrouping("ads");
    builder.setBolt("event_filter", new EventFilterBolt(), parallel).shuffleGrouping("event_deserializer");
    builder.setBolt("event_projection", new EventProjectionBolt(), parallel).shuffleGrouping("event_filter");
    builder.setBolt("redis_join", new RedisJoinBolt(redisServerHost), parallel).shuffleGrouping("event_projection");
    builder.setBolt("campaign_processor", new CampaignProcessor(redisServerHost), parallel * 2)
        .fieldsGrouping("redis_join", new Fields("campaign_id"));

    Config conf = new Config();

    if (args != null && args.length > 0) {
      conf.setNumWorkers(workers);
      conf.setNumAckers(ackers);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      org.apache.storm.utils.Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
