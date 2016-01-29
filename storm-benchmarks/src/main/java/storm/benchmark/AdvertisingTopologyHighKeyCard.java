/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package storm.benchmark;

import benchmark.common.advertising.PooledRedisConnections;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import benchmark.common.Utils;
import java.util.Map;
import java.util.UUID;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;

/**
 * This is a basic example of a Storm topology.
 */
public class AdvertisingTopologyHighKeyCard {

    public static class DeserializeBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {

            JSONObject obj = new JSONObject(tuple.getString(0));
            _collector.emit(tuple, new Values(obj.getString("user_id"),
                                              obj.getString("page_id"),
                                              obj.getString("campaign_id"),
                                              obj.getString("ad_type"),
                                              obj.getString("event_type"),
                                              obj.getString("event_time"),
                                              obj.getString("ip_address")));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("user_id", "page_id", "campaign_id", "ad_type", "event_type", "event_time", "ip_address"));
        }
    }

    public static class EventFilterBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            if(tuple.getStringByField("event_type").equals("view")) {
                _collector.emit(tuple, tuple.getValues());
            }
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("user_id", "page_id", "campaign_id", "ad_type", "event_type", "event_time", "ip_address"));
        }
    }

    public static class EventProjectionBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getStringByField("campaign_id"),
                                              tuple.getStringByField("event_time")));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("campaign_id", "event_time"));
        }
    }

    public static class CampaignProcessorMultiThreaded extends BaseRichBolt {

        private final String redisServerHost;
        private final int numWorkerTheads;

        private final long windowSize;

        private transient OutputCollector _collector;

        private transient PooledRedisConnections redisConnections;


        public CampaignProcessorMultiThreaded(long windowSize, String redisServerHost, int numWorkerTheads) {
            this.windowSize = windowSize;
            this.redisServerHost = redisServerHost;
            this.numWorkerTheads = numWorkerTheads;
        }

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            redisConnections = new PooledRedisConnections(redisServerHost, numWorkerTheads);
        }

        @Override
        public void execute(Tuple tuple) {
            try {
                final String campaign_id = tuple.getStringByField("campaign_id");
                final String event_time = tuple.getStringByField("event_time");

                long timestamp = Long.parseLong(event_time);
                long windowTimestamp = timestamp - (timestamp % windowSize) + windowSize;

                redisConnections.add(campaign_id, String.valueOf(windowTimestamp));
                _collector.ack(tuple);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void cleanup() {
            if (redisConnections != null) {
                redisConnections.shutdown ();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    }

    private static String joinHosts(List<String> hosts, String port) {
        String joined = null;
        for(String s : hosts) {
            if(joined == null) {
                joined = "";
            }
            else {
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

        String redisServerHost = (String)commonConfig.get("redis.host");
        String kafkaTopic = (String)commonConfig.get("kafka.topic");
        int kafkaPartitions = ((Number)commonConfig.get("kafka.partitions")).intValue();
        int workers = ((Number)commonConfig.get("storm.workers")).intValue();
        int ackers = ((Number)commonConfig.get("storm.ackers")).intValue();
        int cores = ((Number)commonConfig.get("process.cores")).intValue();
        int parallel = Math.max(1, cores/5);

        long windowSize = 60 * 60 * 1000; // 60 minutes
        int numRedisThreads = ((Number)commonConfig.get("storm.highcard.redisthreads")).intValue();

        ZkHosts hosts = new ZkHosts(zkServerHosts, zkPath + "/brokers");

//        GlobalPartitionInformation gpi = new GlobalPartitionInformation();
//        gpi.addPartition(0, new Broker("localhost", 9092));
//        BrokerHosts hosts = new StaticHosts(gpi);

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
        builder.setBolt("campaign_processor", new CampaignProcessorMultiThreaded(windowSize, redisServerHost, numRedisThreads), parallel)
            .fieldsGrouping("event_projection", new Fields("campaign_id"));

        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(workers);
            conf.setNumAckers(ackers);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            org.apache.storm.utils.Utils.sleep(10000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
