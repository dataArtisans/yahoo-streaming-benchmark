package flink.benchmark.generator;

import flink.benchmark.BenchmarkConfig;
import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.List;
import java.util.Map;

/**
 * Distributed Data Generator for AdImpression Events.
 * <p>
 * <p>
 * (by default) we generate 100 campaigns, with 10 ads each.
 * We write those 1000 ads into Redis, with ad_is --> campaign_id
 */
public class AdImpressionsGenerator {

  public static void main(String[] args) throws Exception {

    BenchmarkConfig benchmarkConfig = BenchmarkConfig.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(benchmarkConfig.getParameters());

    EventGeneratorSource eventGenerator = new EventGeneratorSource(benchmarkConfig);

    Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
    RedisHelper redisHelper = new RedisHelper(benchmarkConfig);
    redisHelper.prepareRedis(campaigns);
    redisHelper.writeCampaignFile(campaigns);

    DataStream<String> adImpressions = env.addSource(eventGenerator);

    adImpressions.flatMap(new ThroughputLogger<>(240, 1_000_000));

    adImpressions.addSink(new FlinkKafkaProducer09<>(
        benchmarkConfig.kafkaTopic,
        new SimpleStringSchema(),
        benchmarkConfig.getParameters().getProperties(),
        new FixedPartitioner<>()));

    env.execute("Ad Impressions data generator " + benchmarkConfig.getParameters().toMap().toString());
  }
}
