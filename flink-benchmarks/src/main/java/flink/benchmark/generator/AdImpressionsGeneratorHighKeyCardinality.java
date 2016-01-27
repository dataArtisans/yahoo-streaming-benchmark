package flink.benchmark.generator;

import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Distributed Data Generator for AdImpression Events.
 * <p>
 * <p>
 * (by default) we generate 100 campaigns, with 10 ads each.
 * We write those 1000 ads into Redis, with ad_is --> campaign_id
 */
public class AdImpressionsGeneratorHighKeyCardinality {

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		SourceFunction<String> source = new HighKeyCardinalityGenerator(parameterTool).createSource();
		DataStream<String> adImpressions = env.addSource(source);

		adImpressions.flatMap(new ThroughputLogger<String>(240, 1_000_000));

		adImpressions.addSink(new FlinkKafkaProducer08<>(
				parameterTool.getRequired("kafka.topic"),
				new SimpleStringSchema(), 
				parameterTool.getProperties(),
				new FixedPartitioner<String>()));

		env.execute("Ad Impressions data generator " + parameterTool.toMap().toString());
	}
}
