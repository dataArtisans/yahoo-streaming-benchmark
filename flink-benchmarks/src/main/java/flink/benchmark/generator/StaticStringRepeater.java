package flink.benchmark.generator;

import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


/**
 * Write a string into Kafka as fast as possible
 */
public class StaticStringRepeater {

	private final static String DEFAULT_STRING = "{\"user_id\":\"25048ab5-0787-4d2d-a862-135520be0416\"," +
			"\"page_id\":\"0e7d80a5-fcf7-4736-a677-8d635acc9a18\",\"ad_id\":\"982a89fa-9920-441e-864a-607cd24fadae\"," +
			"\"ad_type\":\"banner78\",\"event_type\":\"view\",\"event_time\":\"1452867808407\",\"ip_address\":\"1.2.3.4\"}";

	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parameterTool);

		DataStream<String> adImpressions = env.addSource(new StringRepeater(parameterTool));
		adImpressions.flatMap(new ThroughputLogger<String>(240, 1_000_000));
		adImpressions.addSink(new FlinkKafkaProducer<>(parameterTool.getRequired("kafka.topic"), new SimpleStringSchema(), parameterTool.getProperties(), new FixedPartitioner()));

		env.execute("StaticStringRepeater" + parameterTool.toMap().toString());
	}

	private static class StringRepeater extends RichParallelSourceFunction<String> {
		private final ParameterTool parameterTool;
		private boolean running = true;

		public StringRepeater(ParameterTool parameterTool) {
			this.parameterTool = parameterTool;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			final String s = parameterTool.get("string", DEFAULT_STRING);
			while(running) {
				ctx.collect(s);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
