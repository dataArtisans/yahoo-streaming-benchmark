package flink.benchmark.generator;

import flink.benchmark.AdvertisingTopologyFlinkWindows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.XORShiftRandom;
import redis.clients.jedis.Jedis;
import scala.xml.PrettyPrinter;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Distributed Data Generator for AdImpression Events.
 *
 *
 * (by default) we generate 100 campaigns, with 10 ads each.
 * We write those 1000 ads into Redis, with ad_is --> campaign_id
 */
public class AdImpressionsGenerator {

	public static void main(String[] args) throws Exception {

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		int numCampaigns = 100;
		int numAdsPerCampaign = 10;
		String redisHost = parameterTool.get("redis.host", "localhost");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parameterTool);

		List<String> ads = new ArrayList<>(numCampaigns * numAdsPerCampaign);
		PrintWriter adToCampaignFile = new PrintWriter("ad-to-campaign-ids.txt");

		Jedis redis = new Jedis(redisHost);
		if(parameterTool.has("redis.db")) {
			redis.select(parameterTool.getInt("redis.db"));
		}
		if(parameterTool.has("redis.flush")) {
			redis.flushDB();
		}
		for (int i = 0; i < numCampaigns; i++) {
			String campaign = UUID.randomUUID().toString();
			// add campaign to set of campaigns
			redis.sadd("campaigns", campaign);
			for(int j = 0; j < numAdsPerCampaign; j++) {
				String ad = UUID.randomUUID().toString();
				ads.add(ad);
				adToCampaignFile.println("{\"" + ad + "\":\"" + campaign + "\"}");
				redis.set(ad, campaign);
			}
		}
		redis.close();

		DataStream<String> adImpressions = env.addSource(new EventGenerator(ads, parameterTool));

		adImpressions.flatMap(new AdvertisingTopologyFlinkWindows.ThroughputLogger<String>(240, 1_000_000));

		adImpressions.addSink(new FlinkKafkaProducer<>(parameterTool.getRequired("kafka.topic"), new SimpleStringSchema(), parameterTool.getProperties(), new FixedPartitioner()));

		env.execute("Ad Impressions data generator " + parameterTool.toMap().toString());
	}


	private static class EventGenerator extends RichParallelSourceFunction<String> {

		private final List<String> ads;
		private final String[] eventTypes;
		private final ParameterTool parameterTool;
		private boolean running = true;

		public EventGenerator(List<String> ads, ParameterTool pt) {
			this.parameterTool = pt;
			this.ads = ads;
			this.eventTypes = new String[] {"view", "click", "purchase"};
		}

		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			// both are not used, so we don't need to generate them randomly
			String pageID = UUID.randomUUID().toString();
			String userID = UUID.randomUUID().toString();
			int adsIdx = 0;
			int eventsIdx = 0;
			StringBuilder sb = new StringBuilder();
			long elements = 0;
			// the default values lead to ~500k/sec/core on my machine
			long elementsPerSleep = parameterTool.getLong("sleep.frequency", 500_000);
			long sleepDuration = parameterTool.getLong("sleep.duration", 800);
			while(running) {
				long now = System.currentTimeMillis();
				if(elements++ % elementsPerSleep == 0) {
					if(sleepDuration > 0) {
						Thread.sleep(sleepDuration);
					}
				}
				sb.append("{\"user_id\":\""); sb.append(pageID);
				sb.append("\",\"page_id\":\""); sb.append(userID);
				sb.append("\",\"ad_id\":\""); sb.append(ads.get(adsIdx++));
				sb.append("\",\"ad_type\":\""); sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
				sb.append("\",\"event_type\":\""); sb.append(eventTypes[eventsIdx++]);
				sb.append("\",\"event_time\":\""); sb.append(now);
				sb.append("\",\"ip_address\":\"1.2.3.4\"}");
				if(adsIdx == ads.size()) {
					adsIdx = 0;
				}
				if(eventsIdx == eventTypes.length) {
					eventsIdx = 0;
				}
				sourceContext.collect(sb.toString());
				sb.setLength(0);
			}
			sourceContext.close();
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
