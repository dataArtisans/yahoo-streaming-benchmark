package benchmark.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RedisPerformanceTest {

	/**
	 * Very simple test to see how fast Redis can go
	 */
	public static void main(String[] args) {

		Jedis flush_jedis = new Jedis("localhost");
		flush_jedis.select(1);

		List<String> campaigns = new ArrayList<String>();
		for (int i = 0; i < 100; i++) {
			campaigns.add(UUID.randomUUID().toString());
		}

		long campaignUpdates = 0;
		//long redisOps = 0;
		//long lastLog = -1;
		//long lastElements = 0;

		// log number of redis operations
		StandaloneThroughputLogger stl = new StandaloneThroughputLogger(150000);
		while (true) {
			for (String campaign : campaigns) {

				campaignUpdates++;
				long now = System.currentTimeMillis();
				stl.observe(now);

				if (campaignUpdates % 50000 == 0) {
					System.out.println("Updated " + campaignUpdates + " windows. Redis ops: " + stl.getObservations());
				}

				String timestamp = Long.toString(now % 10000);

				String windowUUID = flush_jedis.hmget(campaign, timestamp).get(0);
				stl.observe(now);
				if (windowUUID == null) {
					windowUUID = UUID.randomUUID().toString();
					flush_jedis.hset(campaign, timestamp, windowUUID);
					stl.observe(now);

					String windowListUUID = flush_jedis.hmget(campaign, "windows").get(0);
					stl.observe(now);
					if (windowListUUID == null) {
						windowListUUID = UUID.randomUUID().toString();
						flush_jedis.hset(campaign, "windows", windowListUUID);
						stl.observe(now);
					}
					flush_jedis.lpush(windowListUUID, timestamp);
					stl.observe(now);
				}

				flush_jedis.hincrBy(windowUUID, "seen_count", 1 /*use static value here*/);
				stl.observe(now);
				flush_jedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
				stl.observe(now);
				flush_jedis.lpush("time_updated", Long.toString(System.currentTimeMillis())); stl.observe(now);
			}
		}
	}

	public static class StandaloneThroughputLogger {
		private long lastLog = -1;
		private long lastElements = 0;
		private long observations = 0;

		private long frequency = 50000;

		public StandaloneThroughputLogger(long frequency) {
			this.frequency = frequency;
		}

		public void observe(long now) {
			if(now == 0) {
				now = System.currentTimeMillis();
			}
			observations++;

			if(observations % frequency == 0) {
				if (lastLog == -1) {
					// init (the first)
					lastLog = now;
					lastElements = observations;
				} else {
					long timeDiff = now - lastLog;
					long elementDiff = observations - lastElements;
					double ex = (1000 / (double) timeDiff);
					System.out.println("During the last " + timeDiff + " ms, we received " + elementDiff + " elements. That's " + (elementDiff * ex) + " elements/second/core.");
					// reinit
					lastLog = now;
					lastElements = observations;
				}
			}
		}

		public long getObservations() {
			return observations;
		}

		public void setObservations(long observations) {
			this.observations = observations;
		}
	}
}
