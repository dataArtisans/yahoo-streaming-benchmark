package benchmark.common.advertising;


import redis.clients.jedis.Jedis;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class PooledRedisConnections {
	
	private final ArrayBlockingQueue<CampaignAndWindow> queue;
	
	private final RedisConnection[] connections;
	
	private volatile Throwable asyncException;
	
	
	public PooledRedisConnections(String redisServerHost, int numConnections) {
		this.queue = new ArrayBlockingQueue<>(numConnections);
		this.connections = new RedisConnection[numConnections];
		for (int i = 0; i < numConnections; i++) {
			Jedis jedis = new Jedis(redisServerHost);
			jedis.select(1);

			this.connections[i] = new RedisConnection(jedis, queue, this);
			this.connections[i].start();
		}
	}
	
	
	public void add(String campaign, String windowTimestamp) throws InterruptedException {
		if (asyncException != null) {
			throw new RuntimeException(asyncException);
		}
		
		queue.put(new CampaignAndWindow(campaign, windowTimestamp));
	}

	public void setAsyncException(Throwable t) {
		if (asyncException == null) {
			asyncException = t;
		}
	}
	
	public void shutdown() {
		for (RedisConnection c : connections) {
			c.shutdown();
		}
	}

	// ------------------------------------------------------------------------

	private static final class CampaignAndWindow {
		
		final String campaign;
		final String windowTimestamp;


		CampaignAndWindow(String campaign, String windowTimestamp) {
			this.campaign = campaign;
			this.windowTimestamp = windowTimestamp;
		}
	}

	// ------------------------------------------------------------------------
	
	public static class RedisConnection extends Thread {

		private final Jedis jedis;
		private final BlockingQueue<CampaignAndWindow> queue;
		private final PooledRedisConnections errorReporter;

		private volatile boolean running;

		public RedisConnection(Jedis jedis, BlockingQueue<CampaignAndWindow> queue, PooledRedisConnections errorReporter) {
			this.jedis = jedis;
			this.queue = queue;
			this.errorReporter = errorReporter;
			this.running = true;
		}

		@Override
		public void run() {
			while (running) {
				try {
					CampaignAndWindow next = queue.take();

					// we do not use hincr here, because this is not applicable for most types
					// of state and statistics
					String jedisEntry = jedis.hget(next.campaign, next.windowTimestamp);
					long count = (jedisEntry == null || jedisEntry.length() == 0) ? 1L : Long.parseLong(jedisEntry) + 1;
					jedis.hset(next.campaign, next.windowTimestamp, String.valueOf(count));
				}
				catch (InterruptedException ignored) {}
				catch (Throwable t) {
					running = false;
					errorReporter.setAsyncException(t);
				}
			}

			jedis.shutdown();
		}

		public void shutdown() {
			this.running = false;
			this.interrupt();
			this.jedis.shutdown();
		}
	}
}
