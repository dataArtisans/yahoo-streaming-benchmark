package flink.benchmark.generator;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.PrintWriter;
import java.util.*;

public class EventGenerator{
  private static final Logger LOG = LoggerFactory.getLogger(EventGenerator.class);

  private final Map<String, List<String>> campaigns;
  private final ParameterTool parameterTool;

  public EventGenerator(ParameterTool pt) {
    this.parameterTool = pt;
    this.campaigns = generateCampaigns();
  }

  public void prepareRedis(){
    String redisHost = parameterTool.get("redis.host", "localhost");
    Jedis redis = new Jedis(redisHost);
    if (parameterTool.has("redis.db")) {
      int redisDB = parameterTool.getInt("redis.db");
      LOG.info("Selecting Redis DB {}.", redisDB);
      redis.select(redisDB);
    }
    if (parameterTool.has("redis.flush")) {
      LOG.info("Flushing Redis DB.");
      redis.flushDB();
    }

    LOG.info("Preparing Redis with campaign data.");
    for(Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
      String campaign = entry.getKey();
      redis.sadd("campaigns", campaign);
      for(String ad : entry.getValue()) {
        redis.set(ad, campaign);
      }
    }
    redis.close();
  }

  public void writeCampaignFile(){
    try {
      PrintWriter adToCampaignFile = new PrintWriter("ad-to-campaign-ids.txt");
      for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
        String campaign = entry.getKey();
        for (String ad : entry.getValue()) {
          adToCampaignFile.println("{\"" + ad + "\":\"" + campaign + "\"}");
        }
      }
      adToCampaignFile.close();
    }
    catch(Throwable t){
      throw new RuntimeException("Error opening ads file", t);
    }
  }

  private Map<String, List<String>> generateCampaigns() {
    int numCampaigns = 100;
    int numAdsPerCampaign = 10;
    Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
    for(int i=0; i<numCampaigns; i++){
      String campaign = UUID.randomUUID().toString();
      ArrayList<String> ads = new ArrayList<>();
      adsByCampaign.put(campaign, ads);
      for(int j=0; j<numAdsPerCampaign; j++){
        ads.add(UUID.randomUUID().toString());
      }
    }
    return adsByCampaign;
  }

  private List<String> flattenCampaigns() {
    // Flatten campaigns into simple list of ads
    List<String> ads = new ArrayList<>();
    for(Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
      for(String ad : entry.getValue()) {
        ads.add(ad);
      }
    }
    return ads;
  }

  public RichParallelSourceFunction<String> createSource(){
    return new Source(parameterTool, flattenCampaigns());
  }

  public static class Source extends RichParallelSourceFunction<String>{
    private ParameterTool parameterTool;
    private final String[] eventTypes;
    private List<String> ads;

    private boolean running = true;

    public Source(ParameterTool parameterTool, List<String> ads){
      this.parameterTool = parameterTool;
      this.eventTypes = new String[]{"view", "click", "purchase"};
      this.ads = ads;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
      // both are not used, so we don't need to generate them randomly
      String pageID = UUID.randomUUID().toString();
      String userID = UUID.randomUUID().toString();
      int adsIdx = 0;
      int eventsIdx = 0;
      StringBuilder sb = new StringBuilder();

      int loadTargetHz = parameterTool.getInt("load.target.hz", 400_000);
      int timesliceLengthMs = parameterTool.getInt("load.timeslice.length.ms", 100);
      int messagesPerTimeslice = loadPerTimeslice(loadTargetHz, getRuntimeContext().getNumberOfParallelSubtasks(), timesliceLengthMs);

      while (running) {
        long emitStartTime = System.currentTimeMillis();
        for (int i = 0; i < messagesPerTimeslice; i++) {
          sb.append("{\"user_id\":\"");
          sb.append(pageID);
          sb.append("\",\"page_id\":\"");
          sb.append(userID);
          sb.append("\",\"ad_id\":\"");
          sb.append(ads.get(adsIdx++));
          sb.append("\",\"ad_type\":\"");
          sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
          sb.append("\",\"event_type\":\"");
          sb.append(eventTypes[eventsIdx++]);
          sb.append("\",\"event_time\":\"");
          sb.append(emitStartTime);
          sb.append("\",\"ip_address\":\"1.2.3.4\"}");
          sourceContext.collect(sb.toString());
          sb.setLength(0);

          if (adsIdx == ads.size()) {
            adsIdx = 0;
          }
          if (eventsIdx == eventTypes.length) {
            eventsIdx = 0;
          }
        }
        // Sleep for the rest of timeslice if needed
        long emitTime = System.currentTimeMillis() - emitStartTime;
        if (emitTime < timesliceLengthMs) {
          Thread.sleep(timesliceLengthMs - emitTime);
        }
      }
      sourceContext.close();
    }

    @Override
    public void cancel() {
      running = false;
    }

    private int loadPerTimeslice(int loadTargetHz, int parallelism, int timesliceLengthMs) {
      int messagesPerOperator = loadTargetHz / parallelism;
      return messagesPerOperator / (1000 / timesliceLengthMs);
    }
  }
}
