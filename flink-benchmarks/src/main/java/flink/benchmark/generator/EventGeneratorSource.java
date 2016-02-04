package flink.benchmark.generator;

import flink.benchmark.BenchmarkConfig;

import java.util.*;

/**
 * A data generator source
 */
public class EventGeneratorSource extends LoadGeneratorSource<String> {

  private int adsIdx = 0;
  private int eventsIdx = 0;
  private StringBuilder sb = new StringBuilder();
  private String pageID = UUID.randomUUID().toString();
  private String userID = UUID.randomUUID().toString();
  private final String[] eventTypes = new String[]{"view", "click", "purchase"};

  private List<String> ads;
  private final Map<String, List<String>> campaigns;

  public EventGeneratorSource(BenchmarkConfig config) {
    super(config.loadTargetHz, config.timeSliceLengthMs);
    this.campaigns = generateCampaigns();
    this.ads = flattenCampaigns();
  }

  public Map<String, List<String>> getCampaigns() {
    return campaigns;
  }

  /**
   * Generate a single element
   */
  @Override
  public String generateElement() {
    if (adsIdx == ads.size()) {
      adsIdx = 0;
    }
    if (eventsIdx == eventTypes.length) {
      eventsIdx = 0;
    }
    sb.setLength(0);
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
    sb.append(System.currentTimeMillis());
    sb.append("\",\"ip_address\":\"1.2.3.4\"}");

    return sb.toString();
  }

  /**
   * Generate a random list of ads and campaigns
   */
  private Map<String, List<String>> generateCampaigns() {
    int numCampaigns = 100;
    int numAdsPerCampaign = 10;
    Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
    for (int i = 0; i < numCampaigns; i++) {
      String campaign = UUID.randomUUID().toString();
      ArrayList<String> ads = new ArrayList<>();
      adsByCampaign.put(campaign, ads);
      for (int j = 0; j < numAdsPerCampaign; j++) {
        ads.add(UUID.randomUUID().toString());
      }
    }
    return adsByCampaign;
  }

  /**
   * Flatten into just ads
   */
  private List<String> flattenCampaigns() {
    // Flatten campaigns into simple list of ads
    List<String> ads = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
      for (String ad : entry.getValue()) {
        ads.add(ad);
      }
    }
    return ads;
  }
}
