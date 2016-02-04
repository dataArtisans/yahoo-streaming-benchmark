package flink.benchmark.generator;

import flink.benchmark.BenchmarkConfig;

import java.util.Random;
import java.util.UUID;

/**
 * A data generator for generating large numbers of campaigns
 */
public class HighKeyCardinalityGeneratorSource extends LoadGeneratorSource<String> {

  private static final String[] eventTypes = {"view", "click", "purchase"};

  private int eventsIdx = 0;
  final StringBuilder elementBase = elementBase();
  final int resetSize = elementBase.length();
  final long campaignMsb = UUID.randomUUID().getMostSignificantBits();
  final long campaignLsbTemplate = UUID.randomUUID().getLeastSignificantBits() & 0xffffffff00000000L;
  final Random random = new Random();

  private final int numCampaigns;

  public HighKeyCardinalityGeneratorSource(BenchmarkConfig config) {
    super(config.loadTargetHz, config.timeSliceLengthMs);
    this.numCampaigns = config.numCampaigns;
  }

  @Override
  public String generateElement() {
    if (eventsIdx == eventTypes.length) {
      eventsIdx = 0;
    }

    long lsb = campaignLsbTemplate + random.nextInt(numCampaigns);
    UUID campaign = new UUID(campaignMsb, lsb);

    elementBase.setLength(resetSize);
    elementBase.append(campaign.toString());
    elementBase.append("\",\"ad_type\":\"banner78\",\"event_type\":\"");
    elementBase.append(eventTypes[eventsIdx++]);
    elementBase.append("\",\"event_time\":\"");
    elementBase.append(System.currentTimeMillis());
    elementBase.append("\",\"ip_address\":\"1.2.3.4\"}");

    return elementBase.toString();
  }

  private StringBuilder elementBase() {
    return new StringBuilder("{\"user_id\":\"" + UUID.randomUUID() + "\",\"page_id\":\"" + UUID.randomUUID() + "\",\"campaign_id\":\"");
  }
}
