package flink.benchmark.generator;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;


public class HighKeyCardinalityGenerator {

  private final ParameterTool parameterTool;

  public HighKeyCardinalityGenerator(ParameterTool pt) {
    this.parameterTool = pt;
  }
  

  public RichParallelSourceFunction<String> createSource() {
    int numCampaigns = parameterTool.getInt("campaigns", 10000000);
    int loadTargetHz = parameterTool.getInt("load.target.hz", 400_000);
    int timesliceLengthMs = parameterTool.getInt("load.timeslice.length.ms", 20);
    return new Source(numCampaigns, loadTargetHz, timesliceLengthMs);
  }

  
  public static class Source extends RichParallelSourceFunction<String>{
    
    private static final String[] eventTypes = { "view", "click", "purchase" };

    private final int numCampaigns;
    private final int loadTargetHz;
    private final int timesliceLengthMs;
    
    private volatile  boolean running = true;

    
    public Source(int numCampaigns, int loadTargetHz, int timesliceLengthMs) {
      this.numCampaigns = numCampaigns;
      this.loadTargetHz = loadTargetHz;
      this.timesliceLengthMs = timesliceLengthMs;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
      
      final Random rnd = new Random();
      
      final StringBuilder sb = new StringBuilder(
              "{\"user_id\":\"" + UUID.randomUUID() + "\",\"page_id\":\"" + UUID.randomUUID() + "\",\"campaign_id\":\"");
      
      final long campaignMsb = UUID.randomUUID().getMostSignificantBits();
      final long campaignLsbTemplate = UUID.randomUUID().getLeastSignificantBits() & 0xffffffff00000000L;
      
      
      final int resetSize = sb.length();
      final int messagesPerTimeslice = loadPerTimeslice(loadTargetHz,
              getRuntimeContext().getNumberOfParallelSubtasks(), timesliceLengthMs);
      
      int eventsIdx = 0;
      
      while (running) {
        long emitStartTime = System.currentTimeMillis();
        for (int i = 0; i < messagesPerTimeslice; i++) {
          sb.setLength(resetSize);
          
          long lsb = campaignLsbTemplate + rnd.nextInt(numCampaigns);
          UUID campaign = new UUID(campaignMsb, lsb);
          sb.append(campaign.toString());
          
          sb.append("\",\"ad_type\":\"banner78\",\"event_type\":\"");
          sb.append(eventTypes[eventsIdx++]);
          sb.append("\",\"event_time\":\"");
          sb.append(emitStartTime);
          sb.append("\",\"ip_address\":\"1.2.3.4\"}");
          
          sourceContext.collect(sb.toString());
          
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
