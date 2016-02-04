package flink.benchmark.generator;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;


/**
 * Base class for LoadGenerator Sources.  Implements features to generate whatever load
 * you require.
 */
public abstract class LoadGeneratorSource<T> extends RichParallelSourceFunction<T> {

  private boolean running = true;

  private final int loadTargetHz;
  private final int timeSliceLengthMs;

  public LoadGeneratorSource(int loadTargetHz, int timeSliceLengthMs) {
    this.loadTargetHz = loadTargetHz;
    this.timeSliceLengthMs = timeSliceLengthMs;
  }

  /**
   * Subclasses must override this to generate a data element
   */
  public abstract T generateElement();


  /**
   * The main loop
   */
  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    int elements = loadPerTimeslice();

    while (running) {
      long emitStartTime = System.currentTimeMillis();
      for (int i = 0; i < elements; i++) {
        sourceContext.collect(generateElement());
      }
      // Sleep for the rest of timeslice if needed
      long emitTime = System.currentTimeMillis() - emitStartTime;
      if (emitTime < timeSliceLengthMs) {
        Thread.sleep(timeSliceLengthMs - emitTime);
      }
    }
    sourceContext.close();
  }

  @Override
  public void cancel() {
    running = false;
  }


  /**
   * Given a desired load figure out how many elements to generate in each timeslice
   * before yielding for the rest of that timeslice
   */
  private int loadPerTimeslice() {
    int messagesPerOperator = loadTargetHz / getRuntimeContext().getNumberOfParallelSubtasks();
    return messagesPerOperator / (1000 / timeSliceLengthMs);
  }

}
