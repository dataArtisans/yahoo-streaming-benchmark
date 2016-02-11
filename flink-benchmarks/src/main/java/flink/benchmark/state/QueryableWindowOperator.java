/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package flink.benchmark.state;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsynchronousStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Custom operator that computes windows and also makes that state directly queryable from
 * outside the streaming system via Akka.
 */
public class QueryableWindowOperator
  extends AbstractStreamOperator<Tuple3<String, Long, Long>>
  implements OneInputStreamOperator<Tuple2<String, Long>, Tuple3<String, Long, Long>>,
  QueryableKeyValueState<String, String> /* key: campaign_id (String), value: long (window count) */ {

  private static final Logger LOG = LoggerFactory.getLogger(QueryableWindowOperator.class);

  private final long windowSize;

  private long lastWatermark = 0;

  // first key is the window key, i.e. the end of the window
  // second key is the key of the element (campaign_id), value is count
  // these are checkpointed, i.e. fault-tolerant

  // (campaign_id --> (window_end_ts, count)
  private Map<String, Map<Long, CountAndAccessTime>> windows;

  // we retain the windows for a bit after emitting to give
  // them time to propagate to their final storage location
  // they are not stored as part of checkpointing
  // private Map<Long, Map<Long, Long>> retainedWindows;
  // private long lastSetTriggerTime = 0;

  private final FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);
  private final RegistrationService registrationService;

  private static ActorSystem actorSystem;
  private static int actorSystemUsers = 0;
  private static final Object actorSystemLock = new Object();

  public QueryableWindowOperator(
    long windowSize,
    RegistrationService registrationService) {
    this.windowSize = windowSize;
    this.registrationService = registrationService;
  }

  @Override
  public void open() throws Exception {
    super.open();

    LOG.info("Opening QueryableWindowOperator {}.", this);

    // don't overwrite if this was initialized in restoreState()
    if (windows == null) {
      windows = new HashMap<>();
    }

    // retainedWindows = new HashMap<>();

    if (registrationService != null) {
      registrationService.start();

      String hostname = registrationService.getConnectingHostname();
      String actorName = "responseActor_" + getRuntimeContext().getIndexOfThisSubtask() + System.nanoTime();

      initializeActorSystem(hostname);

      ActorRef responseActor = actorSystem.actorOf(Props.create(ResponseActor.class, this), actorName);

      String akkaURL = AkkaUtils.getAkkaURL(actorSystem, responseActor);

      registrationService.registerActor(getRuntimeContext().getIndexOfThisSubtask(), akkaURL);
    }
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing QueyrableWindowOperator {}.", this);
    super.close();

    if (registrationService != null) {
      registrationService.stop();
    }

    closeActorSystem(timeout);
  }

  @Override
  public void processElement(StreamRecord<Tuple2<String, Long>> streamRecord) throws Exception {
    long timestamp = streamRecord.getValue().f1;
    long windowStart = timestamp - (timestamp % windowSize);
    long windowEnd = windowStart + windowSize;

    String campaign_id = streamRecord.getValue().f0;

    synchronized (windows) {
      Map<Long, CountAndAccessTime> window = windows.get(campaign_id);
      if (window == null) {
        window = new HashMap<>();
        windows.put(campaign_id, window);
      }

      CountAndAccessTime previous = window.get(windowEnd);
      if (previous == null) {
        previous = new CountAndAccessTime();
        window.put(windowEnd, previous);
        previous.count = 1L;
        previous.lastEventTime = timestamp;
      } else {
        previous.count++;
        previous.lastEventTime = Math.max(previous.lastEventTime, timestamp);
      }
      previous.lastAccessTime = System.currentTimeMillis();
    }
  }

  @Override
  public void processWatermark(Watermark watermark) throws Exception {
    // we'll keep state forever in the operator

	/*	StreamRecord<Tuple3<String, Long, Long>> result = new StreamRecord<>(null, -1);

		Iterator<Map.Entry<String, Map<Long, CountAndAccessTime>>> iterator = windows.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Map<Long, CountAndAccessTime>> campaignWindows = iterator.next();
			for(Map.Entry<Long, CountAndAccessTime> window: campaignWindows.getValue().entrySet()) {
				if(window.getKey() < watermark.getTimestamp() && window.getKey() >= lastWatermark) {
					// emit window
					Tuple3<String, Long, Long> resultTuple = Tuple3.of(campaignWindows.getKey(), window.getKey(), window.getValue().count);
					output.collect(result.replace(resultTuple));
				}
			}
		}
		lastWatermark = watermark.getTimestamp(); **/
  }


  @Override
  public StreamTaskState snapshotOperatorState(final long checkpointId, final long timestamp) throws Exception {
    StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

    synchronized (windows) {
      final Map<String, Map<Long, CountAndAccessTime>> stateSnapshot = new HashMap<>(windows.size());

      for (Map.Entry<String, Map<Long, CountAndAccessTime>> window : windows.entrySet()) {
        stateSnapshot.put(window.getKey(), new HashMap<>(window.getValue()));
      }

      AsynchronousStateHandle<DataInputView> asyncState = new DataInputViewAsynchronousStateHandle(
        checkpointId,
        timestamp,
        stateSnapshot,
        getStateBackend());

      taskState.setOperatorState(asyncState);
      return taskState;
    }
  }

  @Override
  public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
    super.notifyOfCompletedCheckpoint(checkpointId);
  }

  @Override
  public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
    super.restoreState(taskState, recoveryTimestamp);

    @SuppressWarnings("unchecked")
    StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
    DataInputView in = inputState.getState(getUserCodeClassloader());

    int numWindows = in.readInt();

    this.windows = new HashMap<>(numWindows);

    for (int i = 0; i < numWindows; i++) {
      String campaign = in.readUTF();
      Map<Long, CountAndAccessTime> window = new HashMap<>();
      windows.put(campaign, window);

      int numKeys = in.readInt();

      for (int j = 0; j < numKeys; j++) {
        long key = in.readLong(); // ts
        CountAndAccessTime value = new CountAndAccessTime();
        value.count = in.readLong();
        value.lastAccessTime = in.readLong();
        value.lastEventTime = in.readLong();
        window.put(key, value);
      }
    }
  }

  /**
   * Note: This method has nothing to do with a regular getValue() implementation.
   * Its more designed as a remote debugger
   *
   * @throws WrongKeyPartitionException
   */
  @Override
  public String getValue(Long timestamp, String key) throws WrongKeyPartitionException {
    LOG.info("Query for timestamp {} and key {}", timestamp, key);
    if (Math.abs(key.hashCode() % getRuntimeContext().getNumberOfParallelSubtasks()) != getRuntimeContext().getIndexOfThisSubtask()) {
      throw new WrongKeyPartitionException("Key " + key + " is not part of the partition " +
        "of subtask " + getRuntimeContext().getIndexOfThisSubtask());
    }

    if (windows == null) {
      return "No windows created yet";
    }

    synchronized (windows) {
      Map<Long, CountAndAccessTime> window = windows.get(key);
      if (window == null) {
        return "Key is not known. Available campaign IDs " + windows.keySet().toString();
      }
      if (timestamp == null) {
        // return the latency of the last window:
        TreeMap<Long, CountAndAccessTime> orderedMap = new TreeMap<>(window);
        Map.Entry<Long, CountAndAccessTime> first = orderedMap.lastEntry();
        return Long.toString(first.getValue().lastAccessTime - first.getValue().lastEventTime);
      } else {
        // query with timestamp:
        long windowStart = timestamp - (timestamp % windowSize);
        long windowEnd = windowStart + windowSize;
        CountAndAccessTime cat = window.get(windowEnd);
        if (cat == null) {
          return "Timestamp not available";
        }
        return Long.toString(cat.lastAccessTime - cat.lastEventTime);
      }
    }
  }

  @Override
  public String toString() {
    RuntimeContext ctx = getRuntimeContext();

    return ctx.getTaskName() + " (" + ctx.getIndexOfThisSubtask() + "/" + ctx.getNumberOfParallelSubtasks() + ")";
  }

  private static class CountAndAccessTime implements Serializable {
    long count;
    long lastAccessTime;
    long lastEventTime;

    @Override
    public String toString() {
      return "CountAndAccessTime{count=" + count + ", lastAccessTime=" + lastAccessTime + '}';
    }
  }


  private static void initializeActorSystem(String hostname) throws UnknownHostException {
    synchronized (actorSystemLock) {
      if (actorSystem == null) {
        Configuration config = new Configuration();
        Option<scala.Tuple2<String, Object>> remoting = new Some<>(new scala.Tuple2<String, Object>(hostname, 0));

        Config akkaConfig = AkkaUtils.getAkkaConfig(config, remoting);

        LOG.info("Start actory system.");
        actorSystem = ActorSystem.create("queryableWindow", akkaConfig);
        actorSystemUsers = 1;
      } else {
        LOG.info("Actor system has already been started.");
        actorSystemUsers++;
      }
    }
  }

  private static void closeActorSystem(FiniteDuration timeout) {
    synchronized (actorSystemLock) {
      actorSystemUsers--;

      if (actorSystemUsers == 0 && actorSystem != null) {
        actorSystem.shutdown();
        actorSystem.awaitTermination(timeout);

        actorSystem = null;
      }
    }
  }

  private static class DataInputViewAsynchronousStateHandle extends AsynchronousStateHandle<DataInputView> {

    private final long checkpointId;
    private final long timestamp;
    private Map<String, Map<Long, CountAndAccessTime>> stateSnapshot;
    private AbstractStateBackend backend;
    private long size = 0;

    public DataInputViewAsynchronousStateHandle(long checkpointId,
                                                long timestamp,
                                                Map<String, Map<Long, CountAndAccessTime>> stateSnapshot,
                                                AbstractStateBackend backend) {
      this.checkpointId = checkpointId;
      this.timestamp = timestamp;
      this.stateSnapshot = stateSnapshot;
      this.backend = backend;
    }

    @Override
    public StateHandle<DataInputView> materialize() throws Exception {
      AbstractStateBackend.CheckpointStateOutputView out = backend.createCheckpointStateOutputView(
        checkpointId,
        timestamp);

      int numWindows = stateSnapshot.size();
      out.writeInt(numWindows);


      for (Map.Entry<String, Map<Long, CountAndAccessTime>> window : stateSnapshot.entrySet()) {
        out.writeUTF(window.getKey());
        int numKeys = window.getValue().size();
        out.writeInt(numKeys);

        for (Map.Entry<Long, CountAndAccessTime> value : window.getValue().entrySet()) {
          out.writeLong(value.getKey());
          out.writeLong(value.getValue().count);
          out.writeLong(value.getValue().lastAccessTime);
          out.writeLong(value.getValue().lastEventTime);
        }
      }

      this.size = out.size();
      return out.closeAndGetHandle();
    }

    @Override
    public long getStateSize() throws Exception {
      return size;
    }
  }
}
