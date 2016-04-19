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
import org.apache.flink.api.common.state.StateBackend;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

public class QueryableWindowOperatorEvicting
		extends AbstractStreamOperator<Tuple3<String, Long, Long>>
		implements OneInputStreamOperator<UUID, Tuple3<String, Long, Long>>,
		QueryableKeyValueState<String, String> /* key: campaign_id (String), value: long (window count) */{

	private static final Logger LOG = LoggerFactory.getLogger(QueryableWindowOperatorEvicting.class);

	private static final Object actorSystemLock = new Object();
	private static ActorSystem actorSystem;
	private static int actorSystemUsers = 0;
	
	private final long windowSize;

	private long lastWatermark = 0;

	// first key is the window key, i.e. the end of the window
	// second key is the key of the element (campaign_id), value is count
	// these are checkpointed, i.e. fault-tolerant

	// (campaign_id --> (window_end_ts, count)
	private Map<Long, Map<UUID, CountAndAccessTime>> windows;
	
	private Map<UUID, CountAndAccessTime> currentWindowMap;
	private long currentWindow;

	// we retain the windows for a bit after emitting to give
	// them time to propagate to their final storage location
	// they are not stored as part of checkpointing
	// private Map<Long, Map<Long, Long>> retainedWindows;
	// private long lastSetTriggerTime = 0;

	private final FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);
	private final RegistrationService registrationService;

	private final boolean trackAccessTime;
	

	public QueryableWindowOperatorEvicting(
				long windowSize, RegistrationService registrationService, boolean trackAccessTime) {
		this.windowSize = windowSize;
		this.registrationService = registrationService;
		this.trackAccessTime = trackAccessTime;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening QueryableWindowOperator {}." , this);

		// don't overwrite if this was initialized in restoreState()
		if (windows == null) {
			windows = new HashMap<>();
		}

		// retainedWindows = new HashMap<>();

		if(registrationService != null) {
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

		if(registrationService != null) {
			registrationService.stop();
		}

		closeActorSystem(timeout);
	}

	@Override
	public void processElement(StreamRecord<UUID> streamRecord) throws Exception {
		UUID key = streamRecord.getValue();
		long timestamp = streamRecord.getTimestamp();
		long windowTimeStamp = timestamp - (timestamp % windowSize) + windowSize;

		synchronized (windows) {
			Map<UUID, CountAndAccessTime> window;
			if (currentWindow == windowTimeStamp) {
				window = currentWindowMap;
			}
			else {
				window = windows.get(windowTimeStamp);
				if (window == null) {
					window = new HashMap<>();
					windows.put(windowTimeStamp, window);
				}
				
				currentWindowMap = window;
				currentWindow = windowTimeStamp;
			}
			
			CountAndAccessTime previous = window.get(key);
			if (previous == null) {
				previous = new CountAndAccessTime();
				window.put(key, previous);
				previous.count = 1L;
				previous.lastEventTime = timestamp;
			} else {
				previous.count++;
				previous.lastEventTime = Math.max(previous.lastEventTime, timestamp);
			}
			
			if (trackAccessTime) {
				previous.lastAccessTime = System.currentTimeMillis();
			}
		}
	}

	@Override
	public void processWatermark(Watermark watermark) throws Exception {

		StreamRecord<Tuple3<String, Long, Long>> result = new StreamRecord<>(null, -1);

		Iterator<Map.Entry<Long, Map<UUID, CountAndAccessTime>>> iterator = windows.entrySet().iterator();
		
		while (iterator.hasNext()) {
			Map.Entry<Long, Map<UUID, CountAndAccessTime>> entry = iterator.next();
			
			if (entry.getKey() < watermark.getTimestamp()) {
				iterator.remove();
				Long windowTimestamp = entry.getKey();
				
				// emit window
				for (Map.Entry<UUID, CountAndAccessTime> window : entry.getValue().entrySet()) {
					Tuple3<String, Long, Long> resultTuple = Tuple3.of(window.getKey().toString(), windowTimestamp, window.getValue().count);
					output.collect(result.replace(resultTuple));
				}
			}
		}
		
		lastWatermark = watermark.getTimestamp();
	}


	@Override
	public StreamTaskState snapshotOperatorState(final long checkpointId, final long timestamp) throws Exception {
		
		LOG.info("Start async checkpoint @ " + getRuntimeContext().getTaskNameWithSubtasks());
		
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		final Map<Long, Map<UUID, CountAndAccessTime>> stateSnapshot = new HashMap<>(windows.size());
		
		synchronized (windows) {
			for (Map.Entry<Long, Map<UUID, CountAndAccessTime>> window : windows.entrySet()) {
				stateSnapshot.put(window.getKey(), new HashMap<>(window.getValue()));
			}
		}

		AsynchronousStateHandle<DataInputView> asyncState = new DataInputViewAsynchronousStateHandle(
				checkpointId,
				timestamp,
				stateSnapshot,
				getStateBackend());

		taskState.setOperatorState(asyncState);

		LOG.info("Done async checkpoint @ " + getRuntimeContext().getTaskNameWithSubtasks());
		
		return taskState;
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
			Long windowTimestamp = in.readLong();
			Map<UUID, CountAndAccessTime> window = new HashMap<>();
			windows.put(windowTimestamp, window);

			int numKeys = in.readInt();
			for (int j = 0; j < numKeys; j++) {
				UUID key = new UUID(in.readLong(), in.readLong());
				CountAndAccessTime value = new CountAndAccessTime();
				value.count = in.readLong();
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

		if (timestamp != null) {
			long ts = timestamp;
			timestamp = ts - (ts % windowSize) + windowSize;
		}
		
		UUID keyUUID = null;
		try {
			keyUUID = UUID.fromString(key);
		} catch (Exception e) {}

		synchronized (windows) {
			Map<UUID, CountAndAccessTime> window;
			if (timestamp != null) {
				window = windows.get(timestamp);
				if (window == null) {
					return "Window not found. Available window times " + windows.keySet().toString();
				}
			}
			else if (currentWindowMap != null) {
				window = currentWindowMap;
			}
			else {
				return "No data yet";
			}
			
			CountAndAccessTime cat = window.get(keyUUID);
			if (cat != null) {
				return Long.toString(cat.lastAccessTime - cat.lastEventTime);
			}
			else {
				UUID[] keys = new UUID[5];
				Iterator<UUID> iter = window.keySet().iterator();
				
				int i = 0;
				while (iter.hasNext() && i < keys.length) {
					keys[i++] = iter.next();
				}
				
				return "Key not found. Sample keys are " + Arrays.toString(keys);
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
			return "CountAndAccessTime{count=" + count + ", lastAccessTime=" + lastAccessTime +'}';
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
		private Map<Long, Map<UUID, CountAndAccessTime>> stateSnapshot;
		private AbstractStateBackend backend;
		private long size = 0;

		public DataInputViewAsynchronousStateHandle(long checkpointId,
				long timestamp,
				Map<Long, Map<UUID, CountAndAccessTime>> stateSnapshot,
													AbstractStateBackend backend) {
			this.checkpointId = checkpointId;
			this.timestamp = timestamp;
			this.stateSnapshot = stateSnapshot;
			this.backend = backend;
		}

		@Override
		public StateHandle<DataInputView> materialize() throws Exception {

			AbstractStateBackend.CheckpointStateOutputView out =
					backend.createCheckpointStateOutputView(checkpointId, timestamp);
			
			out.writeInt(stateSnapshot.size());
			
			for (Map.Entry<Long, Map<UUID, CountAndAccessTime>> window: stateSnapshot.entrySet()) {
				out.writeLong(window.getKey());
				out.writeInt(window.getValue().size());

				for (Map.Entry<UUID, CountAndAccessTime> value : window.getValue().entrySet()) {
					out.writeLong(value.getKey().getMostSignificantBits());
					out.writeLong(value.getKey().getLeastSignificantBits());
					out.writeLong(value.getValue().count);
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
