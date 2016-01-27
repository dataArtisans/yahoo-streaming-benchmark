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

import akka.actor.Status;
import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ResponseActor<K extends Serializable, V extends Serializable> extends UntypedActor {
	private static final Logger LOG = LoggerFactory.getLogger(ResponseActor.class);

	private final QueryableKeyValueState<K, V> keyValueState;

	public ResponseActor(QueryableKeyValueState<K, V> keyValueState) {
		this.keyValueState = keyValueState;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof QueryState) {
			@SuppressWarnings("unchecked")
			QueryState<K> queryState = (QueryState<K>) message;

			LOG.debug("Received QueryState for key " + queryState.getKey() + ".");

			try {
				V value = keyValueState.getValue(queryState.getTimestamp(), queryState.getKey());

				if (value == null) {
					sender().tell(new StateNotFound<>(queryState.getKey()), getSelf());
				} else {
					sender().tell(new StateFound<>(queryState.getKey(), value), getSelf());
				}
			} catch (WrongKeyPartitionException ex) {
				sender().tell(new Status.Failure(ex), getSelf());
			}

			LOG.debug("Handled QueryState for key " + queryState.getKey() + ".");
		}else {
			throw new RuntimeException("Unknown message " + message);
		}
	}
}
