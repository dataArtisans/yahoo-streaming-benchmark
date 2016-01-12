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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZooKeeperUtils {
	public static CuratorFramework startCuratorFramework(
		String root,
		String zkQuorum,
		int sessionTimeout,
		int connectionTimeout,
		int retryWait,
		int maxRetryAttempts) {

		CuratorFramework result = CuratorFrameworkFactory.builder()
			.connectString(zkQuorum)
			.sessionTimeoutMs(sessionTimeout)
			.connectionTimeoutMs(connectionTimeout)
			.retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
			.namespace(root.startsWith("/") ? root.substring(1) : root)
			.build();

		result.start();

		return result;
	}
}
