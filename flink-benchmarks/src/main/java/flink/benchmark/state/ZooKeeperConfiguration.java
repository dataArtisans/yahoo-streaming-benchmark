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

import java.io.Serializable;

public class ZooKeeperConfiguration implements Serializable {
	private final String rootPath;
	private final String zkQuorum;
	private final int sessionTimeout;
	private final int connectionTimeout;
	private final int retryWait;
	private final int maxRetryAttempts;

	public ZooKeeperConfiguration(
		String rootPath,
		String zkQuorum,
		int sessionTimeout,
		int connectionTimeout,
		int retryWait,
		int maxRetryAttempts) {
		this.rootPath = rootPath;
		this.zkQuorum = zkQuorum;
		this.sessionTimeout = sessionTimeout;
		this.connectionTimeout = connectionTimeout;
		this.retryWait = retryWait;
		this.maxRetryAttempts = maxRetryAttempts;
	}

	public ZooKeeperConfiguration(String rootPath, String zkQuorum) {
		this(
			rootPath,
			zkQuorum,
			60000,
			15000,
			5000,
			3);
	}

	public String getRootPath() {
		return rootPath;
	}

	public String getZkQuorum() {
		return zkQuorum;
	}

	public int getSessionTimeout() {
		return sessionTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public int getRetryWait() {
		return retryWait;
	}

	public int getMaxRetryAttempts() {
		return maxRetryAttempts;
	}
}
