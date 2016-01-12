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
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;

public class ZooKeeperRegistrationService implements RegistrationService, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperRegistrationService.class);

	private final ZooKeeperConfiguration configuration;
	private transient CuratorFramework client;

	public ZooKeeperRegistrationService(ZooKeeperConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void start() {
		client = ZooKeeperUtils.startCuratorFramework(
			configuration.getRootPath(),
			configuration.getZkQuorum(),
			configuration.getSessionTimeout(),
			configuration.getConnectionTimeout(),
			configuration.getRetryWait(),
			configuration.getMaxRetryAttempts());
	}

	@Override
	public void stop() {
		try {
			ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), "/", true);
		} catch (Exception e) {
			LOG.warn("Error while cleaning the ZooKeeper nodes.", e);
		}

		if (client != null) {
			client.close();
			client = null;
		}
	}

	@Override
	public void registerActor(int partition, String actorURL) throws Exception {
		if (client != null) {
			boolean success = false;

			while (!success) {
				try {
					if (client.checkExists().forPath("/" + partition) != null) {
						client.setData().forPath("/" + partition, actorURL.getBytes());

						success = true;
					} else {
						client
							.create()
							.creatingParentsIfNeeded()
							.withMode(CreateMode.EPHEMERAL)
							.forPath("/" + partition, actorURL.getBytes());

						success = true;
					}
				} catch (KeeperException.NoNodeException ex) {
					// ignore and try again
				} catch (KeeperException.NodeExistsException ex) {
					// ignore and try again
				}
			}
		} else {
			throw new RuntimeException("CuratorFramework client has not been initialized.");
		}
	}

	@Override
	public String getConnectingHostname() throws IOException {
		String[] zkAddresses = configuration.getZkQuorum().split(",");

		if (zkAddresses.length <= 0) {
			throw new RuntimeException("There was no ZkQuorum specified. This is required to find" +
					"out the connecting hostname.");
		} else {
			String zkAddress = zkAddresses[0];

			String[] addressParts = zkAddress.split(":");
			String host = addressParts[0];
			int port = Integer.parseInt(addressParts[1]);

			InetSocketAddress targetAddress = new InetSocketAddress(host, port);

			return ConnectionUtils.findConnectingAddress(targetAddress, 2000, 400).getHostName();
		}
	}
}
