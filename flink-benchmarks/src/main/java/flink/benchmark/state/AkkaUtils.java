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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AkkaUtils {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaUtils.class);

	public static Config getDefaultAkkaConfig(String hostname, int port) throws UnknownHostException{
		InetAddress address = InetAddress.getByName(hostname);

		String resolvedHostname = address.getHostName();

		Map<String, Object> configMap = new HashMap<String, Object>();
		List<String> loggers = new ArrayList<String>();

		loggers.add("akka.event.slf4j.Slf4jLogger");

		configMap.put("akka.daemonic", "on");
		configMap.put("akka.loggers", loggers);
		configMap.put("akka.logging-filter", "akka.event.slf4j.Slf4jLoggingFilter");
		configMap.put("akka.log-config-on-start", "off");
		configMap.put("akka.serialize-messages", "off");
		configMap.put("akka.loglevel", getlogLevel());
		configMap.put("akka.stdout-loglevel", "OFF");
		configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider");
		configMap.put("akka.remote.netty.tcp.transport-class", "akka.remote.transport.netty.NettyTransport");
		configMap.put("akka.remote.netty.tcp.port", port + "");
		configMap.put("akka.remote.netty.tcp.tcp-nodelay", "on");
		configMap.put("akka.remote.transport-failure-detector.heartbeat-interval", "1000s");
		configMap.put("akka.remote.transport-failure-detector.acceptable-heartbeat-pause", "6000s");

		if (resolvedHostname != null && resolvedHostname.length() > 0) {
			configMap.put("akka.remote.netty.tcp.hostname", resolvedHostname);
		}

		return ConfigFactory.parseMap(configMap);
	}

	public static String getlogLevel() {
		if (LOG.isTraceEnabled()) {
			return "TRACE";
		} else if (LOG.isDebugEnabled()) {
			return "DEBUG";
		} else if (LOG.isInfoEnabled()) {
			return "INFO";
		} else if (LOG.isWarnEnabled()) {
			return "WARNING";
		} else if (LOG.isErrorEnabled()) {
			return "ERROR";
		} else {
			return "OFF";
		}
	}
}
