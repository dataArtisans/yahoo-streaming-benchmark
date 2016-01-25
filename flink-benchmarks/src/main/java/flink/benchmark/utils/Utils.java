package flink.benchmark.utils;

import java.util.List;
import java.util.Map;

/**
 * Created by robert on 1/14/16.
 */
public class Utils {

	public static String getZookeeperServers(Map conf, String zkPath) {
		if(!conf.containsKey("zookeeper.servers")) {
			throw new IllegalArgumentException("Not zookeeper servers found!");
		}
		return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")), zkPath);
	}

	public static String getKafkaBrokers(Map conf) {
		if(!conf.containsKey("kafka.brokers")) {
			throw new IllegalArgumentException("No kafka brokers found!");
		}
		if(!conf.containsKey("kafka.port")) {
			throw new IllegalArgumentException("No kafka port found!");
		}
		return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")), "");
	}


	public static String listOfStringToString(List<String> list, String port, String path) {
		String val = "";
		for(int i=0; i<list.size(); i++) {
			val += list.get(i) + ":" + port + path;
			if(i < list.size()-1) {
				val += ",";
			}
		}
		return val;
	}
}
