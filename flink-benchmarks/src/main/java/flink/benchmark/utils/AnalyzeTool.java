package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

	public static class Result {

		DescriptiveStatistics latencies;
		SummaryStatistics throughputs;
		Map<String, DescriptiveStatistics> perHostLat;
		Map<String, SummaryStatistics> perHostThr;

		public Result(DescriptiveStatistics latencies, SummaryStatistics throughputs, Map<String, DescriptiveStatistics> perHostLat, Map<String, SummaryStatistics> perHostThr) {
			this.latencies = latencies;
			this.throughputs = throughputs;
			this.perHostLat = perHostLat;
			this.perHostThr = perHostThr;
		}
	}

	public static Result analyze(String file, List<String> toIgnore) throws FileNotFoundException {
		Scanner sc = new Scanner(new File(file));

		String l;
		Pattern latencyPattern = Pattern.compile(".*Latency ([0-9]+) ms.*");
		Pattern throughputPattern = Pattern.compile(".*That's ([0-9.]+) elements\\/second\\/core.*");
		Pattern hostPattern = Pattern.compile("Container: .* on (.+).c.astral-sorter-757..*");
		Pattern stormHostPattern = Pattern.compile(".*Client environment:host.name=(.+).c.astral-sorter-757..*");

		DescriptiveStatistics latencies = new DescriptiveStatistics();
		SummaryStatistics throughputs = new SummaryStatistics();
		String currentHost = null;
		Map<String, DescriptiveStatistics> perHostLat = new HashMap<String, DescriptiveStatistics>();
		Map<String, SummaryStatistics> perHostThr = new HashMap<String, SummaryStatistics>();

		while( sc.hasNextLine()) {
			l = sc.nextLine();
			// ---------- host ---------------
			Matcher hostMatcher = hostPattern.matcher(l);
			if(hostMatcher.matches()) {
				currentHost = hostMatcher.group(1);
				System.err.println("Setting host to "+currentHost);
			}
			Matcher stormHostMatcher = stormHostPattern.matcher(l);
			if(stormHostMatcher.matches()) {
				currentHost = stormHostMatcher.group(1);
				System.err.println("Setting host to "+currentHost+ " (storm)");
			}

			if(toIgnore != null && toIgnore.contains(currentHost)) continue;

			// ---------- latency ---------------
			Matcher latencyMatcher = latencyPattern.matcher(l);
			if(latencyMatcher.matches()) {
				int latency = Integer.valueOf(latencyMatcher.group(1));
				latencies.addValue(latency);

				DescriptiveStatistics perHost = perHostLat.get(currentHost);
				if(perHost == null) {
					perHost = new DescriptiveStatistics();
					perHostLat.put(currentHost, perHost);
				}
				perHost.addValue(latency);
			}

			// ---------- throughput ---------------
			Matcher tpMatcher = throughputPattern.matcher(l);
			if(tpMatcher.matches()) {
				double eps = Double.valueOf(tpMatcher.group(1));
				throughputs.addValue(eps);
			//	System.out.println("epts = "+eps);

				SummaryStatistics perHost = perHostThr.get(currentHost);
				if(perHost == null) {
					perHost = new SummaryStatistics();
					perHostThr.put(currentHost, perHost);
				}
				perHost.addValue(eps);
			}
		}

		return new Result(latencies, throughputs, perHostLat, perHostThr);
	}

	public static void main(String[] args) throws FileNotFoundException {
		Result r1 = analyze(args[0], null);
		DescriptiveStatistics latencies = r1.latencies;
		SummaryStatistics throughputs = r1.throughputs;
		// System.out.println("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
		System.out.println("all-machines;" + latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99)+ ";" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + latencies.getN() + ";" + throughputs.getN());

		System.err.println("================= Latency (" + r1.perHostLat.size() + " reports ) =====================");
		List<Map.Entry<String, DescriptiveStatistics>> orderedPerHostLatency = new ArrayList<Map.Entry<String, DescriptiveStatistics>>();

		for(Map.Entry<String, DescriptiveStatistics> entry : r1.perHostLat.entrySet()) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+") =======");
			System.err.println("Mean latency " + entry.getValue().getMean());
			System.err.println("Median latency " + entry.getValue().getPercentile(50));
			orderedPerHostLatency.add(entry);
		}

		System.err.println("================= Throughput ("+r1.perHostThr.size()+" reports ) =====================");
		for(Map.Entry<String, SummaryStatistics> entry : r1.perHostThr.entrySet()) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+")=======");
			System.err.println("Mean throughput " + entry.getValue().getMean());
		}

		Collections.sort(orderedPerHostLatency, new Comparator<Map.Entry<String, DescriptiveStatistics>>() {
			@Override
			public int compare(Map.Entry<String, DescriptiveStatistics> o1, Map.Entry<String, DescriptiveStatistics> o2) {
				if (o1.getValue().getMean() < o2.getValue().getMean()) {
					return 1;
				} else {
					return -1;
				}
			}
		});

		List<Map.Entry<String, DescriptiveStatistics>> statsToIgnore = orderedPerHostLatency.subList(0, 2);
		List<String> toIgnore = new ArrayList<String>();
		System.err.println("============= HOSTS TO IGNORE (num: "+statsToIgnore.size()+") ============== ");
		for(Map.Entry<String, DescriptiveStatistics> entry : statsToIgnore) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+") =======");
			System.err.println("Mean latency " + entry.getValue().getMean());
			System.err.println("Median latency " + entry.getValue().getPercentile(50));
			toIgnore.add(entry.getKey());
		}

		Result finalResult = analyze(args[0], toIgnore);
		latencies = finalResult.latencies;
		throughputs = finalResult.throughputs;

		System.out.println("-2-machines;" + latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99)+ ";" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + latencies.getN() + ";" + throughputs.getN());



	}
}
