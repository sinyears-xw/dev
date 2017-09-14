package com.cscs.portalSpark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import java.util.*;

public class App {
	private final static String cmpy = "chinaDaas";
	private final static String year = Calendar.getInstance().get(Calendar.YEAR) + "";

	private static class map_date_url implements Function<String, String> {
  		public String call(String s) { 
  			int cmpyindex = s.indexOf(cmpy);
  			int resultindex = s.indexOf("result");
  			if (cmpyindex  == -1 || resultindex == -1)
  				return "";
  			int sindex = s.indexOf(year);
  			String date = s.substring(sindex, cmpyindex - 1);
  			String url = s.substring(cmpyindex + cmpy.length() + 1, resultindex - 1);
  			return date + "|" + url;
  		}
	}

	private static void generateReport(List<String> rs) {
		String date = "";
		int index = 0;
		String line = "";
		Map<String, List<String>> rs_map = new HashMap<String, List<String>>();

		for (int i = 0; i < rs.size(); i++) {
			line = rs.get(i);
			if (line.equals(""))
				continue;
			
			index = line.indexOf("-", line.indexOf("-") + 1);
			
			date = line.substring(0, index);
			if (rs_map.containsKey(date)) {
				rs_map.get(date).add(line);
			} else {
				List<String> values = new ArrayList<String>();
				values.add(line);
				rs_map.put(date, values);
			}
		}

		for (Map.Entry<String, List<String>> entry : rs_map.entrySet()) {
			System.out.println(entry.getKey() + " total requests:" + entry.getValue().size());
			List<String> requests = entry.getValue();
			for (int i = 0; i < requests.size(); i++) {
				System.out.println("    " + requests.get(i));
			}
		}
	}

    public static void main( String[] args ) {
        SparkConf conf = new SparkConf().setAppName("chinaDaasStat");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("hdfs://172.19.6.50:8022/portal/chinaDaasLog");
		generateReport(lines.map(new map_date_url()).collect());
    }
}
