package com.ebay.dss.druidapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryPrepare {
	final Logger logger = LoggerFactory.getLogger(QueryPrepare.class);

	public String getValidInsertQuery(String insertQuery, String scriptName) {
		String output = insertQuery.replace("';'","''").replace("%;", "%").replace("+;<", "+<").trim();
		if (output.contains(";")) {
			String[] queries = output.split(";");
			for (String query : queries) {
				if (!query.toLowerCase().contains("insert into")) {
					output = "";
					continue;
				}
				if(query.length()>=7 && ("lock".equalsIgnoreCase(query.substring(0, 4)) || "locking".equalsIgnoreCase(query.substring(0, 7)))){
					int insertIndex = query.toLowerCase().indexOf("insert into");
					if (insertIndex != -1) {
						query = query.substring(insertIndex);
						output = query.trim();
					}
				}
				if(query.length()>=5 && ("using".equalsIgnoreCase(query.substring(0, 5)))){
					int insertIndex = query.toLowerCase().indexOf("insert into");
					if (insertIndex != -1) {
						query = query.substring(insertIndex);
						output = query.trim();
					}
				}
			}
		} else {
			output = "";
			logger.error("Not a complete query in " + scriptName);
		}
		return output;
	}
	
}
