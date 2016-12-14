package com.ebay.dss.druidapp;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryPrepare {
	final Logger logger = LoggerFactory.getLogger(QueryPrepare.class);

	public String getValidInsertQuery(String insertQuery, String scriptName) {
		String output = insertQuery.replace("';'","''").replace("%;", "%").replace("+;<", "+<").trim();
		if (output.contains(";")) {
			String[] queries = output.split(";");
			for (String query : queries) {
				if (!query.toLowerCase().contains("insert ")
						&& !query.toLowerCase().contains("create ")
						&& !query.toLowerCase().contains("select ")) {
					output = "";
					continue;
				}
				if(query.length()>=7 && ("lock".equalsIgnoreCase(query.substring(0, 4)) || "locking".equalsIgnoreCase(query.substring(0, 7)))){
					int insertIndex = query.toLowerCase().indexOf("insert ");
					if (insertIndex != -1) {
						query = query.substring(insertIndex);
						output = query.trim();
					}
				}
				if(query.length()>=5 && ("using".equalsIgnoreCase(query.substring(0, 5)))){
					int insertIndex = query.toLowerCase().indexOf("insert ");
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
	
	public String getValidQuery(String parseQuery, String scriptName) {
		String output = parseQuery.replace("';'","''").replace("%;", "%").replace("+;<", "+<").trim();
		String[] queries = output.split(";");
		for (String query : queries) {
			if (StringUtils.countMatches(query, "/*") != StringUtils.countMatches(query, "*/")) {
				output = "";
				continue;
			}
			
			if (!query.toLowerCase().contains("insert ")
					&& !query.toLowerCase().contains("create ")
					&& !query.toLowerCase().contains("select ")
					&& !query.toLowerCase().contains("update ")
					&& !query.toLowerCase().contains("merge into ")
					&& !query.toLowerCase().contains("delete ")) {
				output = "";
				continue;
			}
			if(query.length()>=7 && ("lock".equalsIgnoreCase(query.substring(0, 4)) || "locking".equalsIgnoreCase(query.substring(0, 7)))){
				int insertIndex = query.toLowerCase().indexOf("insert ");
				if (insertIndex != -1) {
					query = query.substring(insertIndex);
					output = query.trim();
				}
			}
			if(query.length()>=5 && ("using".equalsIgnoreCase(query.substring(0, 5)))){
				int insertIndex = query.toLowerCase().indexOf("insert ");
				if (insertIndex != -1) {
					query = query.substring(insertIndex);
					output = query.trim();
				}
			}
		}		
		return output;
	}
	
	
}
