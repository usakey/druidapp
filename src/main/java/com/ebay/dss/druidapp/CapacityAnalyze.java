package com.ebay.dss.druidapp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.sql.parser.ParserException;

public class CapacityAnalyze {
	private static final String output = "result_capacity.csv";
	private static int counter = 0;
	private static int errorCounter = 0;
	
	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		
		final Logger logger = LoggerFactory.getLogger(CapacityAnalyze.class);
		
		final Connection connection = MetadataComplete.connMysqlDataBase("doe-db-602106.lvs01.eaz.ebayc3.com","dq","root","root");

		ColumnImpact impact = new ColumnImpact();
		QueryPrepare prepare = new QueryPrepare();
		
		File file = new File(output);
        BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
		
//		String query = "select scriptname, querytext from sa.lineage_test WHERE scriptname <>' ';";
		String query = "select log_dt, queryid, querytext from dq.query_log_capacity3 where log_dt='2016-06-27';";
		logger.info("query is " + query);
		
		PreparedStatement preparedStmt;
		try {
			preparedStmt = connection.prepareStatement(query);
			preparedStmt.execute();
			ResultSet rs = preparedStmt.getResultSet();
			while (rs.next()) {
				impact.resetMap();
				String logDt = rs.getString("log_dt");
				String queryId = rs.getString("queryid");
				if (queryId.equals("163706895250596584")) {
					continue;
				}
				String queryText = rs.getString("querytext");
//				String insQuery = prepare.getValidInsertQuery(queryText, scriptName);
//				String parseQuery = prepare.getValidInsertQuery(queryText, queryId);
				String parseQuery = prepare.getValidQuery(queryText, queryId);
//				if (scriptName.equalsIgnoreCase("dw_sps.etrs3_evo_sbo_snpsht_process_w.ins.sql")
//						) {
//					continue;
//				}
				
				logger.info("Below belongs to " + queryId);
				
				if (!parseQuery.isEmpty()) {
					try {
						++counter;
						TeradataStatementParser parser = new TeradataStatementParser(parseQuery);
						List<SQLStatement> statementList = parser.parseStatementList();
						if (statementList.size() == 0) {
							logger.error("Cannot parse query from " + queryId);
							continue;
						}
						SQLStatement stmt = statementList.get(0);
//						getDependMap(stmt);
						TeradataSchemaStatVisitor visitor = new TeradataSchemaStatVisitor();
						try {
							impact.setDependMap(stmt, visitor, connection);
						} catch (Exception e1) {
							logger.error("error when set depend map", e1);
							logger.error("This script is: " + queryId);
						}
						
//						System.out.println("dep map: " + impact.getDependMap());
//						System.out.println("source map:" + impact.getSourceMap());
						
						try { 
//							bw.write("log_dt,"
//									+ "queryid,"
//									+ "target_database,"
//									+ "target_table,"
//									+ "tartet_column," 
//									+ "source_database," 
//									+ "source_table," 
//									+ "source_column,"
//									+ "column_type");
//							bw.newLine();
							
							Set<String> tgtKeys = impact.getDependMap().keySet();
							for(String tgtKey : tgtKeys) {
								for (String sourceKey : impact.getDependMap().get(tgtKey)) {
									String[] tgtFullCol = impact.splitByDot(tgtKey);
									bw.write(logDt + ",");
									bw.write(queryId + ",");
									if (tgtFullCol.length == 2) {
										bw.write("," );	
									}
									for (String tgt : tgtFullCol) {
										bw.write(tgt.trim() + ",");
//										System.out.print(tgt + ", ");	
									}	
//									System.out.println();
								
									String[] res = impact.getSourceMap().get(sourceKey);
									for (String s :  res) {
										bw.write(s.trim() + ",");
//										System.out.print(s + ", ");	
									}
//									System.out.println();
//									System.out.println("****************");
									bw.newLine();
								}
//								String sourceKey = dependMap.get(tgtKey);
//								System.out.println(sourceKey);
								
							}
						} catch (IOException e) {
							logger.error("Error on writing to file", e);
						}
					} catch (ParserException ex) {
						++errorCounter;
						logger.error("ParserException when parsing " + queryId);
						logger.error("Parser Exception: ", ex);
						continue;
					}
				} else {
					logger.error("script in " + queryId + " is invalid!");
				}	
				logger.info("Above belongs to " + queryId);
			}
			
			preparedStmt.close();
			connection.close();
		} catch (SQLException e) {
			logger.error("Error when connecting to mysql.", e);
		}

		bw.close();
		long endTime = System.currentTimeMillis();
		long execTime = endTime - startTime;
//		logger.info("there are " + impact.getMetaSchemaCount() + " meta schema counted!");
		logger.info("there are " + counter + " queries counted!");
		logger.info("there are " + errorCounter + " parser exception counted!");
		logger.info("time used: " + execTime + "ms");
        logger.info("------ end of " + new Object(){}.getClass().getEnclosingMethod().getName() + " ------");
	}
	
//	
//	private static void processQuery(String scriptName, String query) {
//		
//	}
}
