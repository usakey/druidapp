package com.ebay.dss.druidapp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Set;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.util.Utils;

public class ColumnImpactTest {
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		
		ColumnImpact impact = new ColumnImpact();
		
		QueryPrepare prepare = new QueryPrepare();
		
		final Connection connection = MetadataComplete.connMysqlDataBase("10.64.255.36","sa","sa","sa");
		
		String resource = "teradata-mrg-0.txt";
		String output = "result_test.csv";
		File file = new File(output);
        BufferedWriter bw = null;
        
		String input = Utils.readFromResource(resource);
		input = input.replace("';'","''").replace("%;", "%").replace("+;<", "+<").trim();
		String[] queries = input.split(";");
		System.out.println("This file has "+queries.length+" scripts to analyze.");
		for (String query : queries) {
		     
//			String ins_sql = prepare.getValidInsertQuery(query+";", "test.sql");
			String ins_sql = prepare.getValidQuery(query+";", "test.sql");
			TeradataStatementParser parser = new TeradataStatementParser(ins_sql);
			List<SQLStatement> statementList = parser.parseStatementList();
			if (statementList.size() == 0) {
				System.out.println("Cannot parse query as no valide statement in it!");
				continue;
			}
			SQLStatement stmt = statementList.get(0);
			
//			getDependMap(stmt);
			TeradataSchemaStatVisitor visitor = new TeradataSchemaStatVisitor();
			impact.setDependMap(stmt, visitor, connection);
			
			System.out.println("dep map: " + impact.getDependMap());
			System.out.println("source map:" + impact.getSourceMap());
			
			try {
				bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
				bw.write("target_database,"
						+ "target_table,"
						+ "tartet_column," 
						+ "source_database," 
						+ "source_table," 
						+ "source_column,"
						+ "join_type");
				bw.newLine();
				
				Set<String> tgtKeys = impact.getDependMap().keySet();
				for(String tgtKey : tgtKeys) {
					for (String sourceKey : impact.getDependMap().get(tgtKey)) {
						String[] tgtFullCol = impact.splitByDot(tgtKey);
						if (tgtFullCol.length == 2) {
							bw.write("," );	
						}						
						for (String tgt : tgtFullCol) {
							bw.write(tgt.trim() + ",");
							System.out.print(tgt + ", ");	
						}	
						System.out.println();
					
						String[] res = impact.getSourceMap().get(sourceKey);
						for (String s :  res) {
							bw.write(s.trim() + ",");
							System.out.print(s + ", ");	
						}
						System.out.println();
						System.out.println("****************");
						bw.newLine();
					}
//					String sourceKey = dependMap.get(tgtKey);
//					System.out.println(sourceKey);
					
				}
				bw.close();
			} catch (IOException e) {
				System.out.println("Error on writing to file");
			}
		}      
		connection.close();
		long endTime = System.currentTimeMillis();
		long execTime = endTime - startTime;
		System.out.println("there are " + impact.getMetaSchemaCount() + " meta schema counted!");
		System.out.println("time used: " + execTime + "ms");
        System.out.println("------ end of " + new Object(){}.getClass().getEnclosingMethod().getName() + " ------");
	}
}
