package com.ebay.dss.druidapp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataInsertStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.dialect.teradata.parser.TeradataStatementParser;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.util.Utils;

public class ColumnImpact {
	// target column to source tolumn mapping
    private static HashMap<String, List<String>> dependMap = new HashMap<String, List<String>>();
    
    // source column to detailed source info mapping
//    private static HashMap<String, ArrayList<String[]>> sourceMap = new HashMap<String, ArrayList<String[]>>();
    private static HashMap<String, String[]> sourceMap = new HashMap<String, String[]>();
    
    public static void setDependMap(SQLStatement stmt, SchemaStatVisitor visitor) throws Exception {   	
    	stmt.accept(visitor);
    	Map<String, String> aliasMap = new HashMap<String, String>();
    	aliasMap = visitor.getAliasMap();
    	
    	if (stmt instanceof TeradataInsertStatement) {
    		TeradataInsertStatement insertStmt = (TeradataInsertStatement) stmt;
    		SQLSelect insertQuery = insertStmt.getQuery();
    		TeradataSelectQueryBlock insertBlock = (TeradataSelectQueryBlock) insertQuery.getQuery();

			TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
    		SQLTableSource tableSource = insertBlock.getFrom();
    		if (tableSource instanceof SQLSubqueryTableSource) {
    			tableSource.accept(fromVisitor);
        		System.out.println(fromVisitor.getTables());
    		} else if (tableSource instanceof SQLJoinTableSource) {
    			tableSource.accept(fromVisitor);
    			System.out.println(fromVisitor.getTables());
    		} else if (tableSource instanceof SQLExprTableSource) {
    			tableSource.accept(fromVisitor);
    			System.out.println(fromVisitor.getTables());
    		}
    		
    		if (insertStmt.getColumns() != null 
    				&& insertBlock.getSelectList() != null 
    				&& insertStmt.getColumns().size() == insertBlock.getSelectList().size()) {
    			for (int i=0; i<insertStmt.getColumns().size(); i++) {
    				String fullTargetCol = insertStmt.getTableName() 
    						+ "." 
    						+ insertStmt.getColumns().get(i).toString();
    				
    				System.out.println(insertBlock.getSelectList().get(i));
    				
    				SQLExpr sExpr = insertBlock.getSelectList().get(i).getExpr();
    				
					TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
//					Map<String, SQLObject> subQueryMap = null;
//					if (visitor instanceof TeradataSchemaStatVisitor) {
//						subQueryMap = ((TeradataSchemaStatVisitor) visitor).getSubQueryMap();
//					}
					
    				exprToColumn(sExpr, visitor1, fullTargetCol, aliasMap, fromVisitor);

    				// map target col to source col
    				List<String> fullSourceCol = dependMap.get(fullTargetCol);
    				if(fullSourceCol != null && !fullSourceCol.isEmpty()) {
    					for (String aliasSourceCol : fullSourceCol) {
        					if (aliasSourceCol.split("\\.").length == 2) {
        						String sourceDb = "";
        						String sourceTable = splitByDot(aliasSourceCol)[0];
        						String sourceCol = splitByDot(aliasSourceCol)[1];
        						sourceMap.put(aliasSourceCol, new String[]{sourceDb, sourceTable, sourceCol});
        					} else {
        						String sourceDb = splitByDot(aliasSourceCol)[0];
        						String sourceTable = splitByDot(aliasSourceCol)[1];
        						String sourceCol = splitByDot(aliasSourceCol)[2];
        						sourceMap.put(aliasSourceCol, new String[]{sourceDb, sourceTable, sourceCol});
        					}
        				}
    				}
    				
    			}
    		}
    	} else {
    		throw new Exception("not a valid Teradata insert statement!");
    	}
    }
    
    private static String[] splitByDot(String str) {
		String[] res = new String[3];
		if (str.contains(".")) {
			res = str.split("\\.");
		} else {
			res[1] = str;
		}
		return res;
	}
    
    private static void exprToColumn(SQLExpr expr, SchemaStatVisitor visitor, String targetCols, Map<String, String> aliMap, SchemaStatVisitor fromVisitor) {
    	if (expr instanceof SQLPropertyExpr
    			|| expr instanceof SQLIdentifierExpr) {
    		String colOwner;
    		String colName;
    		if (expr instanceof SQLIdentifierExpr) {
    			colOwner = "";
    			colName =  ((SQLIdentifierExpr) expr).getName();
    			expr = new SQLPropertyExpr(new SQLIdentifierExpr(colOwner), colName);
    		} else {
    			colOwner = ((SQLPropertyExpr) expr).getOwner().toString();
        		colName = ((SQLPropertyExpr) expr).getName().toString();
    		}
    		
			toRealColumn(expr, fromVisitor, targetCols, colOwner, colName, aliMap);    		
    		return;
    	}
    	
    	// deal with different expression
    	if (expr instanceof SQLMethodInvokeExpr) {
    		for (int i=0; i<(((SQLMethodInvokeExpr) expr).getParameters()).size(); i++) {
    			SQLExpr methodExpr =(((SQLMethodInvokeExpr) expr).getParameters()).get(i);
    			methodExpr.accept(visitor);
    			
    			for (Column column : visitor.getColumns()) {
        			String tableName = column.getTable();
        			String colName = column.getName();

        			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap);
        		}
    		}
    		System.out.println("\ttest method: " + visitor.getColumns());
    	} else if (expr instanceof SQLCaseExpr) {
    		for (int i=0; i<((SQLCaseExpr) expr).getItems().size(); i++) {
    			SQLExpr conExpr = ((SQLCaseExpr) expr).getItems().get(i).getConditionExpr();
    			conExpr.accept(visitor);
    		}
    		SQLExpr elseExpr = ((SQLCaseExpr) expr).getElseExpr();
    		elseExpr.accept(visitor);
    		
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap);
    		}
    		
//    		System.out.println("\ttest case: " + visitor.getColumns());
    		return;
    	} else if (expr instanceof SQLCastExpr) {
    		SQLExpr castExpr = ((SQLCastExpr) expr).getExpr();
    		castExpr.accept(visitor);
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap);
    		}
//    		
//    		System.out.println("\ttest cast: " + visitor.getColumns());
    	} else if (expr instanceof SQLBinaryOpExpr
    			|| expr instanceof SQLAggregateExpr) {
    		expr.accept(visitor);
    		
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap);
    		}
    	}
    	// other cases to be added here.
    	
    	
////    		System.out.println("\ttest binary: " + visitor.getColumns());
//    	} else if (expr instanceof SQLAggregateExpr) {
//    		expr.accept(visitor);
//    		for (Column column : visitor.getColumns()) {
//    			String tableName = column.getTable();
//    			String colName = column.getName();
//    			
//    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap);
//    		}
//    		return;
//    	}
    	return;
    }
    
    private static void addIntoMap(String key, String value) {
    	List<String> tempList = null;
		if (!dependMap.containsKey(key)) {
			tempList = new ArrayList<String>();
		    tempList.add(value);
			dependMap.put(key, tempList);
		} else {
			tempList = dependMap.get(key);
			tempList.add(value);
			dependMap.put(key, tempList);
		}
    }
    
    private static void toRealColumn(SQLExpr expr, SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap) {
    	// if colName in fromVisitor#aliasMap --> minor case
		if (fromVisitor.getAliasMap().containsKey(colName)) {
			addIntoMap(targetCols, fromVisitor.getAliasMap().get(colName));
		// if column is real column, 
		// while table and db not: 
		// 1. serarch in gloabl aliasMap
		} else {
			Set<String> aliKeys = aliMap.keySet();
			if (aliKeys.contains(tbName)) {
				// set full db.table to expr
				addIntoMap(targetCols, aliMap.get(tbName)+"."+colName);
    		} else {
    			// 2. iterate through all columns of fromVisitor tree
    			for (Column col : fromVisitor.getColumns()) {
    				if (colName.equalsIgnoreCase(col.getName())) {
    					addIntoMap(targetCols, col.toString());
    					break;
    				}
    			}
    		}
		}
    }

	public static void main(String[] args) throws Exception {
		String resource = "teradata-ins-2.txt";
		String output = "mapping.csv";
		File file = new File(output);
        BufferedWriter bw = null;
        
		String input = Utils.readFromResource(resource);
		input = input.replace("';'","''").replace("%;", "%").replace("+;<", "+<");
		String[] queries = input.split(";");
		System.out.println("This file has "+queries.length+" scripts to analyze.");
		for (String ins_sql : queries) {
		       
			TeradataStatementParser parser = new TeradataStatementParser(ins_sql);
			List<SQLStatement> statementList = parser.parseStatementList();
			SQLStatement stmt = statementList.get(0);
			
//			getDependMap(stmt);
			TeradataSchemaStatVisitor visitor = new TeradataSchemaStatVisitor();
			setDependMap(stmt, visitor);
			
			System.out.println("dep map: " +dependMap);
			System.out.println("source map:" + sourceMap);
			
			try {
				bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
				bw.write("target_database,"
						+ "target_table,"
						+ "tartet_column," 
						+ "source_database," 
						+ "source_table," 
						+ "source_column");
				bw.newLine();
				
				Set<String> tgtKeys = dependMap.keySet();
				for(String tgtKey : tgtKeys) {
					for (String sourceKey : dependMap.get(tgtKey)) {
						String[] tgtFullCol = splitByDot(tgtKey);
						for (String tgt : tgtFullCol) {
							bw.write(tgt.trim() + ",");
							System.out.print(tgt + ", ");	
						}	
						System.out.println();
					
						String[] res = sourceMap.get(sourceKey);
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
        System.out.println("------ end of " + new Object(){}.getClass().getEnclosingMethod().getName() + " ------");
	}
}
