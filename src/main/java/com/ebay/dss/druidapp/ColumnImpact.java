package com.ebay.dss.druidapp;

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
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataInsertStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;

public class ColumnImpact {
	// target column to source tolumn mapping
    private HashMap<String, List<String>> dependMap = new HashMap<String, List<String>>();
    
    // source column to detailed source info mapping
//    private static HashMap<String, ArrayList<String[]>> sourceMap = new HashMap<String, ArrayList<String[]>>();
    private HashMap<String, String[]> sourceMap = new HashMap<String, String[]>();
    
    public void setDependMap(SQLStatement stmt, SchemaStatVisitor visitor) throws Exception {   	
    	stmt.accept(visitor);
    	Map<String, String> aliasMap = new HashMap<String, String>();
    	aliasMap = visitor.getAliasMap();
    	
    	if (stmt instanceof TeradataInsertStatement) {
    		TeradataInsertStatement insertStmt = (TeradataInsertStatement) stmt;
    		SQLSelect insertQuery = insertStmt.getQuery();
    		
    		SQLSelectQuery selectQuery = insertQuery.getQuery();
    		if (selectQuery instanceof SQLUnionQuery) {
    			SQLSelectQueryBlock insertLeftBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getLeft();
    			selectBlockToColumn(insertStmt, insertLeftBlock, aliasMap);
    			
    			SQLSelectQueryBlock insertRightBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getRight();
    			selectBlockToColumn(insertStmt, insertRightBlock, aliasMap);
    		} else {
    			SQLSelectQueryBlock insertBlock = (SQLSelectQueryBlock) insertQuery.getQuery(); 
    			selectBlockToColumn(insertStmt, insertBlock, aliasMap);
    		}
    	} else {
    		throw new Exception("not a valid Teradata insert statement!");
    	}
    }
    
    private void selectBlockToColumn(SQLInsertStatement insertStmt,
			SQLSelectQueryBlock block, Map<String, String> aliasMap) {
    	TeradataSelectQueryBlock insertBlock = (TeradataSelectQueryBlock) block;
    	TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
		SQLTableSource tableSource = insertBlock.getFrom();
		tableSource.accept(fromVisitor);
		
		if (insertStmt.getColumns() != null 
				&& insertBlock.getSelectList() != null 
				&& insertStmt.getColumns().size() == insertBlock.getSelectList().size()) {
			for (int i=0; i<insertStmt.getColumns().size(); i++) {
				String fullTargetCol = insertStmt.getTableName() 
						+ "." 
						+ insertStmt.getColumns().get(i).toString();
								
				// if source table is only one table
				// no need to iterate through the visitor
				// simply add mapping into map.
				if (tableSource instanceof SQLExprTableSource
						&& ((SQLExprTableSource) tableSource).getExpr() != null) {
					addIntoMap(fullTargetCol, 
							((SQLExprTableSource) tableSource).getExpr().toString() + 
							"." +
							insertBlock.getSelectList().get(i).getExpr().toString());
				} else {
					SQLExpr sExpr = insertBlock.getSelectList().get(i).getExpr();
					
					TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
					
					exprToColumn(sExpr, visitor1, fullTargetCol, aliasMap, fromVisitor);
	
				}				
				setSourceMap(fullTargetCol);
			}
		}		
	}
    
	private void setSourceMap(String fullTargetCol) {
		List<String> fullSourceCol = dependMap.get(fullTargetCol);
		if(fullSourceCol != null && !fullSourceCol.isEmpty()) {
			for (String aliasSourceCol : fullSourceCol) {
				if (aliasSourceCol.split("\\.").length == 2) {
					String sourceDb = "";
					String sourceTable = splitByDot(aliasSourceCol)[0].toLowerCase();
					String sourceCol = splitByDot(aliasSourceCol)[1].toLowerCase();
					sourceMap.put(aliasSourceCol, new String[]{sourceDb, sourceTable, sourceCol});
				} else {
					String sourceDb = splitByDot(aliasSourceCol)[0].toLowerCase();
					String sourceTable = splitByDot(aliasSourceCol)[1].toLowerCase();
					String sourceCol = splitByDot(aliasSourceCol)[2].toLowerCase();
					sourceMap.put(aliasSourceCol, new String[]{sourceDb, sourceTable, sourceCol});
				}
			}
		}
	}

	public HashMap<String, List<String>> getDependMap() {
    	return this.dependMap;
    }
    
    
    public HashMap<String, String[]> getSourceMap() {
    	return this.sourceMap;
    }
    
    public String[] splitByDot(String str) {
		String[] res = new String[3];
		if (str.contains(".")) {
			res = str.split("\\.");
		} else {
			res[1] = str;
		}
		return res;
	}
    
    private void exprToColumn(SQLExpr expr, SchemaStatVisitor visitor, String targetCols, Map<String, String> aliMap, SchemaStatVisitor fromVisitor) {
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
    		
			toRealColumn(expr, fromVisitor, targetCols, colOwner, colName.toLowerCase(), aliMap);    		
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
    		return;
    	} else if (expr instanceof SQLCastExpr) {
    		SQLExpr castExpr = ((SQLCastExpr) expr).getExpr();
    		castExpr.accept(visitor);
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap);
    		}
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

    	return;
    }
    
    private void toRealColumn(SQLExpr expr, SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap) {
    	String tbName_lcase = tbName.toLowerCase();
    	String colName_lcase = colName.toLowerCase();
    	// case 1: if colName in fromVisitor#aliasMap
		if (fromVisitor.getAliasMap().containsKey(colName_lcase)) {
			// if get null from aliasMap
			// try to retrieve column info from aliasQueryMap 
			if (fromVisitor.getAliasMap().get(colName_lcase) == null
					&& fromVisitor instanceof TeradataSchemaStatVisitor) {
				Set<String> aliQueryKeys = ((TeradataSchemaStatVisitor) fromVisitor).getAliasQueryMap().keySet();
				for (String key : aliQueryKeys) {
					if (key.contains(colName_lcase)) {
						SQLObject sqlObj = ((TeradataSchemaStatVisitor) fromVisitor).getAliasQuery(key);
						TeradataSchemaStatVisitor newVisitor = new TeradataSchemaStatVisitor();
						sqlObj.accept(newVisitor);
						if (!newVisitor.getColumns().isEmpty()) {
							for (Column col : newVisitor.getColumns()) {
								String newTableName = col.getTable();
				    			String newColName = col.getName();
				    			if (!newColName.equalsIgnoreCase(colName_lcase)) {
				    				toRealColumn(expr, fromVisitor, targetCols, newTableName, newColName, aliMap);	
				    			} else {
				    				concatColumn(fromVisitor, targetCols, newTableName, newColName, aliMap);
				    			}
							}
						} else {
							concatColumn(fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap);
						}
						
						break;
					}
				}				
			} else {
				// otherwise recursively get real column
				toRealColumn(expr, fromVisitor, targetCols, 
						fromVisitor.getAliasMap().get(colName_lcase).contains(".") ? 
								fromVisitor.getAliasMap().get(colName_lcase).split("\\.")[0] : tbName_lcase,
						fromVisitor.getAliasMap().get(colName_lcase).contains(".") ?
								fromVisitor.getAliasMap().get(colName_lcase).split("\\.")[1]: fromVisitor.getAliasMap().get(colName_lcase),
						aliMap);
			}
		// case 2: 
		} else {
			concatColumn(fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap);
		}
    }
    
    private void concatColumn(SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap) {
		Set<String> aliKeys = aliMap.keySet();
		if (aliKeys.contains(tbName)) {
			// 1. set full db.table to expr
			addIntoMap(targetCols, aliMap.get(tbName)+"."+colName);
		} else {
			// 2. lastly, have to iterate through all columns of fromVisitor tree
			for (Column col : fromVisitor.getColumns()) {
				if (colName.equalsIgnoreCase(col.getName())) {
					addIntoMap(targetCols, col.toString());
				}
			}
		}
    }
    
    private void addIntoMap(String key, String value) {
    	List<String> tempList = null;
		if (!dependMap.containsKey(key)) {
			tempList = new ArrayList<String>();
		    tempList.add(value);
			dependMap.put(key, tempList);
		} else {
			tempList = dependMap.get(key);
			if (!tempList.contains(value)) {
			    tempList.add(value);
			    dependMap.put(key, tempList);
			}
		}
    }
}
