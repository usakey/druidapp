package com.ebay.dss.druidapp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataInsertStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.dialect.teradata.visitor.TeradataSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Name;

public class ColumnImpact {
	final Logger logger = LoggerFactory.getLogger(ColumnImpact.class);
	
	// target column to source tolumn mapping
    private LinkedHashMap<String, List<String>> dependMap = new LinkedHashMap<String, List<String>>();
    
    // source column to detailed source info mapping
//    private static HashMap<String, ArrayList<String[]>> sourceMap = new HashMap<String, ArrayList<String[]>>();
    private LinkedHashMap<String, String[]> sourceMap = new LinkedHashMap<String, String[]>();
    
    private static int COUNT = 0;
        
    public void setDependMap(SQLStatement stmt, SchemaStatVisitor visitor, Connection connection) throws SQLException{   	
    	stmt.accept(visitor);
    	Map<String, String> aliasMap = new HashMap<String, String>();
    	aliasMap = visitor.getAliasMap();
    	
    	if (stmt instanceof TeradataInsertStatement) {
    		TeradataInsertStatement insertStmt = (TeradataInsertStatement) stmt;
    		SQLSelect insertQuery = insertStmt.getQuery();
    		
    		// if insert into ... values()...
    		// do nothing
    		if (insertQuery == null) {
    			return;
    		}
    		
    		SQLSelectQuery selectQuery = insertQuery.getQuery();
    		if (selectQuery instanceof SQLUnionQuery) {
    			SQLSelectQueryBlock insertLeftBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getLeft();
    			selectBlockToColumn(insertStmt, insertLeftBlock, aliasMap, connection);
    			
    			SQLSelectQueryBlock insertRightBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getRight();
    			selectBlockToColumn(insertStmt, insertRightBlock, aliasMap, connection);
    		} else {
    			SQLSelectQueryBlock insertBlock = (SQLSelectQueryBlock) insertQuery.getQuery(); 
    			selectBlockToColumn(insertStmt, insertBlock, aliasMap, connection);
    		}
    	} else {
    		logger.error(stmt.toString() + "is not a valid TD insert statement!");
//    		throw new Exception("not a valid Teradata insert statement!");
    	}
    }
    
    public void resetMap() {
    	this.dependMap.clear();
    	this.sourceMap.clear();
    }
    
    private void selectBlockToColumn(SQLInsertStatement insertStmt,
			SQLSelectQueryBlock block, Map<String, String> aliasMap, Connection connection) throws SQLException {
    	TeradataSelectQueryBlock iBlock = (TeradataSelectQueryBlock) block;
    	TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
		SQLTableSource tableSource = iBlock.getFrom();
		tableSource.accept(fromVisitor);
		
		SQLSelectQueryBlock insertBlock = convertFromAllColumnExpr(iBlock, tableSource, connection);
		
		if (insertBlock.getSelectList() != null) {
			// if insert without columns specified
			// retrieve columns from meta schema
			if (insertStmt.getColumns().isEmpty()) {
			    String table = insertStmt.getTableName().toString();
			    if (table.split("\\.").length == 2) { 
			    	String dbName = splitByDot(table)[0].toLowerCase().trim();
					String tbName = splitByDot(table)[1].toLowerCase().trim();
					ArrayList<String> metaSchema = getMetaColumnList(dbName, tbName, connection, "mozart");
					
					for (int i=0; i<metaSchema.size(); i++) {
						String fullTargetCol = dbName + "." + tbName + "." + metaSchema.get(i);
						// if source table is only one table
						// no need to iterate through the visitor
						// simply add mapping into map.
						if (tableSource instanceof SQLExprTableSource
								&& ((SQLExprTableSource) tableSource).getExpr() != null
								&& insertBlock.getSelectList().get(i).getExpr() instanceof SQLIdentifierExpr) {
							addIntoMap(fullTargetCol, 
									((SQLExprTableSource) tableSource).getExpr().toString() + 
									"." +
									insertBlock.getSelectList().get(i).getExpr().toString());
						} else {
							SQLExpr sExpr = insertBlock.getSelectList().get(i).getExpr();
							TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
								
							exprToColumn(sExpr, visitor1, fullTargetCol, aliasMap, fromVisitor, connection);
						}			
						
						setSourceMap(fullTargetCol);
					}
			    } else {
			    	logger.error("no db name or table name!");
			    }
			  // if insert with columns
			  // get columns from insert statement.
			} else if (insertStmt.getColumns().size() == insertBlock.getSelectList().size()){
				for (int i=0; i<insertStmt.getColumns().size(); i++) {
					String fullTargetCol = insertStmt.getTableName() 
							+ "." 
							+ insertStmt.getColumns().get(i).toString();
									
					// if source table is only one table
					// no need to iterate through the visitor
					// simply add mapping into map.
					if (tableSource instanceof SQLExprTableSource
							&& ((SQLExprTableSource) tableSource).getExpr() != null
							&& insertBlock.getSelectList().get(i).getExpr() instanceof SQLIdentifierExpr) {
						addIntoMap(fullTargetCol, 
								((SQLExprTableSource) tableSource).getExpr().toString() + 
								"." +
								insertBlock.getSelectList().get(i).getExpr().toString());
					} else {
						SQLExpr sExpr = insertBlock.getSelectList().get(i).getExpr();
						TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
						
						exprToColumn(sExpr, visitor1, fullTargetCol, aliasMap, fromVisitor, connection);
					}			
					
					setSourceMap(fullTargetCol);
				}
			} else {
				logger.error("insert and select not match!");
			}
			}
					
	}
    
	private SQLSelectQueryBlock convertFromAllColumnExpr(SQLSelectQueryBlock insertBlock, SQLTableSource tableSource, Connection connection) throws SQLException {
		SQLSelectQueryBlock changedBlock = new SQLSelectQueryBlock();
		if (insertBlock instanceof TeradataSelectQueryBlock
				&& insertBlock.getSelectList().size() == 1
				&& insertBlock.getSelectList().get(0).getExpr() instanceof SQLAllColumnExpr) {
			if (tableSource instanceof SQLExprTableSource) {
				String sourceTable = ((SQLExprTableSource) tableSource).getExpr().toString();
				String dbName = splitByDot(sourceTable)[0].toLowerCase().trim();
				String tbName = splitByDot(sourceTable)[1].toLowerCase().trim();
				ArrayList<String> sourceSchema = getMetaColumnList(dbName, tbName, connection, "mozart");
				for (String col : sourceSchema) {
					SQLExpr expr = new SQLPropertyExpr(new SQLIdentifierExpr(sourceTable), col);
					SQLSelectItem newItem = new SQLSelectItem(expr);
					changedBlock.addSelectItem(newItem);
				}
			}
		    return changedBlock;	
		} else {
			return insertBlock;
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
    
    private void exprToColumn(SQLExpr expr, SchemaStatVisitor visitor, String targetCols, Map<String, String> aliMap, SchemaStatVisitor fromVisitor, Connection connection) throws SQLException {
    	if (expr instanceof SQLPropertyExpr
    			|| expr instanceof SQLIdentifierExpr) {
    		String colOwner;
    		String colName;
    		if (expr instanceof SQLIdentifierExpr) {
    			colOwner = "unknown";
    			colName =  ((SQLIdentifierExpr) expr).getName();
    			expr = new SQLPropertyExpr(new SQLIdentifierExpr(colOwner), colName);
    		} else {
    			colOwner = ((SQLPropertyExpr) expr).getOwner().toString();
        		colName = ((SQLPropertyExpr) expr).getName().toString();
    		}
    		
			toRealColumn(expr, fromVisitor, targetCols, colOwner, colName.toLowerCase(), aliMap, connection);    		
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

        			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection);
        		}
    		}
    	} else if (expr instanceof SQLCaseExpr) {
    		for (int i=0; i<((SQLCaseExpr) expr).getItems().size(); i++) {
    			SQLExpr conExpr = ((SQLCaseExpr) expr).getItems().get(i).getConditionExpr();
    			conExpr.accept(visitor);
    		}
    		SQLExpr elseExpr = ((SQLCaseExpr) expr).getElseExpr();
    		if (elseExpr != null) {
    			elseExpr.accept(visitor);
    		}
    		    		
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection);
    		}
    		return;
    	} else if (expr instanceof SQLCastExpr) {
    		SQLExpr castExpr = ((SQLCastExpr) expr).getExpr();
    		castExpr.accept(visitor);
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection);
    		}
    	} else if (expr instanceof SQLBinaryOpExpr
    			|| expr instanceof SQLAggregateExpr) {
    		expr.accept(visitor);
    		
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection);
    		}
    	}
    	// other cases to be added here.

    	return;
    }
    
    private void toRealColumn(SQLExpr expr, SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap, Connection connection) throws SQLException {
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
								String newTableName = 
										fromVisitor.getAliasMap().get(tbName_lcase) == null?
												col.getTable() : 
												fromVisitor.getAliasMap().get(tbName_lcase);
				    			String newColName = col.getName();
				    			if (!newColName.equalsIgnoreCase(colName_lcase)) {
				    				toRealColumn(expr, fromVisitor, targetCols, newTableName, newColName, aliMap, connection);	
				    			} else {
				    				concatColumn(expr, fromVisitor, targetCols, newTableName, newColName, aliMap, connection);
				    			}
							}
						} else {
							concatColumn(expr, fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap, connection);
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
						aliMap,
						connection);
			}
		// case 2: 
		} else {
			concatColumn(expr, fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap, connection);
		}
    }
    
    private void concatColumn(SQLExpr expr, SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap, Connection connection) throws SQLException {
		Set<String> aliKeys = aliMap.keySet();
		if (aliKeys.contains(tbName)) {
			// 1. set full db.table to expr
			addIntoMap(targetCols, aliMap.get(tbName)+"."+colName);
		} else {
			// 2. lastly, have to iterate through all columns of fromVisitor tree
			for (Column col : fromVisitor.getColumns()) {
				if (colName.equalsIgnoreCase(col.getName())) {
					addIntoMap(targetCols, col.toString());
					tbName = col.getTable();			}
			}
		} 
        if (tbName.equalsIgnoreCase("unknown")) {
			dealWithMetaSchema(targetCols, colName, fromVisitor, connection);
		}
    }
    
    private void dealWithMetaSchema(String fullTargetCol, String colName, SchemaStatVisitor fromVisitor, Connection connection) throws SQLException {
		String platform = "mozart";
		
		Set<Name> tables = fromVisitor.getTables().keySet();
		for (Name table : tables) {
			if (table.getName().split("\\.").length == 2) {
				String dbName = splitByDot(table.getName())[0].toLowerCase().trim();
				String tbName = splitByDot(table.getName())[1].toLowerCase().trim();
//				ArrayList<String> metaSchema = mc.getMetaSchema(dbName, tbName, connection, platform);
				ArrayList<String> metaSchema = getMetaColumnList(dbName, tbName, connection, platform);
				// 
				if (metaSchema.contains(colName)) {
					addIntoMap(fullTargetCol,
							dbName + "." + tbName + "." + colName);
					++COUNT;
					System.out.println("$$$$$ " + dbName + "." + tbName + "." + colName);
					break;
				} else {
					continue;
				}
			} else {
				logger.error("*********************");
				logger.error("error on meta schema!");
				logger.error("table name is: " + table + " and full target column is: " + fullTargetCol);
				logger.error("*********************");
			}
		}
	}
    
    private ArrayList<String> getMetaColumnList(String dbName, String tbName, Connection connection, String platform) throws SQLException {
    	MetadataComplete mc = new MetadataComplete();
    	return mc.getMetaSchema(dbName, tbName, connection, platform);
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
    
    public int getMetaSchemaCount() {
    	return COUNT;
    }
}
