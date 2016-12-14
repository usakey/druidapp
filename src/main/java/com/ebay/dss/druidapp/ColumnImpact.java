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
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataCreateTableStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataInsertStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataMergeStatement;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataSelectQueryBlock;
import com.alibaba.druid.sql.dialect.teradata.ast.stmt.TeradataUpdateStatement;
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
    	
    	List<String> fullTargetColumns = new ArrayList<String>();
    	
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
    			fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertLeftBlock, connection);
    			
    			selectBlockToColumn(fullTargetColumns, insertLeftBlock, aliasMap, connection);
    			
    			// in case multiple Union as selectSource
    			while(((SQLUnionQuery) selectQuery).getRight() instanceof SQLUnionQuery) {
    				selectQuery = ((SQLUnionQuery) selectQuery).getRight();
    				insertLeftBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getLeft();
    				fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertLeftBlock, connection);
        			
    				selectBlockToColumn(fullTargetColumns, insertLeftBlock, aliasMap, connection);
    			}
    			SQLSelectQueryBlock insertRightBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getRight();
    			fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertRightBlock, connection);

    			selectBlockToColumn(fullTargetColumns, insertRightBlock, aliasMap, connection);
    		} else {
    			SQLSelectQueryBlock insertBlock = (SQLSelectQueryBlock) insertQuery.getQuery();
    			fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertBlock, connection);

    			selectBlockToColumn(fullTargetColumns, insertBlock, aliasMap, connection);
    			
//    			whereBlockToColumn(insertBlock, aliasMap, connection);
    		}
    	} else if (stmt instanceof SQLSelectStatement) {   		
    		SQLSelectQueryBlock selectBlock = (SQLSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
    		List<SQLSelectItem> selectList = selectBlock.getSelectList();
    		for (SQLSelectItem item : selectList) {
    			if (! (item.getExpr() instanceof SQLIdentifierExpr)) {
    				if (item.getAlias() != null) {
    					fullTargetColumns.add("benson_column_usage." + item.getAlias());	
    				} 
    				//else {
    					
    				//}
    			} else {
        			fullTargetColumns.add("benson_column_usage." + item.toString());	
    			} 
    			
    		}
    		selectBlockToColumn(fullTargetColumns, selectBlock, aliasMap, connection);
    		
    	} else if (stmt instanceof TeradataCreateTableStatement) {
            TeradataCreateTableStatement createStmt = (TeradataCreateTableStatement) stmt;
            SQLExprTableSource targetTable = createStmt.getTableSource();
            
            SQLSelect select = createStmt.getSelect();
            
            if (select != null) {
            	SQLSelectQuery selectQuery = select.getQuery();
        		if (selectQuery instanceof SQLUnionQuery) {
        			SQLSelectQueryBlock insertLeftBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getLeft();
//        			fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertLeftBlock, connection);
        			
        			selectBlockToColumn(fullTargetColumns, insertLeftBlock, aliasMap, connection);
        			
        			// in case multiple Union as selectSource
        			while(((SQLUnionQuery) selectQuery).getRight() instanceof SQLUnionQuery) {
        				selectQuery = ((SQLUnionQuery) selectQuery).getRight();
        				insertLeftBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getLeft();
//        				fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertLeftBlock, connection);
            			
        				selectBlockToColumn(fullTargetColumns, insertLeftBlock, aliasMap, connection);
        			}
        			SQLSelectQueryBlock insertRightBlock = (SQLSelectQueryBlock) ((SQLUnionQuery) selectQuery).getRight();
//        			fullTargetColumns = getFullTargetColumnFromInsert(insertStmt, insertRightBlock, connection);

        			selectBlockToColumn(fullTargetColumns, insertRightBlock, aliasMap, connection);
        		} else {
        			SQLSelectQueryBlock selectBlock = (SQLSelectQueryBlock) select.getQuery();
            		List<SQLSelectItem> selectList = selectBlock.getSelectList();
            		for (SQLSelectItem item : selectList) {
            			if (! (item.getExpr() instanceof SQLIdentifierExpr)) {
            				if (item.getAlias() != null) {
            					fullTargetColumns.add(targetTable.toString() + "." + item.getAlias());	
            				} else if (item.getExpr() instanceof SQLPropertyExpr){
            					String colName = ((SQLPropertyExpr) item.getExpr()).getName().toString();
            					System.out.println(colName);
            					fullTargetColumns.add(targetTable.toString() + "." + colName.toLowerCase());	
            				}
            				
            			} else {
        					fullTargetColumns.add(targetTable.toString() + "." + item.toString());	
            			} 
            		}
        			selectBlockToColumn(fullTargetColumns, selectBlock, aliasMap, connection);        			
        		}
            } else {
            	System.out.println("no select in create!");
            }

    	} else if (stmt instanceof TeradataUpdateStatement) {
    		System.out.println(stmt);
    		TeradataUpdateStatement updStmt = (TeradataUpdateStatement) stmt;
//    		SQLExprTableSource targetTable = (SQLExprTableSource) updStmt.getTableSource();
    		
//    		System.out.println(targetTable);
//    		if (aliasMap != null && aliasMap.get(targetTable.toString()) != null) {
//    			String targetTable = aliasMap.get(targetTable.toString());
//    		}
    		
    		updateToColumn(fullTargetColumns, updStmt, aliasMap, connection);
    	} else if (stmt instanceof TeradataMergeStatement) {
    		TeradataMergeStatement mrgStmt = (TeradataMergeStatement)stmt;
    		System.out.println(mrgStmt);
    		
    		mergeToColumn(fullTargetColumns, mrgStmt, aliasMap, connection);
    	}
    	
    	else {
    		logger.info(stmt.toString() + " is not insert, create, select, update!");
//    		throw new Exception("not a valid Teradata insert statement!");
    	}
    }
    
    private void mergeToColumn(List<String> fullTargetColumns,
			TeradataMergeStatement mrgStmt, Map<String, String> aliasMap,
			Connection connection) throws SQLException {
    	SQLExprTableSource targetTable = (SQLExprTableSource) mrgStmt.getInto();
    	
    	if (mrgStmt.getUpdateClause() != null) {
    		// set target columns
    		List<SQLUpdateSetItem> updItemList = mrgStmt.getUpdateClause().getItems();
    		for (SQLUpdateSetItem updItem : updItemList) {
    			System.out.println(updItem.getColumn() + " " + updItem.getValue());
    			
    			if (aliasMap != null && aliasMap.get(targetTable.toString()) != null) {
        			fullTargetColumns.add(aliasMap.get(targetTable.toString()) + "." + updItem.getColumn());	
        		} else {
        			fullTargetColumns.add(targetTable.toString() + "." + updItem.getColumn());
        		}
    		}
    		System.out.println(fullTargetColumns);
    		
    		// set source 
    		TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
        	
        	SQLTableSource tableSource = mrgStmt.getUsing();
        	tableSource.accept(fromVisitor);
        	fromVisitor.getAliasMap();
        	
        	SQLExpr onExpr = mrgStmt.getOn();
        	System.out.println(onExpr);
        	TeradataSchemaStatVisitor onVisitor = new TeradataSchemaStatVisitor();
        	onExpr.accept(onVisitor);
        	for (int i=0; i<fullTargetColumns.size(); i++) {
        		String fullTargetCol = fullTargetColumns.get(i);
        		exprToColumn(onExpr, onVisitor, fullTargetCol, aliasMap, fromVisitor, connection, "join");
        		setSourceMap(fullTargetCol);
        	}
    	}
    			
	}

	private void updateToColumn(List<String> fullTargetColumns, SQLUpdateStatement updStmt, 
    		Map<String, String> aliasMap, Connection connection) throws SQLException {
    	SQLExprTableSource targetTable = (SQLExprTableSource) updStmt.getTableSource();
    	
    	// set target columns
    	List<SQLUpdateSetItem> updItemList = updStmt.getItems();
    	for (SQLUpdateSetItem updItem : updItemList) {
    		System.out.println(updItem.getColumn() + " " + updItem.getValue());
    		
    		if (aliasMap != null && aliasMap.get(targetTable.toString()) != null) {
    			fullTargetColumns.add(aliasMap.get(targetTable.toString()) + "." + updItem.getColumn());	
    		} else {
    			fullTargetColumns.add(targetTable.toString() + "." + updItem.getColumn());
    		}
    	}    	
    	System.out.println(fullTargetColumns);
    	
    	// set source columns
    	TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
    	TeradataUpdateStatement tUpdStmt = (TeradataUpdateStatement)updStmt;
    	SQLTableSource tableSource = tUpdStmt.getFrom();
    	tableSource.accept(fromVisitor);

    	for (int i=0; i<fullTargetColumns.size(); i++) {
    		String fullTargetCol = fullTargetColumns.get(i);
    		// select and join type to be added here.
    		
    		
    		// parse where
    		if (tUpdStmt.getWhere() != null) {
    			System.out.println("has where!");
    			TeradataSchemaStatVisitor whereVisitor = new TeradataSchemaStatVisitor();
    			tUpdStmt.getWhere().accept(whereVisitor);
    			SQLExpr sExpr = tUpdStmt.getWhere();
    			exprToColumn(sExpr, whereVisitor, fullTargetCol, aliasMap, fromVisitor, connection, "where");
    		}
    		setSourceMap(fullTargetCol);
    	}
    }
    
    private void selectBlockToColumn(List<String> fullTargetCol,
			SQLSelectQueryBlock block, Map<String, String> aliasMap, Connection connection) throws SQLException {
    	TeradataSelectQueryBlock iBlock = (TeradataSelectQueryBlock) block;
    	TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
		SQLTableSource tableSource = iBlock.getFrom();
		tableSource.accept(fromVisitor);
		
		SQLSelectQueryBlock insertBlock = convertFromAllColumnExpr(iBlock, tableSource, connection);
		
		// deal with select list
		if (insertBlock.getSelectList() != null) {
			selectToColumn(fullTargetCol, insertBlock, aliasMap, connection, tableSource);
		}
	}
    
    private void selectToColumn(List<String> fullTargetColList, SQLSelectQueryBlock insertBlock,
			Map<String, String> aliasMap, Connection connection, SQLTableSource tableSource) throws SQLException {
    	TeradataSchemaStatVisitor fromVisitor = new TeradataSchemaStatVisitor();
    	tableSource.accept(fromVisitor);
    	
    	for (int i=0; i<fullTargetColList.size(); i++) {
    		String fullTargetCol = fullTargetColList.get(i);
			// if source table is only one table
			// no need to iterate through the visitor
			// simply add mapping into map.
			if (tableSource instanceof SQLExprTableSource
					&& ((SQLExprTableSource) tableSource).getExpr() != null
					&& insertBlock.getSelectList().get(i).getExpr() instanceof SQLIdentifierExpr) {
				addIntoMap(fullTargetCol, 
						((SQLExprTableSource) tableSource).getExpr().toString() + 
						"." +
						insertBlock.getSelectList().get(i).getExpr().toString() + 
						"#select");
			} else {
				SQLExpr sExpr = insertBlock.getSelectList().get(i).getExpr();
				TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
					
				exprToColumn(sExpr, visitor1, fullTargetCol, aliasMap, fromVisitor, connection, "select");
			}	
			
			// only in case of dependMap have insertion
			// do we check the join and where condition.
			List<String> fullSourceCol = dependMap.get(fullTargetCol);
			if(fullSourceCol != null && !fullSourceCol.isEmpty()) {
				// deal with join clauses
				if (insertBlock.getFrom() instanceof SQLJoinTableSource) {
//					boolean flag = false;
//					SQLJoinTableSource joinSource = (SQLJoinTableSource) insertBlock.getFrom();
//					TeradataSchemaStatVisitor joinVisitor = new TeradataSchemaStatVisitor();
//					joinSource.accept(joinVisitor);
					SQLJoinTableSource tempJoinSource = (SQLJoinTableSource) insertBlock.getFrom();
					List<SQLExpr> exprList = new ArrayList<SQLExpr>(); 
//					SQLExpr conExpr = ((SQLJoinTableSource) insertBlock.getFrom()).getCondition();
					// deal with multiple join clauses.
					while (tempJoinSource.getLeft() instanceof SQLJoinTableSource) {
//						flag = true;
						SQLExpr conExpr = tempJoinSource.getCondition();
						getAllLeafNodes(conExpr, exprList);
						System.out.println(exprList);
						joinToColumn(exprList, fullTargetCol, aliasMap, fromVisitor, connection);
						tempJoinSource = (SQLJoinTableSource) tempJoinSource.getLeft();
					} 
//					if (!flag) {
					SQLExpr conExpr = tempJoinSource.getCondition();
					getAllLeafNodes(conExpr, exprList);
					System.out.println(exprList);
					joinToColumn(exprList, fullTargetCol, aliasMap, fromVisitor, connection);
//					}
//					tempJoinSource = (SQLJoinTableSource) tempJoinSource.getLeft();					
				}
				// deal with where clauses
				if (insertBlock.getWhere() != null) {
					System.out.println("has where!");
					TeradataSchemaStatVisitor whereVisitor = new TeradataSchemaStatVisitor();
					insertBlock.getWhere().accept(whereVisitor);
					SQLExpr sExpr = insertBlock.getWhere();
					exprToColumn(sExpr, whereVisitor, fullTargetCol, aliasMap, fromVisitor, connection, "where");
				}
			}
			
			setSourceMap(fullTargetCol);
		
    	}	
	}
    
    private List<String> getFullTargetColumnFromInsert(SQLInsertStatement insertStmt, SQLSelectQueryBlock insertBlock, Connection connection) throws SQLException {
    	List<String> fullTargetColList = new ArrayList<String>();
    	
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
					fullTargetColList.add(fullTargetCol);
				}
		    } else {
		    	logger.error("no db name or table name!");
		    } 
		 // if insert with columns
		 // get columns from insert statement.
    	} else if (insertStmt.getColumns().size() == insertBlock.getSelectList().size()) {
			for (int i=0; i<insertStmt.getColumns().size(); i++) {
				String fullTargetCol = insertStmt.getTableName() 
						+ "." 
						+ insertStmt.getColumns().get(i).toString();
				fullTargetColList.add(fullTargetCol);
			}
	    }
    	
    	return fullTargetColList;
    }
    
    private void joinToColumn(List<SQLExpr> exprList, String fullTargetCol, Map<String, String> aliasMap, SchemaStatVisitor fromVisitor, Connection connection) throws SQLException {
		for(SQLExpr expr : exprList) {
			if (expr instanceof SQLBinaryOpExpr
					|| expr instanceof SQLBetweenExpr
					|| expr instanceof SQLInListExpr) {
				TeradataSchemaStatVisitor joinVisitor = new TeradataSchemaStatVisitor();
				expr.accept(joinVisitor);
				exprToColumn(expr, joinVisitor, fullTargetCol, aliasMap, fromVisitor, connection, "join");
			} 
			else if (expr instanceof SQLInSubQueryExpr) {
				TeradataSchemaStatVisitor joinVisitor = new TeradataSchemaStatVisitor();
				expr.accept(joinVisitor);
				SQLExpr exp = ((SQLInSubQueryExpr) expr).getExpr();
				exprToColumn(exp, joinVisitor, fullTargetCol, aliasMap, fromVisitor, connection, "join");
				
				// subquery with select
				SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock)((SQLInSubQueryExpr) expr).getSubQuery().getQuery();  
				
				TeradataSchemaStatVisitor innerFromVisitor = new TeradataSchemaStatVisitor();
				SQLTableSource tableSource = queryBlock.getFrom();
				tableSource.accept(innerFromVisitor);
				SQLSelectQueryBlock insertBlock = convertFromAllColumnExpr(queryBlock, tableSource, connection);
				
				for(int i=0; i<insertBlock.getSelectList().size(); i++) {
					if (tableSource instanceof SQLExprTableSource
							&& ((SQLExprTableSource) tableSource).getExpr() != null
							&& insertBlock.getSelectList().get(i).getExpr() instanceof SQLIdentifierExpr) {
						addIntoMap(fullTargetCol, 
								((SQLExprTableSource) tableSource).getExpr().toString() + 
								"." +
								insertBlock.getSelectList().get(i).getExpr().toString() + 
								"#join");
					} else {
						SQLExpr sExpr = insertBlock.getSelectList().get(i).getExpr();
						TeradataSchemaStatVisitor visitor1 = new TeradataSchemaStatVisitor();
							
						exprToColumn(sExpr, visitor1, fullTargetCol, aliasMap, innerFromVisitor, connection, "join");
					}
					if (insertBlock.getWhere() != null) {
//						System.out.println("has where!");
						TeradataSchemaStatVisitor whereVisitor = new TeradataSchemaStatVisitor();
						insertBlock.getWhere().accept(whereVisitor);
						SQLExpr sExpr = insertBlock.getWhere();
						exprToColumn(sExpr, whereVisitor, fullTargetCol, aliasMap, innerFromVisitor, connection, "where");
					}
				}
			}
		}
	}

	private void getAllLeafNodes(SQLExpr expr, List<SQLExpr> exprList) {
    	if (expr == null)
    		return;    	
    	
    	if (expr instanceof SQLBinaryOpExpr) {
    		if ((((SQLBinaryOpExpr)expr).getLeft() instanceof SQLPropertyExpr) || 
    				(((SQLBinaryOpExpr)expr).getRight() instanceof SQLPropertyExpr)) {
    			exprList.add(expr);
    		} else {
	    		getAllLeafNodes(((SQLBinaryOpExpr)expr).getLeft(), exprList);
	    		
	    		getAllLeafNodes(((SQLBinaryOpExpr)expr).getRight(), exprList);
    		}
    	} else if (! (expr instanceof SQLPropertyExpr)){
    		exprList.add(expr);
    	}
    }

	public void resetMap() {
    	this.dependMap.clear();
    	this.sourceMap.clear();
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
					String sourceCol = splitByDot(aliasSourceCol)[1].toLowerCase().split("\\#")[0];
					String columnType = splitByDot(aliasSourceCol)[1].toLowerCase().split("\\#")[1];
					sourceMap.put(aliasSourceCol, new String[]{sourceDb, sourceTable, sourceCol, columnType});
				} else {
					String sourceDb = splitByDot(aliasSourceCol)[0].toLowerCase();
					String sourceTable = splitByDot(aliasSourceCol)[1].toLowerCase();
					String sourceCol = splitByDot(aliasSourceCol)[2].toLowerCase().split("\\#")[0];
					String columnType = splitByDot(aliasSourceCol)[2].toLowerCase().split("\\#")[1];
					sourceMap.put(aliasSourceCol, new String[]{sourceDb, sourceTable, sourceCol, columnType});
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
    
    private void exprToColumn(SQLExpr expr, SchemaStatVisitor visitor, String targetCols, Map<String, String> aliMap, SchemaStatVisitor fromVisitor, Connection connection, String columnType) throws SQLException {
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
    		
			toRealColumn(expr, fromVisitor, targetCols, colOwner, colName.toLowerCase(), aliMap, connection, columnType);    		
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

        			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection, columnType);
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
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection, columnType);
    		}
    		return;
    	} else if (expr instanceof SQLCastExpr) {
    		SQLExpr castExpr = ((SQLCastExpr) expr).getExpr();
    		castExpr.accept(visitor);
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection, columnType);
    		}
    	} else if (expr instanceof SQLBinaryOpExpr
    			|| expr instanceof SQLAggregateExpr
    			|| expr instanceof SQLBetweenExpr
    			|| expr instanceof SQLInListExpr) {
    		expr.accept(visitor);
    		
    		for (Column column : visitor.getColumns()) {
    			String tableName = column.getTable();
    			String colName = column.getName();
    			
    			toRealColumn(expr, fromVisitor, targetCols, tableName, colName, aliMap, connection, columnType);
    		}
    	} else if (expr instanceof SQLJoinTableSource) {
    		System.out.println("join!!");
    	}
    	// other cases to be added here.

    	return;
    }
    
    private void toRealColumn(SQLExpr expr, SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap, Connection connection, String columnType) throws SQLException {
    	String tbName_lcase = tbName.toLowerCase();
    	String colName_lcase = colName.toLowerCase();
    	// deal with system functions
    	// such as date,...others to be added after test
    	// TODO
    	if (colName_lcase.equalsIgnoreCase("date")) {
    		return;
    	}
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
				    				if (newTableName.equalsIgnoreCase("unknown")) {
				    					boolean isMetaColumn = dealWithMetaSchema(targetCols, newColName, fromVisitor, connection, columnType);
				    					if (isMetaColumn) {
				    						continue;
				    					}
				    				}
				    				toRealColumn(expr, fromVisitor, targetCols, newTableName, newColName, aliMap, connection, columnType);	
				    			} else {
				    				concatColumn(expr, fromVisitor, targetCols, newTableName, newColName, aliMap, connection, columnType);
				    			}
							}
						} else {
							concatColumn(expr, fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap, connection, columnType);
						}
						
						break;
					}
				}				
			} else {
				// otherwise recursively get real column
				if (fromVisitor.getAliasMap().get(colName_lcase).contains(".")) {
					String newTable = fromVisitor.getAliasMap().get(colName_lcase).split("\\.")[0].toLowerCase();
					String newColumn = fromVisitor.getAliasMap().get(colName_lcase).split("\\.")[1].toLowerCase();
					if (!newColumn.equalsIgnoreCase(colName_lcase)) {
						toRealColumn(expr, fromVisitor, targetCols, newTable, newColumn, aliMap, connection, columnType);
					} else {
						concatColumn(expr, fromVisitor, targetCols, newTable, newColumn, aliMap, connection, columnType);
					}
					
				} else if (!fromVisitor.getAliasMap().get(colName_lcase).equalsIgnoreCase(colName_lcase)){
					if (tbName_lcase.equalsIgnoreCase("unknown")) {
    					boolean isMetaColumn = dealWithMetaSchema(targetCols, fromVisitor.getAliasMap().get(colName_lcase), fromVisitor, connection, columnType);
    					if (isMetaColumn) {
    						return;
    					}
    				}
					toRealColumn(expr, fromVisitor, targetCols, 
							tbName_lcase,
					        fromVisitor.getAliasMap().get(colName_lcase),
					        aliMap,
					        connection,
					        columnType);
				} else {
					concatColumn(expr, fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap, connection, columnType); 	
				}
			}
		// case 2: 
		} else {
			concatColumn(expr, fromVisitor, targetCols, tbName_lcase, colName_lcase, aliMap, connection, columnType);
		}
    }
    
    private void concatColumn(SQLExpr expr, SchemaStatVisitor fromVisitor, String targetCols, String tbName, String colName, Map<String, String> aliMap, Connection connection, String columnType) throws SQLException {
		boolean flag = false;
    	
    	Set<String> aliKeys = aliMap.keySet();
		if (aliKeys.contains(tbName)) {
			// 1. set full db.table to expr
			flag = true;
			addIntoMap(targetCols, aliMap.get(tbName)+"."+colName+"#"+columnType);
		} else {
			// 2. lastly, have to iterate through all columns of fromVisitor tree
			if (fromVisitor.getColumns().size() == 0) {
				dealWithMetaSchema(targetCols, colName, fromVisitor, connection, columnType);
			}
			for (Column col : fromVisitor.getColumns()) {
				if (colName.equalsIgnoreCase(col.getName())) {
					flag = true;
					tbName = col.getTable();			
					if (tbName.equalsIgnoreCase("unknown")) {
						dealWithMetaSchema(targetCols, colName, fromVisitor, connection, columnType);
					} else {
						addIntoMap(targetCols, col.toString() + "#" + columnType);

					}	
				}
			}
		} 
        if (!flag) {
        	dealWithMetaSchema(targetCols, colName, fromVisitor, connection, columnType);
		}
    }
    
    private boolean dealWithMetaSchema(String fullTargetCol, String colName, SchemaStatVisitor fromVisitor, Connection connection, String columnType) throws SQLException {
		String platform = "mozart";
		
		Set<Name> tables = fromVisitor.getTables().keySet();
		for (Name table : tables) {
			if (table.getName() == null) {
				continue;
			}
			if (table.getName().split("\\.").length == 2) {
				String dbName = splitByDot(table.getName())[0].toLowerCase().trim();
				String tbName = splitByDot(table.getName())[1].toLowerCase().trim();
//				ArrayList<String> metaSchema = mc.getMetaSchema(dbName, tbName, connection, platform);
				ArrayList<String> metaSchema = getMetaColumnList(dbName, tbName, connection, platform);
				// 
				if (metaSchema.contains(colName.toLowerCase()) ||
						metaSchema.contains(colName.toUpperCase())) {
					addIntoMap(fullTargetCol,
							dbName + "." + tbName + "." + colName + "#" + columnType);
					++COUNT;
					System.out.println("$$$$$ " + dbName + "." + tbName + "." + colName + "#" + columnType);
					return true;
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
		return false;
	}
    
    private ArrayList<String> getMetaColumnList(String dbName, String tbName, Connection connection, String platform) throws SQLException {
    	MetadataComplete mc = new MetadataComplete();
    	return mc.getMetaSchema(dbName, tbName, connection, platform);
    }
    
    private void addIntoMap(String key, String value) {
    	List<String> tempList = null;
		if (!dependMap.containsKey(key)) {
			tempList = new ArrayList<String>();
		    tempList.add(value.toLowerCase());
			dependMap.put(key, tempList);
		} else {
			tempList = dependMap.get(key);
			if (!tempList.contains(value.toLowerCase())) {
			    tempList.add(value.toLowerCase());
			    dependMap.put(key, tempList);
			}
		}
    }
    
    public int getMetaSchemaCount() {
    	return COUNT;
    }
}
