package com.ebay.dss.druidapp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataComplete {
	final Logger logger = LoggerFactory.getLogger(MetadataComplete.class);

		
	public ArrayList<String> getMetaSchema(String db, String table, Connection connMysqlsa, String platform) throws SQLException{
		ArrayList<String> metaSchema = new ArrayList<String>();
		String query = "select columnname from sa.columns where databasename = ? and tablename = ? and system = ?";
		
		PreparedStatement stmt = connMysqlsa.prepareStatement(query);
		stmt.setString(1, db);
		stmt.setString(2, table);
		stmt.setString(3, platform);
		stmt.execute();
		ResultSet result = stmt.getResultSet();
		
		if(result.next()){	
			metaSchema.add(result.getString(1).trim().toLowerCase());
			while(result.next()){
				metaSchema.add(result.getString(1).trim().toLowerCase());
			}
			result.close();
			stmt.close();
		} else {
			logger.error("no data from metadata of table: " + db + "." + table);
		}
		return metaSchema;
	}
	
	public static Connection connMysqlDataBase(String ip, String databaseName, String username, String password){
		Connection conn= null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); 
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		  
		try {				  
			String url = "jdbc:mysql://" + ip + "/" + databaseName + "?user=" + username + "&password="+password+"&useUnicode=true&&characterEncoding=gb2312&autoReconnect = true";
//			System.out.print("Connect to MySQL: " + url + " ... \n");
			conn = DriverManager.getConnection(url,username,password);				
		} catch (SQLException e2) {
			e2.printStackTrace();
			System.exit(1);
		}
		return conn;
	}
}
