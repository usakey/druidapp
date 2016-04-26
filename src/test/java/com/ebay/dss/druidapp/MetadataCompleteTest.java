package com.ebay.dss.druidapp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public class MetadataCompleteTest {
	
	public static void main (String[] args) throws SQLException {
		MetadataComplete mc = new MetadataComplete();
		

		final Connection connection = MetadataComplete.connMysqlDataBase("10.64.255.36","sa","sa","sa");
		String db_name = "app_buyer_t";
		String table_name = "vac_sw_live_lstg_w";
		String platform = "mozart";
		
		ArrayList<String> metaSchema = mc.getMetaSchema(db_name, table_name, connection, platform);
		
		for (String meta : metaSchema) {
			System.out.println(meta);
		}
	}
	
}
