package com.ebay.dss.druidapp;

public class AddColumnTypeTest {
	public static void main(String[] args) {
		AddColumnTypeTest test = new AddColumnTypeTest();
		
		String input = "db.tb.col#select";
		String[] res = test.splitByDot(input);
		for (String s : res) {
			if (s.contains("#")) {
				System.out.println(s.split("\\#")[0]);
				System.out.println(s.split("\\#")[1]);
			}
			System.out.println(s);
		}
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

}
