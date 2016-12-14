package com.ebay.dss.druidapp;

import org.apache.commons.lang3.StringUtils;


public class StringOccurrenceTest {
	public static void main(String[] args) {
		String str = "/*ab*/cd/*";
		System.out.println(StringUtils.countMatches(str, "/*"));
		System.out.println(StringUtils.countMatches(str, "*/"));
		
	}
}
