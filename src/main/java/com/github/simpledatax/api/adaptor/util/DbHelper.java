package com.github.simpledatax.api.adaptor.util;

public class DbHelper {
	public static String getOracleJdbcUrl(String host,String port,String instName){
		return "jdbc:oracle:thin:@"+host+":"+port+":"+instName;
	}
	
	public static String getSqlServerJdbcUrl(String host,String port,String instName) {
		return "jdbc:sqlserver://"+host+":"+port+";DatabaseName="+instName;
	}
	
	public static String getOracleSid(String host,String port,String instName) {
		return host+":"+port+"/"+instName;
	}
}
