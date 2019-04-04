package com.zq.simpledatax.helper.parser;

import org.apache.commons.lang3.StringUtils;

import com.zq.simpledatax.api.message.DBDataCollectReader;
import com.zq.simpledatax.api.message.DBDataCollectWriter;
import com.zq.simpledatax.helper.exception.BusiException;

public class Validatetor {

	public static void validateReader(DBDataCollectReader reader) throws BusiException {
		// 校验必填项
		if (StringUtils.isBlank(reader.getDbIp()) || StringUtils.isBlank(reader.getDbPort())
				|| StringUtils.isBlank(reader.getDbInstanceName()) || StringUtils.isBlank(reader.getDbUser())
				|| StringUtils.isBlank(reader.getDbPassword()) || reader.getReadWriteMode() == null
				|| reader.getDbType() == null) {
			throw new BusiException(
					"reader参数校验失败，DbIp、DbPort、DbInstanceName、DbUser、DbPassword、ReadModel、DbType属性不能为空，请检查传入的参数！");
		}
		// 校验是否有采集语句！
		boolean hasTableSql = StringUtils.isNotBlank(reader.getTableName())
				&& StringUtils.isNotBlank(reader.getColumnStrs());
		boolean hasQuerySql = StringUtils.isNotBlank(reader.getSqlScript());
		if (!hasQuerySql && !hasTableSql) {
			throw new BusiException(
					"reader参数校验失败，【TableName、ColumnStrs】和【SqlScript】二选一，【SqlScript】优先级更高，两者不能为同时空！请检查传入的参数！");
		}
	}

	public static void validateWriter(DBDataCollectWriter writer) throws BusiException {
		if (StringUtils.isBlank(writer.getDbIp()) || StringUtils.isBlank(writer.getDbPort())
				|| StringUtils.isBlank(writer.getDbInstanceName()) || StringUtils.isBlank(writer.getDbUser())
				|| StringUtils.isBlank(writer.getDbPassword()) || writer.getDbType() == null
				|| StringUtils.isBlank(writer.getColumnStrs()) || StringUtils.isBlank(writer.getTableName())) {
			throw new BusiException("writer参数校验失败！DbIp、DbPort、DbInstanceName、DbUser、DbPassword、"
					+ "DbType、ColumnStrs、TableName属性不能为空，请检查传入的参数！");

		}
	}

	public static String filterOracleKeyWord(String colString) {
		if(StringUtils.isBlank(colString)) {
			return "";
		}
		String [] cols = colString.split(",");
		StringBuilder res = new StringBuilder();
		for (int i=0;  i<cols.length ; i++) {
			String colName = cols[i];
			if (colName.equalsIgnoreCase("no") || colName.equalsIgnoreCase("by")
					|| colName.equalsIgnoreCase("date")) {
				colName = "\"" + colName.toUpperCase() + "\"";
			}
			res.append(colName);
			if (i != cols.length-1) {
				res.append(",");
			}
		}
		return res.toString();
	}
}
