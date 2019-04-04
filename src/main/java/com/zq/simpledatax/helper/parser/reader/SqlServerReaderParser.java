package com.zq.simpledatax.helper.parser.reader;

import org.apache.commons.lang3.StringUtils;

import com.zq.simpledatax.api.message.DBDataCollectReader;
import com.zq.simpledatax.api.message.DataCollectPlugin;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.helper.ConfigHelper;
import com.zq.simpledatax.helper.DbHelper;
import com.zq.simpledatax.helper.PluginEnum;
import com.zq.simpledatax.helper.PropertiesKey;
import com.zq.simpledatax.helper.exception.BusiException;
import com.zq.simpledatax.helper.parser.Validatetor;
import com.zq.simpledatax.helper.parser.intf.Parser;

public class SqlServerReaderParser implements Parser {

	@Override
	public Configuration parse(DataCollectPlugin plugin) throws BusiException {
		DBDataCollectReader reader = null;
		if (!(plugin instanceof DBDataCollectReader)) {
			throw new BusiException("插件对象转换异常，当前实例不是DBDataCollectReader类型！");
		} else {
			reader = (DBDataCollectReader) plugin;
		}
		validate(reader);
		Configuration configuration = Configuration.newDefault();
		configuration.set("name", PluginEnum.SQLSERVER_READER.getName());
		configuration.set("parameter.username", reader.getDbUser());
		configuration.set("parameter.password", reader.getDbPassword());
		if(StringUtils.isNotBlank(reader.getSqlScript())) {
			configuration.set("parameter.connection[0].querySql[0]", reader.getSqlScript());			
		}else {
			configuration.set("parameter.where", reader.getSqlWhere());
			configuration.set("parameter.column[0]", reader.getColumnStrs());
			configuration.set("parameter.connection[0].table[0]", reader.getTableName());
			configuration.set("parameter.splitPk", reader.getSplitPk());
		}
		String jdbcUrl = DbHelper.getSqlServerJdbcUrl(reader.getDbIp(), reader.getDbPort(), reader.getDbInstanceName());
		configuration.set("parameter.connection[0].jdbcUrl[0]", jdbcUrl);
		String fetchSize = ConfigHelper.getConfigValue(PropertiesKey.JDBC_FETCHSIZE);
		if(StringUtils.isNotBlank(fetchSize)) {
			configuration.set("parameter.fetchSize", fetchSize);			
		}
		return configuration;
	}

	
	private void validate(DBDataCollectReader reader) throws BusiException {
		//校验
		Validatetor.validateReader(reader);
	}
}
