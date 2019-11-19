package com.github.simpledatax.plugin.rdbms.reader.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant;
import com.github.simpledatax.plugin.rdbms.reader.RdbmsReaderKey;
import com.github.simpledatax.plugin.rdbms.util.DBUtil;
import com.github.simpledatax.plugin.rdbms.util.DataBaseType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liuyi on 15/9/18.
 */
public class HintUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderSplitUtil.class);

    public static String buildQueryColumn(String jdbcUrl, String table, String column,Configuration configuration,DataBaseType dataBaseType){
        String username = configuration.getString(RdbmsReaderKey.USERNAME);
        String password = configuration.getString(RdbmsReaderKey.PASSWORD);
        String hint = configuration.getString(RdbmsReaderKey.HINT);
        String hintExpression=null;
        Pattern tablePattern = null;
        if(StringUtils.isNotBlank(hint)){
            String[] tablePatternAndHint = hint.split("#");
            if(tablePatternAndHint.length==1){
               tablePattern = Pattern.compile(".*");
               hintExpression = tablePatternAndHint[0];
            }else{
                tablePattern = Pattern.compile(tablePatternAndHint[0]);
                hintExpression = tablePatternAndHint[1];
            }
        }
    	try{
            if(tablePattern != null && DataBaseType.Oracle.equals(dataBaseType)) {
                Matcher m = tablePattern.matcher(table);
                if(m.find()){
                    String[] tableStr = table.split("\\.");
                    String tableWithoutSchema = tableStr[tableStr.length-1];
                    String finalHint = hintExpression.replaceAll(RdbmsReaderConstant.TABLE_NAME_PLACEHOLDER, tableWithoutSchema);
                    //主库不并发读取
                    if(finalHint.indexOf("parallel") > 0 && DBUtil.isOracleMaster(jdbcUrl, username, password)){
                        LOG.info("master:{} will not use hint:{}", jdbcUrl, finalHint);
                    }else{
                        LOG.info("table:{} use hint:{}.", table, finalHint);
                        return finalHint + column;
                    }
                }
            }
        } catch (Exception e){
            LOG.warn("match hint exception, will not use hint", e);
        }
        return column;
    }
}
