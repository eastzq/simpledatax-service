package com.github.test;

import com.alibaba.fastjson.JSON;
import com.github.simpledatax.api.DxService;
import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.dto.CommonDbReader;
import com.github.simpledatax.api.dto.CommonDbWriter;
import com.github.simpledatax.api.dto.DataCollectJob;
import com.github.simpledatax.api.dto.DataCollectResult;

public class MySql2MySqlTest {

    public static void main(String[] args) throws DxException {
        DataCollectJob job = new DataCollectJob();
        job.setJobId(1);
        job.setChannelNum(2);
        CommonDbReader reader = new CommonDbReader();
        reader.setDbIp("192.168.0.120");
        reader.setDbPort("3306");
        reader.setDbInstanceName("test");
        reader.setSplitPk("COL1");
        reader.setDbUser("root");
        reader.setDbPassword("root");
        reader.setTableName("mysql_load_test");
        reader.setColumnStrs("COL1,COL2,COL3,COL4");
        job.setReader(reader);

        CommonDbWriter writer = new CommonDbWriter();
        writer.setDbIp("192.168.0.121");
        writer.setDbPort("3306");
        writer.setDbUser("root");
        writer.setDbPassword("root");
        writer.setTableName("mysql_load_test");
        writer.setColumnStrs("COL1,COL2,COL3,COL4");
        writer.setDbInstanceName("test");
        job.setWriter(writer);
        DxService service = new DxService();
        DataCollectResult result = service.collect(job);
        System.out.println(JSON.toJSONString(result));
    }
}
