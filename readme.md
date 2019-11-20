## 关于simpledatax
#### 背景
simpledatax基于阿里开源数据采集工具datax做了一些减法，适合特定的场景。关于阿里的datax请移步 https://github.com/alibaba/DataX  

#### 改动说明
1. 将datax进程调用改为线程内调用。修复初始化时实例之间的冲突。
2. 将插件包和调度包集成到同一个包内，方便修改。
3. 插件间不再使用不同的类加载器。插件预加载机制。同时不改变参数的json配置处理机制。
4. 新增参数对象作为入参，封装接口，提取关键参数用于配置。
5. 修改内部调度机制，移除sleep收集任务执行状态的机制，修改为各个任务执行完毕时汇报情况，使用Future获取返回结果，可以稍微提高下效率。
6. 新增返回调度结果实例。用于界面展现。
7. 移除TaskGroupContainer，只能单机使用，如果需要集群建议结合分布式服务框架使用。
8. 调整了信息汇报部分代码，目前来看更加直观，也更易于修改。

#### 文档说明
1. 请参考阿里各个插件的文档。如果有更新需要替换插件并调整下代码。

#### 调用示例
```java
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
```


#### 下一步方向
1. 去除配置项的class类和代码里的全路径。
2. ...



