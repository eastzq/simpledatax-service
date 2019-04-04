## 关于simpledatax
#### 背景
simpledatax基于阿里开源数据采集工具datax，关于阿里的datax请移步 https://github.com/alibaba/DataX  

#### 改动说明
1. 将datax进程调用改为线程内调用。
2. 将插件包和调度包集成到同一个包内，方便修改。插件间不再使用不同的类加载器。
3. 新增参数对象作为入参，封装接口，提取关键参数用于配置。
4. 修改调度机制，新增Future用于获取最新结果。
5. 待续...