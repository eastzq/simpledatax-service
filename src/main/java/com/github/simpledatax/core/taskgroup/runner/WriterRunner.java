package com.github.simpledatax.core.taskgroup.runner;

import com.github.simpledatax.common.plugin.AbstractTaskPlugin;
import com.github.simpledatax.common.plugin.RecordReceiver;
import com.github.simpledatax.common.spi.Writer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Created by jingxing on 14-9-1.
 * <p/>
 * 单个slice的writer执行调用
 */
public class WriterRunner extends AbstractRunner implements Callable<Boolean> {

    private static final Logger LOG = LoggerFactory
            .getLogger(WriterRunner.class);

    private RecordReceiver recordReceiver;

    public void setRecordReceiver(RecordReceiver receiver) {
        this.recordReceiver = receiver;
    }

    public WriterRunner(AbstractTaskPlugin abstractTaskPlugin) {
        super(abstractTaskPlugin);
    }

    @Override
    public Boolean call() throws Exception{
        Validate.isTrue(this.recordReceiver != null);        
        Writer.Task taskWriter = (Writer.Task) this.getPlugin();
        //统计waitReadTime，并且在finally end
        try {
            LOG.debug("task writer starts to do init ...");
            taskWriter.init();

            LOG.debug("task writer starts to do prepare ...");
            taskWriter.prepare();
            
            LOG.debug("task writer starts to write ...");
            taskWriter.startWrite(recordReceiver);
           
            LOG.debug("task writer starts to do post ...");
            taskWriter.post();
            super.markSuccess();        
        } catch (Throwable e) {
            LOG.error("Writer Runner Received Exceptions:{}",e);
            super.markFail(e);
            return false;
        } finally {
            LOG.debug("task writer starts to do destroy ...");
            try {
            	super.destroy();
            }catch (Exception e) {
            	e.printStackTrace();
            }
        }
		return true;
    }
    
    public boolean supportFailOver(){
    	Writer.Task taskWriter = (Writer.Task) this.getPlugin();
    	return taskWriter.supportFailOver();
    }

    public void shutdown(){
        recordReceiver.shutdown();
    }
}
