package com.github.simpledatax.api.dto;

import java.io.Serializable;

public class DataCollectJob implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 默认并发数：1
     */
    public static final int DEFAULT_OCCURS = 1;

    public DataCollectJob() {
        this.setOccurs(DEFAULT_OCCURS);
    }

    /**
     * 采集任务描述
     */
    private String jobName;

    /**
     * 采集对象唯一标识符
     */
    private long jobId;

    /**
     * 是否清除临时目录。
     */
    private boolean clearJobTempFile = false;

    /**
     * 读规则对象
     */
    private DataCollectReader reader;

    /**
     * 写规则对象
     */
    private DataCollectWriter writer;

    /** 并发数 */
    private int occurs;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public DataCollectReader getReader() {
        return reader;
    }

    public void setReader(DataCollectReader reader) {
        this.reader = reader;
    }

    public DataCollectWriter getWriter() {
        return writer;
    }

    public void setWriter(DataCollectWriter writer) {
        this.writer = writer;
    }

    public int getOccurs() {
        return occurs;
    }

    public void setOccurs(int occurs) {
        this.occurs = occurs;
    }

    public boolean isClearJobTempFile() {
        return clearJobTempFile;
    }

    public void setClearJobTempFile(boolean isClearJobTempFile) {
        this.clearJobTempFile = isClearJobTempFile;
    }

}
