package com.github.simpledatax.api.dto;

import java.io.Serializable;

public class DataCollectResult implements Serializable {
    /**
     * @Fields serialVersionUID : TODO
     */

    private static final long serialVersionUID = 76197988825690340L;
    /**
     * 任务开始时间
     */
	private String startTime;
	/**
	 * 任务结束时间
	 */
	private String endTime;
	/**
	 * 任务总计耗时 s
	 */
	private long totalCosts;
	
	/**
	 * 任务平均流量 byte/s
	 */
	private long byteSpeedPerSecond;
	/**
	 * 记录写入速度 record/s
	 */
	private long recordSpeedPerSecond;
	/**
	 * 读出记录总数 条
	 */
	private long totalReadRecords;
	
	/**
	 * 读写失败总数 条
	 */
    private long totalErrorRecords;
    
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public long getTotalCosts() {
		return totalCosts;
	}
	public void setTotalCosts(long totalCosts) {
		this.totalCosts = totalCosts;
	}
	public long getByteSpeedPerSecond() {
		return byteSpeedPerSecond;
	}
	public void setByteSpeedPerSecond(long byteSpeedPerSecond) {
		this.byteSpeedPerSecond = byteSpeedPerSecond;
	}
	public long getRecordSpeedPerSecond() {
		return recordSpeedPerSecond;
	}
	public void setRecordSpeedPerSecond(long recordSpeedPerSecond) {
		this.recordSpeedPerSecond = recordSpeedPerSecond;
	}
	public long getTotalReadRecords() {
		return totalReadRecords;
	}
	public void setTotalReadRecords(long totalReadRecords) {
		this.totalReadRecords = totalReadRecords;
	}
	public long getTotalErrorRecords() {
		return totalErrorRecords;
	}
	public void setTotalErrorRecords(long totalErrorRecords) {
		this.totalErrorRecords = totalErrorRecords;
	} 

    @Override
    public String toString() {
        return "DataCollectResult [startTime=" + startTime + ", endTime=" + endTime + ", totalCosts=" + totalCosts
                + ", byteSpeedPerSecond=" + byteSpeedPerSecond + ", recordSpeedPerSecond=" + recordSpeedPerSecond
                + ", totalReadRecords=" + totalReadRecords + ", totalErrorRecords=" + totalErrorRecords + "]";
    }

    public String toCNString() {
        StringBuilder strSB = new StringBuilder(1024);
        strSB.append("采集信息 ");
        strSB.append("[ 开始时间:").append(startTime);
        strSB.append(", 结束时间:").append(endTime);
        strSB.append(", 总计耗时:").append(totalCosts).append("s");
        strSB.append(", 平均流量:").append(byteSpeedPerSecond).append("byte/s");
        strSB.append(", 记录写入速度:").append(recordSpeedPerSecond).append("record/s");
        strSB.append(", 读出记录总数:").append(totalReadRecords).append("条");
        strSB.append(", 读写失败总数:").append(totalErrorRecords).append("条 ]");
        return strSB.toString();
    }
}
